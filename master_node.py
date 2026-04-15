import xmlrpc.server
import xmlrpc.client  # <-- NEW: Allows Master to make outgoing requests
import sqlite3
import os
import time
import threading      # <-- NEW: Allows the balancer to run in the background

DB_FILE = "master_metadata.db"

# --- NEW: Live Node Tracker ---
# Dictionary to store { '127.0.0.1:5001': 1678888999.0 }
live_nodes = {}
HEARTBEAT_TIMEOUT = 6.0 # If we don't hear from a node in 6 seconds, it's dead.

def receive_heartbeat(node_address):
    """Catches the ping from the Data Nodes and updates their timestamp."""
    live_nodes[node_address] = time.time()
    print(f"💓 Heartbeat received from {node_address}") 
    return True

def get_active_nodes():
    """Called by the Client to find out which nodes are currently alive."""
    current_time = time.time()
    active = []
    
    for node, last_ping in list(live_nodes.items()):
        if current_time - last_ping <= HEARTBEAT_TIMEOUT:
            active.append(node)
        else:
            # Node has timed out / died!
            print(f"[WARNING] Data Node {node} is DEAD.")
            del live_nodes[node]
            
    return active

def init_db():
    """Initializes the SQLite database to store metadata."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    # Table to track which chunks belong to which files, and where they live
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS file_chunks (
            filename TEXT,
            chunk_name TEXT,
            node_ip TEXT
        )
    ''')
    conn.commit()
    conn.close()

# --- RPC EXPOSED FUNCTIONS ---
# These functions can be called over the network by the Client or Data Nodes

def delete_file_metadata(filename):
    """Deletes the file's chunk map from the Master's database."""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        # Delete all rows where the filename matches
        cursor.execute("DELETE FROM file_chunks WHERE filename=?", (filename,))
        conn.commit()
        conn.close()
        print(f"🗑️ [INFO] Deleted metadata for file: {filename}")
        return True
    except Exception as e:
        print(f"[ERROR] Database error during deletion: {e}")
        return False

def register_file_chunks(filename, chunk_data):
    """
    Called by the Client when uploading. 
    chunk_data is a list of dictionaries: [{'chunk_name': '...', 'node_ip': '...'}, ...]
    """
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        for chunk in chunk_data:
            cursor.execute("INSERT INTO file_chunks (filename, chunk_name, node_ip) VALUES (?, ?, ?)",
                           (filename, chunk['chunk_name'], chunk['node_ip']))
        conn.commit()
        conn.close()
        print(f"[INFO] Registered new file: {filename} with {len(chunk_data)} chunks.")
        return True
    except Exception as e:
        print(f"[ERROR] Database error: {e}")
        return False

def get_file_directory():
    """Called by the Client to populate the Streamlit file browser."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    # Count how many chunks each file has
    cursor.execute("SELECT filename, COUNT(chunk_name) FROM file_chunks GROUP BY filename")
    files = cursor.fetchall()
    conn.close()
    
    # Format for the GUI
    registry = {}
    for filename, chunk_count in files:
         registry[filename] = f"{chunk_count} chunks stored"
    return registry

def get_chunk_locations(filename):
    """Called by the Client when downloading a file."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT chunk_name, node_ip FROM file_chunks WHERE filename=?", (filename,))
    locations = cursor.fetchall()
    conn.close()
    return locations

# --- BACKGROUND REBALANCER ---
def replication_monitor():
    """Runs continuously to ensure all chunks meet the Replication Factor."""
    REPLICATION_FACTOR = 2
    
    while True:
        time.sleep(10) # Check the system every 10 seconds
        
        active_nodes = get_active_nodes()
        
        # If we don't have at least 2 nodes online, we can't replicate anyway
        if len(active_nodes) < REPLICATION_FACTOR:
            continue
            
        try:
            conn = sqlite3.connect(DB_FILE)
            cursor = conn.cursor()
            
            # Fetch every chunk in the database
            cursor.execute("SELECT filename, chunk_name, node_ip FROM file_chunks")
            all_chunks = cursor.fetchall()
            
            # Group them to see how many copies exist: { 'part_1': {'filename': 'file.txt', 'nodes': ['127.0.0.1:5001']} }
            chunk_map = {}
            for filename, chunk_name, node_ip in all_chunks:
                if chunk_name not in chunk_map:
                    chunk_map[chunk_name] = {'filename': filename, 'nodes': []}
                chunk_map[chunk_name]['nodes'].append(node_ip)
                
            # Check every chunk for missing backups
            for chunk_name, data in chunk_map.items():
                current_nodes = data['nodes']
                
                if len(current_nodes) < REPLICATION_FACTOR:
                    # UNDER-REPLICATED! We need to fix this.
                    # 1. Find an active node that HAS the chunk
                    source_node = next((n for n in current_nodes if n in active_nodes), None)
                    
                    # 2. Find an active node that DOES NOT have the chunk
                    target_node = next((n for n in active_nodes if n not in current_nodes), None)
                    
                    if source_node and target_node:
                        print(f"🔄 [BALANCER] Fixing {chunk_name}: Copying from {source_node[-4:]} to {target_node[-4:]}...")
                        
                        try:
                            # Fetch from Source
                            source_conn = xmlrpc.client.ServerProxy(f"http://{source_node}")
                            chunk_data = source_conn.get_chunk(chunk_name)
                            
                            # Push to Target
                            target_conn = xmlrpc.client.ServerProxy(f"http://{target_node}")
                            target_conn.store_chunk(chunk_name, chunk_data)
                            
                            # Update the Database
                            cursor.execute("INSERT INTO file_chunks (filename, chunk_name, node_ip) VALUES (?, ?, ?)",
                                           (data['filename'], chunk_name, target_node))
                            conn.commit()
                            print(f"✅ [BALANCER] Successfully replicated {chunk_name} to backup node!")
                        except Exception as e:
                            print(f"⚠️ [BALANCER] Failed to transfer {chunk_name}: {e}")
                            
            conn.close()
        except Exception as e:
            print(f"[ERROR] Balancer crashed: {e}")

# --- SERVER STARTUP ---
def start_master():
    init_db()
    
    # Set up the network server on port 5000
    server_address = ('0.0.0.0', 5000)
    server = xmlrpc.server.SimpleXMLRPCServer(server_address, allow_none=True)
    
    # Register the existing core functions
    server.register_function(register_file_chunks, "register_file_chunks")
    server.register_function(get_file_directory, "get_file_directory")
    server.register_function(get_chunk_locations, "get_chunk_locations")
    
    # Register the heartbeat function
    server.register_function(receive_heartbeat, "receive_heartbeat")
    
    # Register the active nodes tracking function
    server.register_function(get_active_nodes, "get_active_nodes")

    # Register the delete function
    server.register_function(delete_file_metadata, "delete_file_metadata")
    
    print("🧠 Master Node is running on port 5000...")
    print("Waiting for connections from Clients or Data Nodes...")
    # ---> NEW: Start the background replication monitor <---
    threading.Thread(target=replication_monitor, daemon=True).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nMaster Node shutting down.")

if __name__ == "__main__":
    start_master()
