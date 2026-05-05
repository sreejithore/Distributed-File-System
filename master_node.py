import xmlrpc.server
import xmlrpc.client
import sqlite3
import os
import time
import threading

DB_FILE = "master_metadata.db"

# --- Live Node Tracker ---
live_nodes = {}
HEARTBEAT_TIMEOUT = 6.0 

def receive_heartbeat(node_address):
    """Catches the ping from the Data Nodes and updates their timestamp."""
    live_nodes[node_address] = time.time()
    print(f"💓 Heartbeat received from {node_address}") # Silenced for clean logs
    return True

def get_active_nodes():
    """Called by the Client to find out which nodes are currently alive."""
    current_time = time.time()
    active = []
    
    # print(f"🔍 [DEBUG] Client requested nodes. Current dictionary: {live_nodes}")
    
    for node, last_ping in list(live_nodes.items()):
        if current_time - last_ping <= HEARTBEAT_TIMEOUT:
            active.append(node)
        else:
            print(f"[WARNING] Data Node {node} is DEAD.")
            del live_nodes[node]
            
    return active

def init_db():
    """Initializes the SQLite database to store metadata."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    # ---> NEW: Added chunk_hash to the table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS file_chunks (
            filename TEXT,
            chunk_name TEXT,
            node_ip TEXT,
            chunk_hash TEXT,
            chunk_size INTEGER
        )
    ''')
    conn.commit()
    conn.close()

# --- RPC EXPOSED FUNCTIONS ---

def get_cluster_stats():
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(DISTINCT filename) FROM file_chunks")
        total_files = cursor.fetchone()[0]
        
        # ---> NEW: SUM the exact byte sizes instead of just counting chunks
        cursor.execute("SELECT node_ip, SUM(chunk_size) FROM file_chunks GROUP BY node_ip")
        node_chunks = cursor.fetchall()
        conn.close()
        
        node_storage = {}
        for ip, total_bytes in node_chunks:
            node_name = f"Node {ip.split(':')[-1]}"
            # ---> NEW: Convert raw bytes to Megabytes and round to 2 decimal places
            size_in_mb = round(total_bytes / (1024 * 1024), 2)
            node_storage[node_name] = size_in_mb
            
        return {
            "total_files": total_files,
            "node_storage_mb": node_storage,
            "replication_factor": 2
        }
    except Exception as e:
        print(f"[ERROR] Failed to fetch cluster stats: {e}")
        return {"total_files": 0, "node_storage_mb": {}, "replication_factor": 2}
    
def delete_file_metadata(filename):
    """Deletes the file's chunk map from the Master's database."""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM file_chunks WHERE filename=?", (filename,))
        conn.commit()
        conn.close()
        print(f"🗑️ [INFO] Deleted metadata for file: {filename}")
        return True
    except Exception as e:
        print(f"[ERROR] Database error during deletion: {e}")
        return False

def register_file_chunks(filename, chunk_data):
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        for chunk in chunk_data:
            # ---> NEW: Insert the chunk_size into the database
            cursor.execute("INSERT INTO file_chunks (filename, chunk_name, node_ip, chunk_hash, chunk_size) VALUES (?, ?, ?, ?, ?)",
                           (filename, chunk['chunk_name'], chunk['node_ip'], chunk['hash'], chunk['size_bytes']))
        conn.commit()
        conn.close()
        print(f"[INFO] Registered new file: {filename}")
        return True
    except Exception as e:
        print(f"[ERROR] Database error: {e}")
        return False

def get_file_directory():
    """Called by the Client to populate the Streamlit file browser."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT filename, COUNT(DISTINCT chunk_name) FROM file_chunks GROUP BY filename")
    files = cursor.fetchall()
    conn.close()
    
    registry = {}
    for filename, chunk_count in files:
         registry[filename] = f"{chunk_count} chunks stored"
    return registry

def get_chunk_locations(filename):
    """Called by the Client when downloading a file."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    # ---> NEW: Fetch chunk_hash from the database
    cursor.execute("SELECT chunk_name, node_ip, chunk_hash FROM file_chunks WHERE filename=?", (filename,))
    locations = cursor.fetchall()
    conn.close()
    return locations

# --- BACKGROUND REBALANCER ---
def replication_monitor():
    """Runs continuously to ensure all chunks meet the Replication Factor."""
    REPLICATION_FACTOR = 2
    
    while True:
        time.sleep(10)
        
        active_nodes = get_active_nodes()
        
        if len(active_nodes) < REPLICATION_FACTOR:
            continue
            
        try:
            conn = sqlite3.connect(DB_FILE)
            cursor = conn.cursor()
            
            # ---> UPDATE QUERY 1: Select the chunk_size alongside the hash
            cursor.execute("SELECT filename, chunk_name, node_ip, chunk_hash, chunk_size FROM file_chunks")
            all_chunks = cursor.fetchall()
            
            chunk_map = {}
            for filename, chunk_name, node_ip, chunk_hash, chunk_size in all_chunks:
                if chunk_name not in chunk_map:
                    # Save the exact byte size in our temporary dictionary
                    chunk_map[chunk_name] = {'filename': filename, 'nodes': [], 'hash': chunk_hash, 'size_bytes': chunk_size}
                chunk_map[chunk_name]['nodes'].append(node_ip)
                
            for chunk_name, data in chunk_map.items():
                current_nodes = data['nodes']
                
                if len(current_nodes) < REPLICATION_FACTOR:
                    source_node = next((n for n in current_nodes if n in active_nodes), None)
                    target_node = next((n for n in active_nodes if n not in current_nodes), None)
                    
                    if source_node and target_node:
                        print(f"🔄 [BALANCER] Fixing {chunk_name}: Copying from {source_node[-4:]} to {target_node[-4:]}...")
                        
                        try:
                            source_conn = xmlrpc.client.ServerProxy(f"http://{source_node}")
                            chunk_data = source_conn.get_chunk(chunk_name)
                            
                            target_conn = xmlrpc.client.ServerProxy(f"http://{target_node}")
                            target_conn.store_chunk(chunk_name, chunk_data)
                            
                            # ---> UPDATE QUERY 2: Insert the chunk_size for the backup row
                            cursor.execute("INSERT INTO file_chunks (filename, chunk_name, node_ip, chunk_hash, chunk_size) VALUES (?, ?, ?, ?, ?)",
                                           (data['filename'], chunk_name, target_node, data['hash'], data['size_bytes']))
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
    
    server_address = ('0.0.0.0', 5000)
    server = xmlrpc.server.SimpleXMLRPCServer(server_address, allow_none=True)
    
    server.register_function(get_cluster_stats, "get_cluster_stats")
    server.register_function(register_file_chunks, "register_file_chunks")
    server.register_function(get_file_directory, "get_file_directory")
    server.register_function(get_chunk_locations, "get_chunk_locations")
    server.register_function(receive_heartbeat, "receive_heartbeat")
    server.register_function(get_active_nodes, "get_active_nodes")
    server.register_function(delete_file_metadata, "delete_file_metadata")
    
    print("🧠 Master Node is running on port 5000...")
    print("Waiting for connections from Clients or Data Nodes...")
    
    threading.Thread(target=replication_monitor, daemon=True).start()
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nMaster Node shutting down.")

if __name__ == "__main__":
    start_master()
