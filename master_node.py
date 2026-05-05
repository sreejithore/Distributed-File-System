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
    # ---> NEW: Added version to the table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS file_chunks (
            filename TEXT,
            chunk_name TEXT,
            node_ip TEXT,
            chunk_hash TEXT,
            chunk_size INTEGER,
            timestamp REAL,
            version INTEGER
        )
    ''')
    conn.commit()
    conn.close()

# --- RPC EXPOSED FUNCTIONS ---

def verify_chunk_exists(chunk_name):
    """Answers the Data Node: Is this chunk still in the active database?"""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM file_chunks WHERE chunk_name=?", (chunk_name,))
        exists = cursor.fetchone() is not None
        conn.close()
        return exists
    except Exception as e:
        print(f"[ERROR] Database verification error: {e}")
        return True # If database is locked/busy, play it safe and say "Yes, keep it"

def get_storage_timeline():
    """Fetches the raw timeline of every chunk added to the system."""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("SELECT node_ip, chunk_size, timestamp FROM file_chunks ORDER BY timestamp ASC")
        data = cursor.fetchall()
        conn.close()
        return data
    except Exception as e:
        print(f"[ERROR] Timeline error: {e}")
        return []
    
def get_cluster_stats():
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(DISTINCT filename) FROM file_chunks")
        total_files = cursor.fetchone()[0]
        
        cursor.execute("SELECT node_ip, SUM(chunk_size) FROM file_chunks GROUP BY node_ip")
        node_chunks = cursor.fetchall()
        conn.close()
        
        node_storage = {}
        for ip, total_bytes in node_chunks:
            node_name = f"Node {ip.split(':')[-1]}"
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
    """Deletes the file's chunk map from the Master's database (All versions)."""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM file_chunks WHERE filename=?", (filename,))
        conn.commit()
        conn.close()
        print(f"🗑️ [INFO] Deleted metadata for all versions of file: {filename}")
        return True
    except Exception as e:
        print(f"[ERROR] Database error during deletion: {e}")
        return False

# ---> NEW FUNCTION
def get_next_version(filename):
    """Checks the database to find the next available version number for a file."""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(version) FROM file_chunks WHERE filename=?", (filename,))
        max_v = cursor.fetchone()[0]
        conn.close()
        return (max_v + 1) if max_v else 1
    except Exception as e:
        return 1

# ---> NEW FUNCTION
def get_all_chunk_locations(filename):
    """Fetches ALL chunks across ALL versions. Used for the Master Purge delete."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT chunk_name, node_ip FROM file_chunks WHERE filename=?", (filename,))
    locations = cursor.fetchall()
    conn.close()
    return locations

def register_file_chunks(filename, version, chunk_data):
    """Called by the Client when uploading."""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        for chunk in chunk_data:
            current_time = time.time() 
            # ---> NEW: Include version in the insert
            cursor.execute("INSERT INTO file_chunks (filename, chunk_name, node_ip, chunk_hash, chunk_size, timestamp, version) VALUES (?, ?, ?, ?, ?, ?, ?)",
                           (filename, chunk['chunk_name'], chunk['node_ip'], chunk['hash'], chunk['size_bytes'], current_time, version))
            time.sleep(0.01) 
            
        conn.commit()
        conn.close()
        print(f"[INFO] Registered new file: {filename} (v{version})")
        return True
    except Exception as e:
        print(f"[ERROR] Database error: {e}")
        return False

def get_file_directory():
    """Groups chunks by file and builds a version history list."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT filename, version, COUNT(DISTINCT chunk_name) FROM file_chunks GROUP BY filename, version")
    files = cursor.fetchall()
    conn.close()
    
    registry = {}
    for filename, version, chunk_count in files:
        if filename not in registry:
            registry[filename] = {'latest': version, 'versions': [], 'total_chunks': 0}
        registry[filename]['versions'].append(version)
        if version > registry[filename]['latest']:
            registry[filename]['latest'] = version
        registry[filename]['total_chunks'] += chunk_count
        
    for f in registry:
        registry[f]['versions'].sort(reverse=True) # Sort newest to oldest
        
    return registry

def get_chunk_locations(filename, version):
    """Fetches chunks for a specific version."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    # ---> NEW: Filter by version
    cursor.execute("SELECT chunk_name, node_ip, chunk_hash FROM file_chunks WHERE filename=? AND version=?", (filename, version))
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
            
            # ---> NEW: Fetch version alongside other fields
            cursor.execute("SELECT filename, chunk_name, node_ip, chunk_hash, chunk_size, timestamp, version FROM file_chunks")
            all_chunks = cursor.fetchall()
            
            # Group them to see how many copies exist
            chunk_map = {}
            for filename, chunk_name, node_ip, chunk_hash, chunk_size, timestamp, version in all_chunks:
                if chunk_name not in chunk_map:
                    chunk_map[chunk_name] = {'filename': filename, 'nodes': [], 'hash': chunk_hash, 'size_bytes': chunk_size, 'version': version}
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
                            
                            # ---> NEW: Record the exact time the balancer healed the node, and preserve version
                            heal_time = time.time() 
                            cursor.execute("INSERT INTO file_chunks (filename, chunk_name, node_ip, chunk_hash, chunk_size, timestamp, version) VALUES (?, ?, ?, ?, ?, ?, ?)",
                                           (data['filename'], chunk_name, target_node, data['hash'], data['size_bytes'], heal_time, data['version']))
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
    server.register_function(verify_chunk_exists, "verify_chunk_exists")
    server.register_function(register_file_chunks, "register_file_chunks")
    server.register_function(get_file_directory, "get_file_directory")
    server.register_function(get_chunk_locations, "get_chunk_locations")
    server.register_function(receive_heartbeat, "receive_heartbeat")
    server.register_function(get_active_nodes, "get_active_nodes")
    server.register_function(delete_file_metadata, "delete_file_metadata")
    server.register_function(get_storage_timeline, "get_storage_timeline")
    
    # ---> NEW: Register the new API endpoints
    server.register_function(get_next_version, "get_next_version")
    server.register_function(get_all_chunk_locations, "get_all_chunk_locations")
    
    print("🧠 Master Node is running on port 5000...")
    print("Waiting for connections from Clients or Data Nodes...")
    
    threading.Thread(target=replication_monitor, daemon=True).start()
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nMaster Node shutting down.")

if __name__ == "__main__":
    start_master()
