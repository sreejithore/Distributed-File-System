import xmlrpc.server
import xmlrpc.client
import os
import threading
import time
import sys 

# --- NODE CONFIGURATION ---
NODE_IP = "127.0.0.1"

# Allow passing port via terminal, default to 5001 if none provided
if len(sys.argv) > 1:
    NODE_PORT = int(sys.argv[1])
else:
    NODE_PORT = 5001  

MASTER_URL = "http://127.0.0.1:5000"
STORAGE_DIR = f"./storage_node_{NODE_PORT}"

# Ensure the storage directory exists
if not os.path.exists(STORAGE_DIR):
    os.makedirs(STORAGE_DIR)

# --- CORE STORAGE FUNCTIONS ---

def store_chunk(chunk_name, chunk_data):
    """Saves a binary chunk to the local hard drive."""
    try:
        file_path = os.path.join(STORAGE_DIR, chunk_name)
        with open(file_path, "wb") as f:
            f.write(chunk_data.data)
        print(f"[STORE] Saved chunk: {chunk_name}")
        return True
    except Exception as e:
        print(f"[ERROR] Failed to save {chunk_name}: {e}")
        return False

def get_chunk(chunk_name):
    """Reads a binary chunk from the local hard drive and sends it over the network."""
    try:
        file_path = os.path.join(STORAGE_DIR, chunk_name)
        if os.path.exists(file_path):
            with open(file_path, "rb") as f:
                return xmlrpc.client.Binary(f.read())
        else:
            raise FileNotFoundError(f"Chunk {chunk_name} not found on this node.")
    except Exception as e:
        print(f"[ERROR] Failed to retrieve {chunk_name}: {e}")
        raise

def delete_chunk(chunk_name):
    """Deletes a physical chunk from the local hard drive."""
    try:
        file_path = os.path.join(STORAGE_DIR, chunk_name)
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"[DELETE] Removed chunk: {chunk_name}")
            return True
        return False
    except Exception as e:
        print(f"[ERROR] Failed to delete {chunk_name}: {e}")
        return False

# --- BACKGROUND THREADS ---

def garbage_collector():
    """Periodically asks the Master if local chunks are still valid."""
    while True:
        time.sleep(15) # Check the system every 15 seconds
        try:
            master_conn = xmlrpc.client.ServerProxy(MASTER_URL)
            
            if not os.path.exists(STORAGE_DIR):
                continue
                
            # Look at every physical file in the storage folder
            local_chunks = os.listdir(STORAGE_DIR)
            
            for chunk_name in local_chunks:
                file_path = os.path.join(STORAGE_DIR, chunk_name)
                
                # Only check actual files (ignore system folders if any)
                if os.path.isfile(file_path):
                    # Ask the Master if this chunk is still registered in the SQLite DB
                    is_valid = master_conn.verify_chunk_exists(chunk_name)
                    
                    if not is_valid:
                        os.remove(file_path)
                        print(f"🗑️ [GARBAGE COLLECTOR] Purged orphaned chunk: {chunk_name}")
                        
        except ConnectionRefusedError:
            # If Master is offline, do nothing. We don't want to accidentally delete data!
            pass
        except Exception as e:
            pass

def send_heartbeat():
    """Runs in the background and pings the Master every 2 seconds."""
    while True:
        try:
            master = xmlrpc.client.ServerProxy(MASTER_URL)
            master.receive_heartbeat(f"{NODE_IP}:{NODE_PORT}")
        except Exception as e:
            print(f"⚠️ [ERROR] Heartbeat failed to reach Master: {e}") 
        time.sleep(2)

# --- SERVER STARTUP ---

def start_data_node():
    # 1. Start the heartbeat thread in the background
    heartbeat_thread = threading.Thread(target=send_heartbeat, daemon=True)
    heartbeat_thread.start()
    
    # 2. ---> THE FIX: We must actually start the Garbage Collector thread! <---
    gc_thread = threading.Thread(target=garbage_collector, daemon=True)
    gc_thread.start()
    
    # 3. Start the RPC server to listen for incoming file transfers
    server_address = ('0.0.0.0', NODE_PORT)
    server = xmlrpc.server.SimpleXMLRPCServer(server_address, allow_none=True)
    
    server.register_function(store_chunk, "store_chunk")
    server.register_function(get_chunk, "get_chunk")
    server.register_function(delete_chunk, "delete_chunk")
    
    print(f"💾 Data Node is running on port {NODE_PORT}...")
    print(f"Saving files to: {STORAGE_DIR}")
    print(f"🧹 Garbage Collector is ACTIVE.")
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down Data Node.")

if __name__ == "__main__":
    start_data_node()
