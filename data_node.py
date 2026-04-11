import xmlrpc.server
import xmlrpc.client
import os
import threading
import time
import sys # <-- NEW IMPORT

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
    """Receives a binary chunk from the Client and saves it to disk."""
    try:
        # chunk_data arrives as an xmlrpc.client.Binary object, so we extract the raw bytes with .data
        file_path = os.path.join(STORAGE_DIR, chunk_name)
        with open(file_path, 'wb') as f:
            f.write(chunk_data.data)
        print(f"[SUCCESS] Stored chunk: {chunk_name}")
        return True
    except Exception as e:
        print(f"[ERROR] Failed to store chunk: {e}")
        return False

def get_chunk(chunk_name):
    """Reads a chunk from disk and sends it back to the Client."""
    try:
        file_path = os.path.join(STORAGE_DIR, chunk_name)
        with open(file_path, 'rb') as f:
            raw_bytes = f.read()
            # Wrap the bytes in an XML-RPC Binary object for safe network transit
            return xmlrpc.client.Binary(raw_bytes)
    except Exception as e:
        print(f"[ERROR] Failed to read chunk: {e}")
        return None

# --- HEARTBEAT MECHANISM ---
'''
def send_heartbeat():
    """Runs in the background and pings the Master every 2 seconds."""
    while True:
        try:
            master = xmlrpc.client.ServerProxy(MASTER_URL)
            # We will add this function to the Master Node in the next step!
            master.receive_heartbeat(f"{NODE_IP}:{NODE_PORT}")
        except Exception:
            # If the Master is offline, silently fail and try again in 2 seconds
            pass
        time.sleep(2)
'''

def send_heartbeat():
    """Runs in the background and pings the Master every 2 seconds."""
    while True:
        try:
            master = xmlrpc.client.ServerProxy(MASTER_URL)
            master.receive_heartbeat(f"{NODE_IP}:{NODE_PORT}")
        except Exception as e:
            # ---> CHANGED: Force it to print the error instead of 'pass' <---
            print(f"⚠️ [ERROR] Heartbeat failed to reach Master: {e}") 
        time.sleep(2)

# --- SERVER STARTUP ---

def start_data_node():
    # 1. Start the heartbeat thread in the background
    heartbeat_thread = threading.Thread(target=send_heartbeat, daemon=True)
    heartbeat_thread.start()
    
    # 2. Start the RPC server to listen for file transfers
    server_address = ('0.0.0.0', NODE_PORT)
    server = xmlrpc.server.SimpleXMLRPCServer(server_address, allow_none=True)
    
    server.register_function(store_chunk, "store_chunk")
    server.register_function(get_chunk, "get_chunk")
    
    print(f"💾 Data Node is running on port {NODE_PORT}...")
    print(f"Saving files to: {STORAGE_DIR}")
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nData Node shutting down.")

if __name__ == "__main__":
    start_data_node()
