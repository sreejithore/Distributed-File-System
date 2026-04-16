import streamlit as st
import time
import xmlrpc.client
from client_logic import split_file, stitch_file

# --- INITIALIZE SESSION STATE ---
if 'connected' not in st.session_state:
    st.session_state['connected'] = False
if 'file_registry' not in st.session_state:
    # Mocking the Master Node's database
    st.session_state['file_registry'] = {} 

st.set_page_config(page_title="DFS Client Gateway", layout="wide")
st.title("Distributed File System Dashboard")

# --- 1. SIDEBAR: System Control & Status ---
with st.sidebar:
    st.header("System Connection")
    # Input boxes for the IP and Port of your Master Node
    master_ip = st.text_input("Master Node IP", "127.0.0.1")
    master_port = st.text_input("Port", "5000")
    
    # A button to establish the initial connection
    if st.button("Connect"):
        st.session_state['connected'] = True
        # Save the typed IP and Port into session state so the upload logic can use them later
        st.session_state['master_ip'] = master_ip
        st.session_state['master_port'] = master_port
        # ---------------------------------
        
        st.success(f"Connected to Master at {master_ip}:{master_port}!")
    
    if st.session_state.get('connected', False):
        st.success("🟢 System Status: ONLINE")
    else:
        st.error("🔴 System Status: OFFLINE")

# --- 2. MAIN DASHBOARD: User Actions ---
col1, col2 = st.columns(2)
with col1:
    st.subheader("Upload a File")
    # Use st.file_uploader()
    uploaded_file = st.file_uploader("Choose a file to upload to DFS")

    if uploaded_file is not None and st.session_state.get('connected', False):
        if st.button("Upload to DFS"):
            with st.status("Processing Upload...", expanded=True) as status:
                st.write("1. Reading file into memory...")
                file_bytes = uploaded_file.getvalue()
                
                st.write(f"2. Splitting '{uploaded_file.name}' into chunks...")
                chunks = split_file(file_bytes, uploaded_file.name)
                
                # Connect to Master
                master_url = f"http://{st.session_state.get('master_ip', '127.0.0.1')}:{st.session_state.get('master_port', '5000')}"
                master_conn = xmlrpc.client.ServerProxy(master_url)
                
                st.write("3. Asking Master for active Data Nodes...")
                active_nodes = master_conn.get_active_nodes()
                
                # Safety check: Prevent upload if the whole cluster is down!
                if not active_nodes:
                    status.update(label="Upload Failed!", state="error", expanded=False)
                    st.error("⚠️ Both Data Nodes are currently unavailable. Please wait and try again after some time.")
                    st.stop() # Stops the script from continuing
                
                st.write(f"   -> Found {len(active_nodes)} live nodes: {active_nodes}")
                
                st.write("4. Assigning chunks (Replication Factor = 2)...")
                metadata = []
                REPLICATION_FACTOR = 2
                
                # Make sure we don't try to replicate more times than we have nodes!
                rep_factor = min(REPLICATION_FACTOR, len(active_nodes))
                
                for index, chunk in enumerate(chunks):
                    assigned_nodes = []
                    # Pick a primary node and a backup node
                    for r in range(rep_factor):
                        target_node = active_nodes[(index + r) % len(active_nodes)]
                        assigned_nodes.append(target_node)
                        
                        # Tell the Master about ALL copies
                        metadata.append({'chunk_name': chunk['chunk_name'], 'node_ip': target_node})
                        
                    chunk['assigned_nodes'] = assigned_nodes # Save for the transfer step
                
                st.write("5. Registering map with Master Database...")
                master_conn.register_file_chunks(uploaded_file.name, metadata)
                
                st.write("6. Transferring data to Worker Nodes (Primary + Backup)...")
                for chunk in chunks:
                    for target_node in chunk['assigned_nodes']:
                        try:
                            node_conn = xmlrpc.client.ServerProxy(f"http://{target_node}")
                            binary_wrapper = xmlrpc.client.Binary(chunk['raw_bytes'])
                            node_conn.store_chunk(chunk['chunk_name'], binary_wrapper)
                        except Exception as e:
                            st.warning(f"Failed to send {chunk['chunk_name']} to {target_node}")
                
                status.update(label="Upload Complete!", state="complete", expanded=False)
                st.success(f"{uploaded_file.name} successfully replicated across the cluster!")

with col2:
    st.subheader("Files in DFS")
    
    # Only try to fetch files if the system is currently connected
    if st.session_state.get('connected', False):
        try:
            # Connect to the Master Node over the network
            master_url = f"http://{st.session_state.get('master_ip', '127.0.0.1')}:{st.session_state.get('master_port', '5000')}"
            master_conn = xmlrpc.client.ServerProxy(master_url)
            
            # Fetch the live directory from the Master's SQLite database
            live_registry = master_conn.get_file_directory()
            
            # Display the files with Download and Delete buttons
            if live_registry:
                for filename, status in live_registry.items():
                    
                    # Create a mini-grid: 3 parts text, 1 part Download, 1 part Delete
                    text_col, dl_col, del_col = st.columns([3, 1, 1])
                    
                    with text_col:
                        st.markdown(f"**📄 {filename}** ({status})")
                    
                    with dl_col:
                        if st.button("Download", key=f"dl_btn_{filename}"):
                            
                            # ---> NEW: Check if the cluster is alive BEFORE trying to download <---
                            active_nodes = master_conn.get_active_nodes()
                            
                            if not active_nodes:
                                st.error("⚠️ Both Data Nodes are currently unavailable. Please wait and try again after some time.")
                            else:
                                with st.spinner(f"Fetching chunks for {filename}..."):
                                    import os
                                    
                                    # Ask Master where the chunks are
                                    chunk_locations = master_conn.get_chunk_locations(filename)
                                    
                                    # Ensure we have a unique list of chunk names to loop through
                                    chunk_names = list(dict.fromkeys([loc[0] for loc in chunk_locations]))
                                    
                                    # Create a downloads folder if it doesn't exist
                                    if not os.path.exists("downloads"):
                                        os.makedirs("downloads")
                                    
                                    save_path = f"downloads/recovered_{filename}"
                                    
                                    # Download from Data Nodes and stitch together
                                    try:
                                        # Group the locations by chunk name so we know our backups
                                        # Example: {'part1': ['127.0.0.1:5001', '127.0.0.1:5002']}
                                        chunk_map = {}
                                        for c_name, n_ip in chunk_locations:
                                            if c_name not in chunk_map:
                                                chunk_map[c_name] = []
                                            chunk_map[c_name].append(n_ip)
                                            
                                        with open(save_path, 'wb') as outfile:
                                            for chunk_name in chunk_names:
                                                chunk_recovered = False
                                                
                                                # Try each node that holds this chunk until one works
                                                for target_node in chunk_map.get(chunk_name, []):
                                                    try:
                                                        node_conn = xmlrpc.client.ServerProxy(f"http://{target_node}")
                                                        chunk_data = node_conn.get_chunk(chunk_name)
                                                        
                                                        outfile.write(chunk_data.data)
                                                        chunk_recovered = True
                                                        break # Success! Stop trying backups for this chunk.
                                                    except Exception:
                                                        print(f"[WARNING] Node {target_node} is down. Trying backup...")
                                                
                                                if not chunk_recovered:
                                                    raise Exception(f"All nodes holding {chunk_name} are completely offline!")
                                                
                                        st.success(f"Successfully downloaded to: {save_path}")
                                    except Exception as e:
                                        st.error(f"Download failed: {e}")

                    with del_col:
                        # Use type="primary" to make the Delete button stand out (red)
                        if st.button("Delete", type="primary", key=f"del_btn_{filename}"):
                            with st.spinner("Purging file from cluster..."):
                                try:
                                    # 1. Ask Master where ALL copies of the chunks live
                                    chunk_locations = master_conn.get_chunk_locations(filename)
                                    
                                    # 2. Tell every Data Node holding a piece to physically delete it
                                    for chunk_name, node_ip in chunk_locations:
                                        try:
                                            node_conn = xmlrpc.client.ServerProxy(f"http://{node_ip}")
                                            node_conn.delete_chunk(chunk_name)
                                        except Exception:
                                            # If a node is currently offline, we just skip it for now
                                            print(f"⚠️ Could not reach {node_ip} to delete {chunk_name}")
                                            
                                    # 3. Wipe the memory from the Master's database
                                    success = master_conn.delete_file_metadata(filename)
                                    
                                    if success:
                                        st.success("File completely purged!")
                                        import time
                                        time.sleep(1) # Pause so the user sees the success message
                                        st.rerun() # Instantly refresh UI so it disappears from the list
                                    else:
                                        st.error("Failed to delete metadata from Master.")
                                except Exception as e:
                                    st.error(f"Deletion error: {e}")
            else:
                st.info("ℹ️ No files currently in the system.")
                
        except ConnectionRefusedError:
            st.error("Lost connection to Master Node.")
    else:
        st.info("Please connect to the Master Node to view files.")
