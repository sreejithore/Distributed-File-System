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

    if uploaded_file is not None and st.session_state['connected']:
        if st.button("Upload to DFS"):
            with st.status("Processing Upload...", expanded=True) as status:
                st.write("1. Reading file into memory...")
                file_bytes = uploaded_file.getvalue()
                
                st.write(f"2. Splitting '{uploaded_file.name}' into 2MB chunks...")
                chunks = split_file(file_bytes, uploaded_file.name)
                
                # Connect to Master
                master_url = f"http://{st.session_state.get('master_ip', '127.0.0.1')}:{st.session_state.get('master_port', '5000')}"
                master_conn = xmlrpc.client.ServerProxy(master_url)
                
                st.write("3. Asking Master for active Data Nodes...")
                active_nodes = master_conn.get_active_nodes()
                
                # Safety check: Prevent upload if the whole cluster is down!
                if not active_nodes:
                    status.update(label="Upload Failed!", state="error", expanded=False)
                    st.error("No active Data Nodes found. Is your cluster running?")
                    st.stop() # Stops the script from continuing
                
                st.write(f"   -> Found {len(active_nodes)} live nodes: {active_nodes}")
                
                st.write("4. Assigning chunks (Load Balancing)...")
                metadata = []
                # Distribute chunks evenly using the modulo operator (Round-Robin)
                for index, chunk in enumerate(chunks):
                    # If you have 2 nodes, index 0 goes to Node A, index 1 to Node B, index 2 to Node A...
                    target_node = active_nodes[index % len(active_nodes)]
                    
                    metadata.append({'chunk_name': chunk['chunk_name'], 'node_ip': target_node})
                    chunk['assigned_node'] = target_node # Save the target for the next step
                
                st.write("5. Registering map with Master Database...")
                master_conn.register_file_chunks(uploaded_file.name, metadata)
                
                st.write("6. Transferring data directly to Worker Nodes...")
                for chunk in chunks:
                    target_node = chunk['assigned_node']
                    node_conn = xmlrpc.client.ServerProxy(f"http://{target_node}")
                    
                    # Wrap and send
                    binary_wrapper = xmlrpc.client.Binary(chunk['raw_bytes'])
                    node_conn.store_chunk(chunk['chunk_name'], binary_wrapper)
                
                status.update(label="Upload Complete!", state="complete", expanded=False)
                st.success(f"{uploaded_file.name} successfully distributed across the cluster!")
                
with col2:
    st.subheader("Files in DFS")
    
    # Only try to fetch files if the system is currently connected
    if st.session_state.get('connected', False):
        try:
            # 1. Connect to the Master Node over the network
            master_url = f"http://{st.session_state.get('master_ip', '127.0.0.1')}:{st.session_state.get('master_port', '5000')}"
            master_conn = xmlrpc.client.ServerProxy(master_url)
            
            # 2. Fetch the live directory from the Master's SQLite database
            live_registry = master_conn.get_file_directory()
            
            # 3. Display the files with Download buttons
            if live_registry:
                # Loop through every file in the registry
                for filename, status in live_registry.items():
                    
                    # Create a mini-grid: 3 parts for the text, 1 part for the button
                    text_col, button_col = st.columns([3, 1])
                    
                    with text_col:
                        st.markdown(f"**📄 {filename}** ({status})")

                    with button_col:
                        if st.button("Download", key=f"dl_btn_{filename}"):
                            with st.spinner(f"Fetching chunks for {filename}..."):
                                import os
                                
                                # 1. Ask Master where the chunks are
                                chunk_locations = master_conn.get_chunk_locations(filename)
                                
                                # Create a downloads folder if it doesn't exist
                                if not os.path.exists("downloads"):
                                    os.makedirs("downloads")
                                
                                save_path = f"downloads/recovered_{filename}"
                                
                                # 2. Download from Data Nodes and stitch together
                                try:
                                    with open(save_path, 'wb') as outfile:
                                        for chunk_name, node_ip in chunk_locations:
                                            # Connect to the specific Data Node holding this chunk
                                            node_conn = xmlrpc.client.ServerProxy(f"http://{node_ip}")
                                            
                                            # Fetch the binary data over the network
                                            chunk_data = node_conn.get_chunk(chunk_name)
                                            
                                            # Write it to our final stitched file
                                            outfile.write(chunk_data.data)
                                            
                                    st.success(f"Successfully downloaded to: {save_path}")
                                except Exception as e:
                                    st.error(f"Download failed. A Data Node might be offline: {e}")
            else:
                st.info("ℹ️ No files currently in the system.")
                
        except ConnectionRefusedError:
            st.error("Lost connection to Master Node.")
    else:
        st.info("Please connect to the Master Node to view files.")
