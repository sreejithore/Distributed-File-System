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
    # Input boxes for the IP and Port of your Master Node [cite: 104]
    master_ip = st.text_input("Master Node IP", "127.0.0.1")
    master_port = st.text_input("Port", "5000")
    
    # A button to establish the initial connection [cite: 105]
    if st.button("Connect"):
        st.session_state['connected'] = True
        st.success(f"Connected to Master at {master_ip}:{master_port}!")
    
    if st.session_state['connected']:
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
            # The "Behind the Scenes" Console
            with st.status("Processing Upload...", expanded=True) as status:
                st.write("1. Reading file into memory...")
                file_bytes = uploaded_file.getvalue()
                
                st.write(f"2. Splitting '{uploaded_file.name}' into 2MB chunks...")
                # Call our logic function
                chunk_names = split_file(file_bytes, uploaded_file.name)
                time.sleep(1) # Simulating network delay
                
                st.write("3. Contacting Master for node addresses...")
                
                # Connect to the Master Node over the network
                master_url = f"http://{st.session_state.get('master_ip', '127.0.0.1')}:{st.session_state.get('master_port', '5000')}"
                master_conn = xmlrpc.client.ServerProxy(master_url)
                
                # Prepare the chunk data format the Master expects
                # (For now, we will use a dummy IP for the node_ip until we build Module 3)
                chunk_data = [{'chunk_name': name, 'node_ip': '127.0.0.1:5001'} for name in chunk_names]
                
                # Call the Master's function over the network!
                master_conn.register_file_chunks(uploaded_file.name, chunk_data)
                
                st.write(f"4. Sending {len(chunk_names)} chunks to Data Nodes...")
                
                status.update(label="Upload Complete!", state="complete", expanded=False)
                st.success(f"{uploaded_file.name} successfully chunked and stored!")
with col2:
    st.subheader("Files in DFS")
    
    if st.session_state['connected']:
        try:
            # Connect to Master
            master_url = f"http://{st.session_state.get('master_ip', '127.0.0.1')}:{st.session_state.get('master_port', '5000')}"
            master_conn = xmlrpc.client.ServerProxy(master_url)
            
            # Fetch the live directory from the Master's SQLite database
            live_registry = master_conn.get_file_directory()
            
            if live_registry:
                for filename, status in live_registry.items():
                    st.markdown(f"**📄 {filename}** ({status})")
                    
                    if st.button(f"Download {filename}"):
                        with st.spinner("Fetching chunks..."):
                            # Ask Master where the chunks are
                            chunk_locations = master_conn.get_chunk_locations(filename)
                            
                            # Extract just the chunk names to pass to our stitcher
                            chunk_names = [loc[0] for loc in chunk_locations]
                            
                            success, path = stitch_file(filename, chunk_names)
                            if success:
                                st.success(f"File downloaded successfully to: {path}")
                            else:
                                st.error(path)
            else:
                st.info("No files currently in the system.")
        except ConnectionRefusedError:
            st.error("Lost connection to Master Node.")
    else:
        st.info("Please connect to the Master Node to view files.")
