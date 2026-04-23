import streamlit as st
import time
import os
import hashlib
import xmlrpc.client
from client_logic import split_file, stitch_file

# --- INITIALIZE SESSION STATE ---
if 'connected' not in st.session_state:
    st.session_state['connected'] = False
if 'file_registry' not in st.session_state:
    st.session_state['file_registry'] = {} 

st.set_page_config(page_title="DFS Client Gateway", layout="wide")
st.title("Distributed File System Dashboard")

# --- 1. SIDEBAR: System Control & Status ---
with st.sidebar:
    st.header("System Connection")
    master_ip = st.text_input("Master Node IP", "127.0.0.1")
    master_port = st.text_input("Port", "5000")
    
    if st.button("Connect"):
        st.session_state['connected'] = True
        st.session_state['master_ip'] = master_ip
        st.session_state['master_port'] = master_port
        st.success(f"Connected to Master at {master_ip}:{master_port}!")
    
    if st.session_state.get('connected', False):
        st.success("🟢 System Status: ONLINE")
    else:
        st.error("🔴 System Status: OFFLINE")

# --- CLUSTER HEALTH DASHBOARD ---
@st.fragment(run_every=2)
def display_cluster_health():
    st.markdown("---")
    st.subheader("🖥️ Live Cluster Health & Analytics")
    
    if st.session_state.get('connected', False):
        try:
            master_url = f"http://{st.session_state.get('master_ip', '127.0.0.1')}:{st.session_state.get('master_port', '5000')}"
            master_conn = xmlrpc.client.ServerProxy(master_url)
            
            active_nodes = master_conn.get_active_nodes()
            cluster_stats = master_conn.get_cluster_stats()
            
            # --- 1. HEALTH INDICATORS ---
            health_col1, health_col2 = st.columns(2)
            
            with health_col1:
                if "127.0.0.1:5001" in active_nodes:
                    st.success("### 🟢 Node A (5001): **ACTIVE**")
                else:
                    st.error("### 🔴 Node A (5001): **OFFLINE**")
                    
            with health_col2:
                if "127.0.0.1:5002" in active_nodes:
                    st.success("### 🟢 Node B (5002): **ACTIVE**")
                else:
                    st.error("### 🔴 Node B (5002): **OFFLINE**")
            
            # --- 2. LIVE METRICS ---
            st.markdown("### 📊 Storage Analytics")
            metric_col1, metric_col2, metric_col3 = st.columns(3)
            
            total_files = cluster_stats.get("total_files", 0)
            rep_factor = cluster_stats.get("replication_factor", 2)
            node_storage = cluster_stats.get("node_storage_mb", {})
            network_capacity = len(active_nodes) * 500
            
            with metric_col1:
                st.metric("Total Files Stored", total_files)
            with metric_col2:
                st.metric("Current Replication Factor", rep_factor)
            with metric_col3:
                st.metric("Total Network Capacity", f"{network_capacity} MB")
                
            # --- 3. DATA DISTRIBUTION CHART ---
            if node_storage:
                st.markdown("**Data Distribution Across Nodes (MB)**")
                import pandas as pd
                chart_data = pd.DataFrame(
                    list(node_storage.values()), 
                    index=list(node_storage.keys()), 
                    columns=["Storage Used (MB)"]
                )
                st.bar_chart(chart_data, height=200)
            else:
                st.info("ℹ️ No data is currently stored in the cluster.")
                
        except Exception as e:
            st.warning("⚠️ Cannot communicate with Master Node to check cluster health.")
    else:
        st.info("Please connect to the Master Node in the sidebar to view live cluster health.")
    
    st.markdown("---")

display_cluster_health()

# --- 2. MAIN DASHBOARD: User Actions ---
col1, col2 = st.columns(2)
with col1:
    st.subheader("Upload a File")
    uploaded_file = st.file_uploader("Choose a file to upload to DFS")

    if uploaded_file is not None and st.session_state.get('connected', False):
        if st.button("Upload to DFS"):
            with st.status("Processing Upload...", expanded=True) as status:
                st.write("1. Reading file into memory...")
                file_bytes = uploaded_file.getvalue()
                
                st.write(f"2. Splitting '{uploaded_file.name}' into chunks...")
                chunks = split_file(file_bytes, uploaded_file.name)
                
                master_url = f"http://{st.session_state.get('master_ip', '127.0.0.1')}:{st.session_state.get('master_port', '5000')}"
                master_conn = xmlrpc.client.ServerProxy(master_url)
                
                st.write("3. Asking Master for active Data Nodes...")
                active_nodes = master_conn.get_active_nodes()
                
                if not active_nodes:
                    status.update(label="Upload Failed!", state="error", expanded=False)
                    st.error("⚠️ Both Data Nodes are currently unavailable. Please wait and try again after some time.")
                    st.stop()
                
                st.write(f"   -> Found {len(active_nodes)} live nodes: {active_nodes}")
                st.write("4. Assigning chunks (Replication Factor = 2)...")
                
                metadata = []
                REPLICATION_FACTOR = 2
                rep_factor = min(REPLICATION_FACTOR, len(active_nodes))
                
                for index, chunk in enumerate(chunks):
                    assigned_nodes = []
                    for r in range(rep_factor):
                        target_node = active_nodes[(index + r) % len(active_nodes)]
                        assigned_nodes.append(target_node)
                        metadata.append({
                            'chunk_name': chunk['chunk_name'], 
                            'node_ip': target_node,
                            'hash': chunk['hash'] 
                        })
                    chunk['assigned_nodes'] = assigned_nodes
                
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
                st.toast(f"✅ {uploaded_file.name} successfully replicated!")

with col2:
    st.subheader("Files in DFS")
    
    if st.session_state.get('connected', False):
        try:
            master_url = f"http://{st.session_state.get('master_ip', '127.0.0.1')}:{st.session_state.get('master_port', '5000')}"
            master_conn = xmlrpc.client.ServerProxy(master_url)
            
            live_registry = master_conn.get_file_directory()
            
            if live_registry:
                for filename, status in live_registry.items():
                    
                    # ---> NEW: 4-Column Layout to fit the Preview button
                    text_col, prev_col, dl_col, del_col = st.columns([3, 1, 1, 1])
                    
                    with text_col:
                        st.markdown(f"**📄 {filename}** ({status})")
                        
                    # ---> NEW: In-Memory Image Preview Logic
                    with prev_col:
                        # Smart check to disable button if the file is not an image
                        is_image = filename.lower().endswith(('.png', '.jpg', '.jpeg', '.webp', '.gif'))
                        preview_clicked = st.button("Preview", key=f"prev_btn_{filename}", disabled=not is_image)
                    
                    with dl_col:
                        if st.button("Download", key=f"dl_btn_{filename}"):
                            active_nodes = master_conn.get_active_nodes()
                            if not active_nodes:
                                st.error("⚠️ Both Data Nodes are currently unavailable.")
                            else:
                                with st.spinner(f"Fetching chunks for {filename}..."):
                                    chunk_locations = master_conn.get_chunk_locations(filename)
                                    chunk_names = list(dict.fromkeys([loc[0] for loc in chunk_locations]))
                                    
                                    if not os.path.exists("downloads"):
                                        os.makedirs("downloads")
                                    
                                    save_path = f"downloads/recovered_{filename}"
                                    
                                    try:
                                        chunk_map = {}
                                        for c_name, n_ip, c_hash in chunk_locations:
                                            if c_name not in chunk_map:
                                                chunk_map[c_name] = {'nodes': [], 'hash': c_hash}
                                            chunk_map[c_name]['nodes'].append(n_ip)
                                            
                                        with open(save_path, 'wb') as outfile:
                                            for chunk_name in chunk_names:
                                                chunk_recovered = False
                                                expected_hash = chunk_map[chunk_name]['hash']
                                                
                                                for target_node in chunk_map[chunk_name]['nodes']:
                                                    try:
                                                        node_conn = xmlrpc.client.ServerProxy(f"http://{target_node}")
                                                        chunk_data = node_conn.get_chunk(chunk_name)
                                                        downloaded_hash = hashlib.sha256(chunk_data.data).hexdigest()
                                                        
                                                        if downloaded_hash == expected_hash:
                                                            outfile.write(chunk_data.data)
                                                            chunk_recovered = True
                                                            break
                                                        else:
                                                            st.warning(f"⚠️ CORRUPTION DETECTED on {target_node}!")
                                                    except Exception:
                                                        pass
                                                
                                                if not chunk_recovered:
                                                    raise Exception(f"All nodes holding {chunk_name} are offline/corrupt!")
                                                
                                        st.toast(f"✅ Successfully downloaded {filename}!")
                                    except Exception as e:
                                        st.error(f"Download failed: {e}")

                    with del_col:
                        if st.button("Delete", type="primary", key=f"del_btn_{filename}"):
                            with st.spinner("Purging file from cluster..."):
                                try:
                                    chunk_locations = master_conn.get_chunk_locations(filename)
                                    for loc in chunk_locations:
                                        try:
                                            node_conn = xmlrpc.client.ServerProxy(f"http://{loc[1]}")
                                            node_conn.delete_chunk(loc[0])
                                        except Exception:
                                            pass
                                            
                                    success = master_conn.delete_file_metadata(filename)
                                    if success:
                                        st.toast("🗑️ File completely purged!")
                                        time.sleep(1) 
                                        st.rerun() 
                                    else:
                                        st.error("Failed to delete metadata.")
                                except Exception as e:
                                    st.error(f"Deletion error: {e}")
                                    
                    # ---> RENDERING THE PREVIEW (Displays underneath the file entry if clicked)
                    if preview_clicked:
                        active_nodes = master_conn.get_active_nodes()
                        if not active_nodes:
                            st.error("⚠️ Cluster Offline.")
                        else:
                            with st.spinner("Fetching chunks from RAM..."):
                                try:
                                    chunk_locations = master_conn.get_chunk_locations(filename)
                                    chunk_names = list(dict.fromkeys([loc[0] for loc in chunk_locations]))
                                    chunk_map = {}
                                    for c_name, n_ip, c_hash in chunk_locations:
                                        if c_name not in chunk_map:
                                            chunk_map[c_name] = {'nodes': [], 'hash': c_hash}
                                        chunk_map[c_name]['nodes'].append(n_ip)
                                        
                                    # Use a bytearray to act as an in-memory canvas
                                    file_bytes = bytearray()
                                    
                                    for chunk_name in chunk_names:
                                        chunk_recovered = False
                                        expected_hash = chunk_map[chunk_name]['hash']
                                        
                                        for target_node in chunk_map[chunk_name]['nodes']:
                                            try:
                                                node_conn = xmlrpc.client.ServerProxy(f"http://{target_node}")
                                                chunk_data = node_conn.get_chunk(chunk_name)
                                                downloaded_hash = hashlib.sha256(chunk_data.data).hexdigest()
                                                if downloaded_hash == expected_hash:
                                                    file_bytes.extend(chunk_data.data) # Append bytes into memory
                                                    chunk_recovered = True
                                                    break
                                            except Exception:
                                                pass
                                        
                                        if not chunk_recovered:
                                            raise Exception("Missing or corrupt chunks prevented preview.")
                                    
                                    # Pass the compiled bytes directly into Streamlit's image renderer
                                    st.image(file_bytes, caption=f"Live Preview: {filename}", use_container_width=True)
                                except Exception as e:
                                    st.error(f"Preview failed: {e}")
                    
                    st.divider() # Adds a clean line below each file and its preview
                                    
            else:
                st.info("ℹ️ No files currently in the system.")
                
        except ConnectionRefusedError:
            st.error("Lost connection to Master Node.")
    else:
        st.info("Please connect to the Master Node to view files.")
