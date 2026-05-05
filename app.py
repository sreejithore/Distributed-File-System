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

# --- 1. SIDEBAR: Navigation & Connection ---
with st.sidebar:
    st.header("Navigation")
    app_page = st.radio("Go To:", ["Main Dashboard", "Storage Analytics"])
    
    st.divider()
    
    st.header("System Connection")
    master_ip = st.text_input("Master Node IP", "127.0.0.1")
    master_port = st.text_input("Port", "5000")
    
    if st.button("Connect"):
        st.session_state['connected'] = True
        st.session_state['master_ip'] = master_ip
        st.session_state['master_port'] = master_port
        st.toast(f"✅ Connected to Master at {master_ip}:{master_port}!")
    
    if st.session_state.get('connected', False):
        st.success("🟢 System Status: ONLINE")
    else:
        st.error("🔴 System Status: OFFLINE")

# ==========================================
#         FRAGMENT 1: NODE STATUS
# ==========================================
@st.fragment(run_every=2)
def display_node_status():
    st.subheader("🖥️ Live Cluster Health")
    if st.session_state.get('connected', False):
        try:
            master_url = f"http://{st.session_state.get('master_ip', '127.0.0.1')}:{st.session_state.get('master_port', '5000')}"
            master_conn = xmlrpc.client.ServerProxy(master_url)
            active_nodes = master_conn.get_active_nodes()
            
            health_col1, health_col2 = st.columns(2)
            with health_col1:
                if "127.0.0.1:5001" in active_nodes:
                    st.success("### 🟢 Node A : **ACTIVE**")
                else:
                    st.error("### 🔴 Node A : **OFFLINE**")
                    
            with health_col2:
                if "127.0.0.1:5002" in active_nodes:
                    st.success("### 🟢 Node B : **ACTIVE**")
                else:
                    st.error("### 🔴 Node B : **OFFLINE**")
        except Exception:
            st.warning("⚠️ Cannot communicate with Master Node.")
    else:
        st.info("Please connect to the Master Node in the sidebar.")
    st.markdown("---")

# ==========================================
#         FRAGMENT 2: ANALYTICS PAGE
# ==========================================
@st.fragment(run_every=2)
def display_analytics():
    if st.session_state.get('connected', False):
        try:
            master_url = f"http://{st.session_state.get('master_ip', '127.0.0.1')}:{st.session_state.get('master_port', '5000')}"
            master_conn = xmlrpc.client.ServerProxy(master_url)
            
            active_nodes = master_conn.get_active_nodes()
            cluster_stats = master_conn.get_cluster_stats()
            
            st.markdown("### 📊 Storage Analytics Overview")
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
                
            st.divider()
            
            if node_storage:
                st.markdown("**Data Distribution Across Nodes (MB)**")
                import pandas as pd
                chart_data = pd.DataFrame(
                    list(node_storage.values()), 
                    index=list(node_storage.keys()), 
                    columns=["Storage Used (MB)"]
                )
                st.bar_chart(chart_data, height=350)
            else:
                st.info("ℹ️ No data is currently stored in the cluster.")
        except Exception:
            st.warning("⚠️ Cannot fetch analytics from Master Node.")
    else:
        st.info("Please connect to the Master Node to view analytics.")


# ==========================================
#         MODAL: IMAGE PREVIEW DIALOG
# ==========================================
# ---> NEW: This creates a beautiful pop-up that closes if you click outside of it!
@st.dialog("🖼️ Live Image Preview", width="large")
def show_preview_dialog(filename, master_ip, master_port):
    try:
        master_url = f"http://{master_ip}:{master_port}"
        master_conn = xmlrpc.client.ServerProxy(master_url)
        
        active_nodes = master_conn.get_active_nodes()
        if not active_nodes:
            st.error("⚠️ Cluster Offline. Cannot fetch preview.")
            return
            
        with st.spinner(f"Fetching '{filename}' from RAM..."):
            chunk_locations = master_conn.get_chunk_locations(filename)
            chunk_names = list(dict.fromkeys([loc[0] for loc in chunk_locations]))
            chunk_map = {}
            for c_name, n_ip, c_hash in chunk_locations:
                if c_name not in chunk_map:
                    chunk_map[c_name] = {'nodes': [], 'hash': c_hash}
                chunk_map[c_name]['nodes'].append(n_ip)
                
            file_bytes = bytearray()
            for chunk_name in chunk_names:
                chunk_recovered = False
                expected_hash = chunk_map[chunk_name]['hash']
                
                for target_node in chunk_map[chunk_name]['nodes']:
                    try:
                        node_conn = xmlrpc.client.ServerProxy(f"http://{target_node}")
                        chunk_data = node_conn.get_chunk(chunk_name)
                        if hashlib.sha256(chunk_data.data).hexdigest() == expected_hash:
                            file_bytes.extend(chunk_data.data) 
                            chunk_recovered = True
                            break
                    except Exception:
                        pass
                
                if not chunk_recovered:
                    raise Exception("Missing or corrupt chunks prevented preview.")
            
            # Displays the image cleanly inside the modal
            st.image(bytes(file_bytes), use_container_width=True)
            
    except Exception as e:
        st.error(f"❌ Preview failed: {e}")


# ==========================================
#               PAGE ROUTING
# ==========================================

if app_page == "Main Dashboard":
    st.title("Distributed File System Dashboard")
    display_node_status()

    col1, col2 = st.columns(2)
    
    # --- UPLOAD SECTION ---
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
                    
                    active_nodes = master_conn.get_active_nodes()
                    
                    if not active_nodes:
                        status.update(label="Upload Failed!", state="error", expanded=False)
                        st.toast("⚠️ Both Data Nodes are currently unavailable.", icon="🚨")
                        st.stop()
                    
                    st.write(f"   -> Found {len(active_nodes)} live nodes")
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
                    
                    st.write("5. Registering map with Master...")
                    master_conn.register_file_chunks(uploaded_file.name, metadata)
                    
                    st.write("6. Transferring data to Worker Nodes...")
                    for chunk in chunks:
                        for target_node in chunk['assigned_nodes']:
                            try:
                                node_conn = xmlrpc.client.ServerProxy(f"http://{target_node}")
                                binary_wrapper = xmlrpc.client.Binary(chunk['raw_bytes'])
                                node_conn.store_chunk(chunk['chunk_name'], binary_wrapper)
                            except Exception:
                                st.toast(f"⚠️ Failed to send {chunk['chunk_name']} to {target_node}")
                    
                    status.update(label="Upload Complete!", state="complete", expanded=False)
                    st.toast(f"✅ {uploaded_file.name} successfully replicated!")

    # --- FILE LIST SECTION (SCROLLABLE) ---
    with col2:
        st.subheader("Files in DFS")
        
        if st.session_state.get('connected', False):
            try:
                master_url = f"http://{st.session_state.get('master_ip', '127.0.0.1')}:{st.session_state.get('master_port', '5000')}"
                master_conn = xmlrpc.client.ServerProxy(master_url)
                live_registry = master_conn.get_file_directory()
                
                if live_registry:
                    with st.container(height=400, border=True):
                        for filename, status in live_registry.items():
                            
                            text_col, prev_col, dl_col, del_col = st.columns([3, 1, 1, 1])
                            
                            with text_col:
                                st.markdown(f"**📄 {filename}** ({status})")
                                
                            with prev_col:
                                is_image = filename.lower().endswith(('.png', '.jpg', '.jpeg', '.webp', '.gif'))
                                preview_clicked = st.button("Preview", key=f"prev_btn_{filename}", disabled=not is_image)
                            
                            with dl_col:
                                if st.button("Download", key=f"dl_btn_{filename}"):
                                    active_nodes = master_conn.get_active_nodes()
                                    if not active_nodes:
                                        st.toast("⚠️ Both Data Nodes are unavailable.", icon="🚨")
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
                                                                    st.toast(f"⚠️ CORRUPTION DETECTED on {target_node}!", icon="🚨")
                                                            except Exception:
                                                                pass
                                                        
                                                        if not chunk_recovered:
                                                            raise Exception(f"All nodes offline/corrupt for {chunk_name}!")
                                                        
                                                st.toast(f"✅ Successfully downloaded {filename}!")
                                            except Exception as e:
                                                st.toast(f"❌ Download failed: {e}")

                            with del_col:
                                if st.button("Delete", type="primary", key=f"del_btn_{filename}"):
                                    with st.spinner("Purging file..."):
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
                                                st.toast("❌ Failed to delete metadata.")
                                        except Exception as e:
                                            st.toast(f"❌ Deletion error: {e}")
                                            
                            # ---> NEW: Calling the Dialog Box!
                            if preview_clicked:
                                show_preview_dialog(filename, st.session_state['master_ip'], st.session_state['master_port'])
                            
                            st.divider()
                else:
                    st.info("ℹ️ No files currently in the system.")
                    
            except ConnectionRefusedError:
                st.error("Lost connection to Master Node.")
        else:
            st.info("Please connect to the Master Node to view files.")

elif app_page == "Storage Analytics":
    st.title("System Analytics")
    display_analytics()
