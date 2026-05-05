import streamlit as st
import time
import os
import hashlib
import xmlrpc.client
import pandas as pd
import altair as alt
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
                    st.success("### 🟢 Node A (5001): **ACTIVE**")
                else:
                    st.error("### 🔴 Node A (5001): **OFFLINE**")
                    
            with health_col2:
                if "127.0.0.1:5002" in active_nodes:
                    st.success("### 🟢 Node B (5002): **ACTIVE**")
                else:
                    st.error("### 🔴 Node B (5002): **OFFLINE**")
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
                chart_data = pd.DataFrame(
                    list(node_storage.values()), 
                    index=list(node_storage.keys()), 
                    columns=["Storage Used (MB)"]
                )
                st.bar_chart(chart_data, height=350)
            else:
                st.info("ℹ️ No data is currently stored in the cluster.")
                
            st.divider()
            
            timeline_data = master_conn.get_storage_timeline()
            
            if timeline_data:
                st.markdown("### 📈 Storage Growth Over Time")
                
                df = pd.DataFrame(timeline_data, columns=["Node", "Size", "Timestamp"])
                df['Node'] = df['Node'].apply(lambda x: f"Node {x.split(':')[-1]}") 
                df['Size (MB)'] = df['Size'] / (1024 * 1024) 
                
                import time as sys_time
                current_unix_time = sys_time.time()
                present_rows = []
                for node in df['Node'].unique():
                    present_rows.append({"Node": node, "Size (MB)": 0.0, "Timestamp": current_unix_time})
                
                df = pd.concat([df, pd.DataFrame(present_rows)], ignore_index=True)
                
                df['Time'] = pd.to_datetime(df['Timestamp'], unit='s') 
                df = df.sort_values('Time')
                df['Total Storage (MB)'] = df.groupby('Node')['Size (MB)'].cumsum()
                
                line_chart = alt.Chart(df).mark_line(point=True, interpolate='step-after').encode(
                    x=alt.X('Time:T', title='Time of Upload / Replication'),
                    y=alt.Y('Total Storage (MB):Q', title='Total Storage (MB)'),
                    color=alt.Color('Node:N', legend=alt.Legend(title="Servers")),
                    tooltip=['Node', 'Time', 'Total Storage (MB)']
                ).properties(height=400).interactive()
                
                st.altair_chart(line_chart, use_container_width=True)
            else:
                st.info("ℹ️ No timeline data is currently available in the cluster.")

        except Exception as e:
            st.warning(f"⚠️ Cannot fetch analytics from Master Node: {e}")
    else:
        st.info("Please connect to the Master Node to view analytics.")


# ==========================================
#         MODAL 1: IMAGE PREVIEW
# ==========================================
@st.dialog("🖼️ Live Image Preview", width="large")
def show_preview_dialog(filename, version, master_ip, master_port): 
    try:
        master_url = f"http://{master_ip}:{master_port}"
        master_conn = xmlrpc.client.ServerProxy(master_url)
        
        active_nodes = master_conn.get_active_nodes()
        if not active_nodes:
            st.error("⚠️ Cluster Offline. Cannot fetch preview.")
            return
            
        with st.spinner(f"Fetching '{filename}' (v{version}) from RAM..."):
            chunk_locations = master_conn.get_chunk_locations(filename, version) 
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
            
            st.image(bytes(file_bytes), use_container_width=True)
            
    except Exception as e:
        st.error(f"❌ Preview failed: {e}")

# ==========================================
#      NEW MODAL 2: THE CHUNK MATRIX
# ==========================================
@st.dialog("⏬ Network Protocol: Reassembling File", width="large")
def download_matrix_dialog(filename, version, master_ip, master_port):
    """Visualizes the download and hash-verification process in a Hacker Matrix grid."""
    st.markdown(f"**Target:** `{filename}` (v{version})")
    
    try:
        master_url = f"http://{master_ip}:{master_port}"
        master_conn = xmlrpc.client.ServerProxy(master_url)
        active_nodes = master_conn.get_active_nodes()
        
        if not active_nodes:
            st.error("⚠️ Cluster Offline. Download aborted.")
            return

        chunk_locations = master_conn.get_chunk_locations(filename, version)
        chunk_names = list(dict.fromkeys([loc[0] for loc in chunk_locations]))
        
        if not chunk_names:
            st.error("⚠️ File metadata not found in Master Node.")
            return

        # 1. Initialize State Tracker (0: Pending, 1: Fetching, 2: Verified, -1: Corrupt)
        chunk_states = {name: 0 for name in chunk_names}
        
        # UI Placeholders for real-time updates
        progress_bar = st.progress(0)
        status_text = st.empty()
        grid_placeholder = st.empty()
        
        # The CSS for the Hacker Grid
        css_styles = """
        <style>
        .matrix-grid { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 15px; margin-bottom: 20px;}
        .chunk-box { width: 35px; height: 35px; border-radius: 4px; display: flex; align-items: center; justify-content: center; font-family: monospace; font-size: 11px; font-weight: bold; color: white; transition: all 0.2s ease-in-out; }
        .pending { background-color: #1e293b; border: 1px solid #334155; color: #64748b; }
        .fetching { background-color: #eab308; box-shadow: 0 0 12px #eab308; animation: pulse 0.8s infinite; color: black; border: 1px solid #fef08a;}
        .verified { background-color: #22c55e; box-shadow: 0 0 8px #22c55e; border: 1px solid #86efac; }
        .corrupt { background-color: #ef4444; box-shadow: 0 0 12px #ef4444; border: 1px solid #fca5a5; }
        @keyframes pulse { 0% { transform: scale(1); } 50% { transform: scale(1.15); } 100% { transform: scale(1); } }
        </style>
        """

        # Function to redraw the grid dynamically
        def render_grid():
            html = css_styles + "<div class='matrix-grid'>"
            for idx, name in enumerate(chunk_names):
                state = chunk_states[name]
                if state == 0:   css_class = "pending"
                elif state == 1: css_class = "fetching"
                elif state == 2: css_class = "verified"
                else:            css_class = "corrupt"
                
                html += f"<div class='chunk-box {css_class}'>{idx}</div>"
            html += "</div>"
            grid_placeholder.markdown(html, unsafe_allow_html=True)

        render_grid() # Draw initial gray grid

        # 2. Build map of where chunks live
        chunk_map = {}
        for c_name, n_ip, c_hash in chunk_locations:
            if c_name not in chunk_map:
                chunk_map[c_name] = {'nodes': [], 'hash': c_hash}
            chunk_map[c_name]['nodes'].append(n_ip)

        # 3. Create folder and begin transfer
        if not os.path.exists("downloads"):
            os.makedirs("downloads")
        save_path = f"downloads/recovered_v{version}_{filename}"

        with open(save_path, 'wb') as outfile:
            for idx, chunk_name in enumerate(chunk_names):
                # Update UI: Fetching
                chunk_states[chunk_name] = 1
                status_text.markdown(f"📡 **Fetching:** `{chunk_name}` ...")
                render_grid()
                
                # ARTIFICIAL DELAY: So we can actually see the UI matrix working on localhost
                time.sleep(0.38) 
                
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
                            # Update UI: Verified Green
                            chunk_states[chunk_name] = 2
                            break
                        else:
                            # Update UI: Corrupt Red
                            chunk_states[chunk_name] = -1
                            render_grid()
                            time.sleep(0.5) # Pause so user sees the red flash
                    except Exception:
                        pass
                
                if not chunk_recovered:
                    status_text.error(f"❌ FATAL: All nodes offline/corrupt for `{chunk_name}`!")
                    chunk_states[chunk_name] = -1
                    render_grid()
                    return # Abort download
                
                # Update progress bar
                progress = (idx + 1) / len(chunk_names)
                progress_bar.progress(progress)
                render_grid()

        status_text.success(f"✅ **Integrity Verified.** File saved to `/downloads`!")
        time.sleep(1.5)
        st.rerun() # Close modal and refresh UI

    except Exception as e:
        st.error(f"❌ Network Protocol Failed: {e}")

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
                    master_url = f"http://{st.session_state.get('master_ip', '127.0.0.1')}:{st.session_state.get('master_port', '5000')}"
                    master_conn = xmlrpc.client.ServerProxy(master_url)
                    
                    active_nodes = master_conn.get_active_nodes()
                    
                    if not active_nodes:
                        status.update(label="Upload Failed!", state="error", expanded=False)
                        st.toast("⚠️ Both Data Nodes are currently unavailable.", icon="🚨")
                        st.stop()

                    st.write("1. Reading file into memory...")
                    file_bytes = uploaded_file.getvalue()
                    
                    st.write("2. Checking version history...")
                    next_version = master_conn.get_next_version(uploaded_file.name)
                    st.write(f"   -> Assigning as Version {next_version}")
                    
                    st.write(f"3. Splitting '{uploaded_file.name}' into chunks...")
                    chunks = split_file(file_bytes, uploaded_file.name, next_version)
                    
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
                                'hash': chunk['hash'],
                                'size_bytes': chunk.get('size_bytes', len(chunk['raw_bytes']))
                            })
                        chunk['assigned_nodes'] = assigned_nodes
                    
                    st.write("5. Registering map with Master...")
                    master_conn.register_file_chunks(uploaded_file.name, next_version, metadata)
                    
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
                    st.toast(f"✅ {uploaded_file.name} (v{next_version}) successfully replicated!")

    # --- FILE LIST SECTION (SCROLLABLE) ---
    with col2:
        st.subheader("Files in DFS")
        
        if st.session_state.get('connected', False):
            try:
                master_url = f"http://{st.session_state.get('master_ip', '127.0.0.1')}:{st.session_state.get('master_port', '5000')}"
                master_conn = xmlrpc.client.ServerProxy(master_url)
                
                live_registry = master_conn.get_file_directory()
                
                if live_registry:
                    with st.container(height=500, border=True):
                        for filename, data in live_registry.items():
                            
                            text_col, ver_col, prev_col, dl_col, del_col = st.columns([3, 1.5, 1, 1, 1])
                            
                            with text_col:
                                st.markdown(f"**📄 {filename}**")
                                st.caption(f"{data['total_chunks']} chunks across {len(data['versions'])} versions")
                                
                            with ver_col:
                                if len(data['versions']) > 1:
                                    selected_version = st.selectbox(
                                        "Version", 
                                        options=data['versions'], 
                                        format_func=lambda x: f"v{x}", 
                                        key=f"ver_{filename}", 
                                        label_visibility="collapsed"
                                    )
                                else:
                                    selected_version = data['versions'][0]
                                    st.write(f"**v{selected_version}**")
                                
                            with prev_col:
                                is_image = filename.lower().endswith(('.png', '.jpg', '.jpeg', '.webp', '.gif'))
                                preview_clicked = st.button("Preview", key=f"prev_btn_{filename}", disabled=not is_image)
                            
                            # ---> NEW: Download Button Triggers the Modal
                            with dl_col:
                                if st.button("Download", key=f"dl_btn_{filename}"):
                                    download_matrix_dialog(filename, selected_version, st.session_state['master_ip'], st.session_state['master_port'])

                            with del_col:
                                if st.button("Delete", type="primary", key=f"del_btn_{filename}"):
                                    with st.spinner("Purging ALL versions from cluster..."):
                                        try:
                                            all_chunk_locations = master_conn.get_all_chunk_locations(filename)
                                            for loc in all_chunk_locations:
                                                try:
                                                    node_conn = xmlrpc.client.ServerProxy(f"http://{loc[1]}")
                                                    node_conn.delete_chunk(loc[0])
                                                except Exception:
                                                    pass 
                                                    
                                            success = master_conn.delete_file_metadata(filename)
                                            if success:
                                                st.toast("🗑️ All file versions permanently purged!")
                                                time.sleep(1) 
                                                st.rerun() 
                                            else:
                                                st.toast("❌ Failed to delete metadata.")
                                        except Exception as e:
                                            st.toast(f"❌ Deletion error: {e}")
                                            
                            if preview_clicked:
                                show_preview_dialog(filename, selected_version, st.session_state['master_ip'], st.session_state['master_port'])
                            
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
