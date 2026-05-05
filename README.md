# 🚀 Distributed File System (DFS)

A fully functional, fault-tolerant Distributed File System built from scratch in Python. This project implements a Master-Worker distributed storage system inspired by HDFS and GFS, using local processes to simulate a real cluster.

---

## 🧠 System Architecture

The system operates on a **Master-Worker** architecture divided into three core services:

1. **Client Interface (Gateway)**
   - Built with Streamlit, this GUI orchestrates uploads, downloads, previews, and cluster actions.
   - It splits files into chunks, requests version information from the Master, and sends chunks to Data Nodes.

2. **Master Node (Metadata Server)**
   - Tracks file metadata, chunk locations, file version history, and live node status.
   - Stores metadata in `master_metadata.db` using SQLite.
   - Runs a background replication monitor to keep the cluster fault tolerant.

3. **Data Nodes (Storage Workers)**
   - Persist binary chunks to local disk under `storage_node_<port>/` folders.
   - Receive chunks over XML-RPC and respond to replication, download, and delete requests.
   - Send heartbeat pings to the Master to report health.

---

## ✨ Core Features

### UI Features

- **Streamlit dashboard** for user-friendly cluster management.
- **Sidebar master connection** panel with IP and port entry.
- **Live node health view** showing which storage nodes are active.
- **File upload workflow** that handles chunking and version-aware replication.
- **File registry** with version selection, preview, download, and delete actions.
- **Image preview modal** for supported image formats.
- **Download matrix UI** that visualizes chunk fetching and hash verification.
- **Storage analytics page** with cluster metrics, bar charts, and upload timeline.

### Backend Features

- **SQLite metadata service** for persistent file/chunk records.
- **Heartbeat-based node tracking** to detect active Data Nodes.
- **Versioned file uploads** with metadata stored per version.
- **Chunk-level replication** across multiple storage nodes (replication factor = 2).
- **Background replication monitor** that heals under-replicated data.
- **Data node garbage collection** to remove orphaned chunks.
- **Hash verification on download** to ensure file integrity.
- **Distributed deletion** that removes chunk metadata and physical chunks.

---

## 🛠️ Tech Stack

- **Language:** Python 3.x
- **Frontend:** Streamlit
- **Networking:** `xmlrpc` (Python Standard Library)
- **Database:** `sqlite3` (Python Standard Library)
- **Data processing:** standard Python file and hashing utilities

---

## 🚀 How to Run the Cluster

You can simulate the distributed system locally by running the Master, two Data Nodes, and the Streamlit client in separate terminals.

### 1. Boot Up the Cluster

Open four separate terminal windows and run the following commands:

* **Terminal 1 — Master Node:**
  ```bash
  python master_node.py
  ```
  Wait for the `Master Node is running on port 5000...` message.

* **Terminal 2 — Data Node A:**
  ```bash
  python data_node.py 5001
  ```
  This creates `storage_node_5001/`.

* **Terminal 3 — Data Node B:**
  ```bash
  python data_node.py 5002
  ```
  This creates `storage_node_5002/`.

* **Terminal 4 — Streamlit Client:**
  ```bash
  streamlit run app.py
  ```

---

### 2. Interact via the GUI

1. Open the URL shown by Streamlit (usually `http://localhost:8501`).
2. Enter `127.0.0.1` for Master Node IP and `5000` for Master Port.
3. Click **Connect**.
4. Upload a file on the **Main Dashboard**.
5. View versions, preview images, download files, or delete stored data.
6. Use the **Storage Analytics** page for cluster storage metrics and history.

---

## 🌪️ Chaos Test (Fault Tolerance Demo)

This project is built to survive partial failures. Try this workflow to validate the self-healing design:

1. Start Master, Node A, Node B, and the Streamlit client.
2. Upload an image or PDF through the dashboard.
3. Confirm chunks were written into both `storage_node_5001/` and `storage_node_5002/`.
4. Kill Node A with `Ctrl + C` in Terminal 2.
5. In the Streamlit UI, click **Download** next to the file.

Result: the system will detect Node A is offline, fetch from the remaining node, and reconstruct the file into `downloads/`.

---

## 🔧 Directory Structure

- `app.py` — Streamlit UI and client orchestration
- `master_node.py` — master metadata server and replication monitor
- `data_node.py` — storage node process and heartbeat worker
- `client_logic.py` — chunk splitting and reassembly helpers
- `downloads/` — output folder for recovered downloads
- `storage_node_5001/` — storage files for node 5001
- `storage_node_5002/` — storage files for node 5002
- `temp_chunks/` — temporary chunk fragments used during reassembly/testing
- `master_metadata.db` — SQLite metadata database created by the master

---

## 📝 Notes

- This is a prototype and is not production-ready.
- Replication is currently fixed at **2 copies**.
- Master node only stores metadata, not file contents.
- Data nodes store raw chunk files locally.

---

## ⚠️ Troubleshooting

- If the dashboard cannot connect, verify the master is running on `127.0.0.1:5000`.
- If a storage node is offline, uploads will still attempt to replicate to the active node(s).
- If a file is missing during download, inspect the node logs for heartbeat or replication errors.
- Check your browser console and Streamlit logs for detailed error output.
