# 🚀 Distributed File System (DFS)

A fully functional, fault-tolerant Distributed File System built from scratch in Python. This project is a Master-Worker distributed storage system inspired by industry-standard architectures like Hadoop (HDFS) and the Google File System (GFS). It distributes data across multiple storage nodes, balances upload loads, and ensures high availability through data replication and self-healing downloads.

---

## 🧠 System Architecture

The system operates on a **Master-Worker** architecture divided into three core microservices:

1. **The Client Interface (Gateway):** A sleek, modern GUI built with Streamlit. It acts as the orchestrator—taking large local files, splitting them into manageable chunks, and sending those chunks directly to the Data Nodes based on instructions from the Master.
2. **The Master Node (The Brain / Metadata Server):** A lightweight metadata server that tracks file locations via an SQLite database and constantly monitors the network for active worker nodes using background heartbeat threads. It never touches actual file data to prevent bottlenecks.
3. **The Data Nodes (The Storage / Workers):** The physical storage engines running on dynamic network ports. They receive binary data from the Client, save the chunks to their local hard drives, and continuously ping the Master to report their health.

---

## ✨ Core Features

* **Data Partitioning (Chunking):** The system automatically splits large files into smaller, manageable chunks before transmitting them across the network.
* **Intelligent Load Balancing:** The Master Node actively tracks live workers, and the Client deals chunks evenly across the cluster to prevent any single server from bottlenecking.
* **Fault Tolerance & Replication (Self-Healing):** Implements a Replication Factor of 2. If a user is downloading a file and a Data Node suffers a catastrophic hardware failure, the system instantly catches the error, pivots to the backup node, and stitches the file together without losing a single byte of data.
* **Background Rebalancer:** A continuous background thread that monitors the database. If a node goes offline and a new one boots up, the Rebalancer automatically copies data over to the new node to restore the system's fault tolerance without any human intervention!
* **Cluster Blackout Protection:** Intelligent pre-checks ensure that if the entire cluster goes offline, the UI gracefully blocks uploads/downloads and alerts the user with a clean, professional message rather than crashing.

---

## 🛠️ Tech Stack

* **Language:** Python 3.x
* **Networking/RPC:** `xmlrpc` (Python Standard Library) for fast, reliable remote procedure calls.
* **Database:** `sqlite3` (Python Standard Library) for persistent metadata storage.
* **Frontend/GUI:** `streamlit` for a responsive, modern web dashboard.

---

## 🚀 How to Run the Cluster

To test the system locally, you will simulate the distributed network by running multiple terminal windows on different ports.

### 1. Boot Up the Cluster
Open **four separate terminal windows** and run the following commands in order:

* **Terminal 1 (The Master Node):**
  ```bash
  python master_node.py

(Wait for the "Master Node is running on port 5000" message)

* **Terminal 2 (Data Node A):**
```bash
  python data_node.py 5001
```
(This will automatically create a storage_node_5001 folder)

* **Terminal 3 (Data Node B):**
```bash
python data_node.py 5002
```
(This will automatically create a storage_node_5002 folder)

* **Terminal 4 (The Streamlit Client):**
```bash
streamlit run app.py
```

---

### 2. Interact via the GUI
Open the URL provided by Streamlit (usually http://localhost:8501).

Enter the Master Node IP (127.0.0.1) and Port (5000) in the sidebar and click Connect.

Upload a file. You will see the chunks distributed across your local storage_node_ folders!

🌪️ The "Chaos Test" (Proving Fault Tolerance)
Want to see the self-healing architecture in action? Try this:

With the Master and both Data Nodes running, upload an image or PDF via the GUI.

Verify that the file chunks have been duplicated into both storage_node_5001 and storage_node_5002.

Go to Terminal 2 (Data Node 5001) and violently crash it by pressing Ctrl + C. Half of your cluster is now dead.

Go to the Streamlit UI and click Download next to your file.

Result: The system will attempt to contact Node 5001, realize it is offline, seamlessly pivot to the backups on Node 5002, and reconstruct your file perfectly into your downloads/ folder.
