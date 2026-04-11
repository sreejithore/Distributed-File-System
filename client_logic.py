import os

CHUNK_SIZE = 2 * 1024 * 1024  # 2MB chunks

def split_file(file_bytes, filename):
    """
    Splits a file into chunks in memory to be sent over the network.
    """
    chunk_data_list = []
    total_size = len(file_bytes)
    
    num_chunks = (total_size // CHUNK_SIZE) + (1 if total_size % CHUNK_SIZE > 0 else 0)
    
    for i in range(num_chunks):
        start = i * CHUNK_SIZE
        end = min((i + 1) * CHUNK_SIZE, total_size)
        
        chunk_bytes = file_bytes[start:end]
        chunk_name = f"{filename}_part{i+1}"
        
        # Instead of saving to disk, we add the raw bytes to our list
        chunk_data_list.append({
            "chunk_name": chunk_name,
            "raw_bytes": chunk_bytes
        })
    
    return chunk_data_list

def stitch_file(filename, chunk_names, output_dir="."):
    """
    Simulates downloading chunks and merging them back together.
    """
    output_path = os.path.join(output_dir, f"downloaded_{filename}")
    
    with open(output_path, 'wb') as outfile:
        for chunk_name in chunk_names:
            chunk_path = f"temp_chunks/{chunk_name}"
            if os.path.exists(chunk_path):
                with open(chunk_path, 'rb') as infile:
                    outfile.write(infile.read())
            else:
                return False, f"Missing chunk: {chunk_name}"
                
    return True, output_path
