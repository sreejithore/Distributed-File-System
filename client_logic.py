import os
import hashlib

CHUNK_SIZE = 2 * 1024 * 1024  # 2MB chunks

def split_file(file_bytes, filename):
    """Splits a file into chunks and calculates a SHA-256 hash for each."""
    chunks = []
    
    for i in range(0, len(file_bytes), CHUNK_SIZE):
        chunk_data = file_bytes[i:i+CHUNK_SIZE]
        
        # ---> NEW: Calculate the SHA-256 hash of this specific chunk
        chunk_hash = hashlib.sha256(chunk_data).hexdigest()
        
        chunks.append({
            'chunk_name': f"{filename}_part_{i}",
            'raw_bytes': chunk_data,
            'hash': chunk_hash  # Save the hash in the dictionary
        })
        
    return chunks

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
