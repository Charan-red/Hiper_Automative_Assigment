import os
import json
from typing import Dict, Optional, Tuple
import hashlib
from datetime import datetime, timedelta
import asyncio
from pathlib import Path

# Configuration
UPLOAD_DIR = "uploads"
CHUNK_DIR = "chunks"
STALE_THRESHOLD = timedelta(hours=1)  # Time after which chunks are considered stale

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(CHUNK_DIR, exist_ok=True)

# In-memory storage for tracking uploads (in a production system, use a database)
upload_tracker: Dict[str, Dict] = {}

async def save_chunk(chunk: 'FileChunk'):
    chunk.timestamp = datetime.now().timestamp()
    
    # Save chunk to disk
    chunk_filename = f"{chunk.file_id}_{chunk.start_byte}_{chunk.end_byte}.chunk"
    chunk_path = os.path.join(CHUNK_DIR, chunk_filename)
    
    with open(chunk_path, "wb") as f:
        f.write(chunk.data)
    
    # Update upload tracker
    if chunk.file_id not in upload_tracker:
        upload_tracker[chunk.file_id] = {
            "chunks": [],
            "total_size": chunk.total_size,
            "last_updated": chunk.timestamp
        }
    
    upload_tracker[chunk.file_id]["chunks"].append({
        "start": chunk.start_byte,
        "end": chunk.end_byte,
        "path": chunk_path,
        "checksum": chunk.checksum,
        "timestamp": chunk.timestamp
    })
    upload_tracker[chunk.file_id]["last_updated"] = chunk.timestamp
    
    # Check if file is complete
    if chunk.total_size is not None:
        total_received = sum(c["end"] - c["start"] + 1 for c in upload_tracker[chunk.file_id]["chunks"])
        if total_received >= chunk.total_size:
            await assemble_file(chunk.file_id)

async def assemble_file(file_id: str):
    if file_id not in upload_tracker:
        raise ValueError(f"File {file_id} not found in tracker")
    
    file_info = upload_tracker[file_id]
    chunks = sorted(file_info["chunks"], key=lambda x: x["start"])
    
    # Verify we have all chunks
    expected_size = file_info["total_size"]
    total_received = sum(c["end"] - c["start"] + 1 for c in chunks)
    
    if expected_size is None or total_received < expected_size:
        return False  # Not complete
    
    # Create the complete file
    output_path = os.path.join(UPLOAD_DIR, file_id)
    
    with open(output_path, "wb") as outfile:
        for chunk in chunks:
            with open(chunk["path"], "rb") as infile:
                outfile.write(infile.read())
    
    # Clean up chunks
    for chunk in chunks:
        try:
            os.remove(chunk["path"])
        except OSError:
            pass
    
    # Update tracker
    upload_tracker[file_id]["status"] = "complete"
    
    return True

async def get_file_status(file_id: str) -> dict:
    if file_id not in upload_tracker:
        return {
            "file_id": file_id,
            "status": "not_found",
            "received_bytes": 0,
            "total_bytes": None,
            "last_updated": None,
            "chunks": []
        }
    
    file_info = upload_tracker[file_id]
    chunks = file_info.get("chunks", [])
    
    if file_info.get("status") == "complete":
        status = "complete"
    elif chunks:
        status = "partial"
    else:
        status = "pending"
    
    received_bytes = sum(c["end"] - c["start"] + 1 for c in chunks)
    
    return {
        "file_id": file_id,
        "status": status,
        "received_bytes": received_bytes,
        "total_bytes": file_info.get("total_size"),
        "last_updated": file_info.get("last_updated"),
        "next_byte": max(c["end"] for c in chunks) + 1 if chunks else 0,
        "chunks": [{"start": c["start"], "end": c["end"]} for c in chunks]
    }

async def get_file(file_id: str, return_metadata: bool = False):
    file_path = os.path.join(UPLOAD_DIR, file_id)
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File {file_id} not found")
    
    if return_metadata:
        file_size = os.path.getsize(file_path)
        return file_path, file_size
    return file_path

async def cleanup_stale_chunks():
    now = datetime.now().timestamp()
    threshold = (datetime.now() - STALE_THRESHOLD).timestamp()
    
    for file_id, file_info in list(upload_tracker.items()):
        if file_info.get("status") == "complete":
            continue
        
        stale = True
        for chunk in file_info["chunks"]:
            if chunk["timestamp"] > threshold:
                stale = False
                break
        
        if stale:
            # Remove stale chunks from disk
            for chunk in file_info["chunks"]:
                try:
                    os.remove(chunk["path"])
                except OSError:
                    pass
            
            # Remove from tracker
            del upload_tracker[file_id]

async def persist_incomplete_files():
    now = datetime.now().timestamp()
    threshold = (datetime.now() - STALE_THRESHOLD).timestamp()
    
    for file_id, file_info in list(upload_tracker.items()):
        if file_info.get("status") == "complete":
            continue
        
        # Check if any chunks are recent
        recent_activity = any(
            chunk["timestamp"] > threshold 
            for chunk in file_info["chunks"]
        )
        
        if not recent_activity and file_info["chunks"]:
            # Persist incomplete file
            output_path = os.path.join(UPLOAD_DIR, f"{file_id}.incomplete")
            
            with open(output_path, "wb") as outfile:
                for chunk in sorted(file_info["chunks"], key=lambda x: x["start"]):
                    with open(chunk["path"], "rb") as infile:
                        outfile.write(infile.read())
            
            # Clean up chunks
            for chunk in file_info["chunks"]:
                try:
                    os.remove(chunk["path"])
                except OSError:
                    pass
            
            # Remove from tracker
            del upload_tracker[file_id]