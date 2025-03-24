from fastapi import FastAPI, Depends, HTTPException, status, UploadFile, Header, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.background import BackgroundTasks
from typing import Optional, Dict
import os
import uuid
import time
from datetime import datetime, timedelta

from .auth import verify_token, create_access_token
from .models import FileChunk, FileStatus, User
from .storage import (
    save_chunk,
    assemble_file,
    get_file_status,
    get_file,
    cleanup_stale_chunks,
    persist_incomplete_files
)
from .background import setup_background_tasks

app = FastAPI(title="File Transfer API", version="1.0.0")

security = HTTPBearer()

# Setup background tasks
app.add_event_handler("startup", setup_background_tasks)
app.add_event_handler("shutdown", lambda: print("Shutting down..."))

# Mock user database (in a real app, use a proper database)
USERS_DB = {
    "device1": User(username="device1", password="securepassword1"),
    "device2": User(username="device2", password="securepassword2"),
}

@app.post("/token")
async def login_for_access_token(form_data: User):
    user = USERS_DB.get(form_data.username)
    if not user or user.password != form_data.password:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
        )
    access_token = create_access_token(data={"sub": user.username})
    return {"access_token": access_token, "token_type": "bearer"}

@app.post("/upload/{file_id}")
async def upload_file_chunk(
    file_id: str,
    request: Request,
    content_range: str = Header(None),
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    verify_token(credentials.credentials)
    
    # Parse content range header
    if not content_range:
        raise HTTPException(status_code=400, detail="Content-Range header required")
    
    try:
        unit, range_spec = content_range.split()
        if unit != "bytes":
            raise ValueError
        byte_range, total_size = range_spec.split("/")
        start_byte, end_byte = map(int, byte_range.split("-"))
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid Content-Range header")
    
    # Read chunk data with custom header
    raw_data = await request.body()
    if len(raw_data) < 12:  # Minimum header size
        raise HTTPException(status_code=400, detail="Invalid chunk format")
    
    # Extract header (first 12 bytes)
    header = raw_data[:12]
    chunk_data = raw_data[12:]
    
    # Verify checksum
    checksum = sum(chunk_data) % 256
    if checksum != header[-1]:
        raise HTTPException(status_code=400, detail="Checksum verification failed")
    
    # Save chunk
    chunk = FileChunk(
        file_id=file_id,
        start_byte=start_byte,
        end_byte=end_byte,
        data=chunk_data,
        checksum=checksum,
        total_size=int(total_size) if total_size != "*" else None
    )
    
    await save_chunk(chunk)
    
    return {"message": "Chunk received successfully", "next_byte": end_byte + 1}

@app.get("/status/{file_id}")
async def get_file_transfer_status(
    file_id: str,
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    verify_token(credentials.credentials)
    status_info = await get_file_status(file_id)
    return status_info

@app.get("/download/{file_id}")
async def download_file(
    file_id: str,
    request: Request,
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    verify_token(credentials.credentials)
    
    range_header = request.headers.get("Range")
    if not range_header:
        return await get_file(file_id)
    
    try:
        unit, byte_range = range_header.split("=")
        if unit != "bytes":
            raise ValueError
        start_byte, end_byte = map(int, byte_range.split("-"))
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid Range header")
    
    file_path, file_size = await get_file(file_id, return_metadata=True)
    
    if start_byte >= file_size or end_byte >= file_size:
        raise HTTPException(status_code=416, detail="Requested range not satisfiable")
    
    chunk_size = end_byte - start_byte + 1
    
    def file_iterator():
        with open(file_path, "rb") as f:
            f.seek(start_byte)
            remaining = chunk_size
            while remaining > 0:
                bytes_to_read = min(4096, remaining)
                data = f.read(bytes_to_read)
                if not data:
                    break
                yield data
                remaining -= len(data)
    
    headers = {
        "Content-Range": f"bytes {start_byte}-{end_byte}/{file_size}",
        "Accept-Ranges": "bytes",
        "Content-Length": str(chunk_size),
        "Content-Type": "application/octet-stream",
    }
    
    return StreamingResponse(
        file_iterator(),
        headers=headers,
        status_code=206,
    )

@app.post("/cleanup")
async def trigger_cleanup(
    background_tasks: BackgroundTasks,
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    verify_token(credentials.credentials)
    background_tasks.add_task(persist_incomplete_files)
    background_tasks.add_task(cleanup_stale_chunks)
    return {"message": "Cleanup process started"}