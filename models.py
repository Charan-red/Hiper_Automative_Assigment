from pydantic import BaseModel
from typing import Optional

class User(BaseModel):
    username: str
    password: str

class FileChunk(BaseModel):
    file_id: str
    start_byte: int
    end_byte: int
    data: bytes
    checksum: int
    total_size: Optional[int] = None
    timestamp: float = 0.0  # Will be set when saved

class FileStatus(BaseModel):
    file_id: str
    status: str  # "pending", "partial", "complete"
    received_bytes: int
    total_bytes: Optional[int]
    last_updated: float
    chunks: list