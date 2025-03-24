import os
import hashlib

def calculate_checksum(data: bytes) -> int:
    return sum(data) % 256

def validate_chunk_header(header: bytes) -> tuple:
    if len(header) < 12:
        return False, None, None, None
    
    start_byte = int.from_bytes(header[:4], byteorder='big')
    end_byte = int.from_bytes(header[4:8], byteorder='big')
    checksum = int.from_bytes(header[8:12], byteorder='big')
    
    return True, start_byte, end_byte, checksum

def generate_file_id() -> str:
    return str(uuid.uuid4())