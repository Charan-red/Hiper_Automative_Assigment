import asyncio
from fastapi import BackgroundTasks
from .storage import cleanup_stale_chunks, persist_incomplete_files

async def periodic_cleanup():
    while True:
        await asyncio.sleep(3600)  # Run every hour
        await cleanup_stale_chunks()
        await persist_incomplete_files()

def setup_background_tasks():
    loop = asyncio.get_event_loop()
    loop.create_task(periodic_cleanup())