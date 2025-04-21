import asyncio
from bleak import BleakClient
import struct

name1 = "IMU1"
name2 = "IMU2"
characteristic_uuid = "61A885A4-41C3-60D0-9A53-6D652A70D29C"
address1 = '00:BE:44:C0:6B:3D'
address2 = '58:3B:C2:56:8E:07'

async def run():
    # Tạo queue cho hai thiết bị
    queue1 = asyncio.Queue()
    queue2 = asyncio.Queue()
    
    # Rest of your functions will go here
    
    print("Script loaded correctly")

if __name__ == "__main__":
    asyncio.run(run())