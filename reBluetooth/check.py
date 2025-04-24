name1 = "IMU1"
name2 = "IMU2"
# These are the correct addresses for the IMU devices
characteristic_uuid = "61A885A4-41C3-60D0-9A53-6D652A70D29C"  # <-- set your notify UUID here

# Service UUID
IMU_SERVICE_UUID = "2412b5cbd460800c15c39ba9ac5a8ade"

# LED Control Characteristic UUID
LED_CONTROL_CHAR_UUID = "5b02651040-88c29746d8be6c736a087a"

# IMU Notification Characteristic UUID
IMU_NOTIFY_CHAR_UUID = "61a885a441c360d09a536d652a70d29c"

address1 = '00:BE:44:C0:6B:3D'  # IMU1 address
address2 = '58:3B:C2:56:8E:07'  # IMU2 address

import asyncio
from bleak import BleakClient, BleakScanner
import struct  # Thêm thư viện struct để xử lý dữ liệu nhị phân
import numpy as np  # Thêm numpy để xử lý số thập phân
from collections import deque

async def run():
    print(f"Will connect directly to the provided device addresses:")
    print(f"- {name1}: {address1}")
    print(f"- {name2}: {address2}")
    
    # Tạo queue cho hai thiết bị
    queue1 = asyncio.Queue()
    queue2 = asyncio.Queue()
    
    # Biến flag để theo dõi trạng thái kết nối
    connected = {name1: False, name2: False}

    # Hàm xử lý notification cho từng thiết bị
    async def handle_notification(device_name, queue, sender, data):
        if len(data) >= 28:
            try:
                values = struct.unpack('<7f', data[:28])
                # Chỉ hiển thị các giá trị số, không hiển thị nhãn q0=, q1=, ...
                output = f"{device_name}: {values[0]:.6f} {values[1]:.6f} {values[2]:.6f} {values[3]:.6f} {values[4]:.6f} {values[5]:.6f} {values[6]:.6f}"
                await queue.put(output)
            except struct.error:
                pass

    # Hàm kết nối với thiết bị và lắng nghe notification
    async def connect_device(device_name, device_address, queue):
        retry_count = 0
        max_retries = 5
        
        while retry_count < max_retries:
            try:
                print(f"Connecting to {device_name} at {device_address}...")
                async with BleakClient(device_address) as client:
                    if not client.is_connected:
                        print(f"Failed to connect to {device_name}. Retrying...")
                        retry_count += 1
                        await asyncio.sleep(1)
                        continue
                    
                    connected[device_name] = True
                    print(f"Connected to {device_name} [{device_address}]")
                    
                    # Check if the characteristic exists - using services property instead of get_services()
                    services = client.services
                    characteristics = []
                    for service in services:
                        for char in service.characteristics:
                            characteristics.append(char.uuid)
                    
                    if characteristic_uuid not in characteristics:
                        print(f"Warning: Characteristic {characteristic_uuid} not found on {device_name}")
                        print(f"Available characteristics: {', '.join(characteristics[:5])}...")
                        
                        # If the specific characteristic is not found, try to find a notifiable one
                        for service in services:
                            for char in service.characteristics:
                                if "notify" in char.properties:
                                    print(f"Using alternative characteristic: {char.uuid}")
                                    characteristic_to_use = char.uuid
                                    break
                            if "characteristic_to_use" in locals():
                                break
                    else:
                        characteristic_to_use = characteristic_uuid
                    
                    # Tạo hàm notification riêng cho thiết bị này
                    async def device_notification(s, d):
                        await handle_notification(device_name, queue, s, d)
                    
                    await client.start_notify(characteristic_to_use, device_notification)
                    
                    # Reset retry count after successful connection
                    retry_count = 0
                    
                    # Giữ kết nối cho đến khi bị ngắt
                    while True:
                        await asyncio.sleep(0.5)
                        
            except Exception as e:
                print(f"Error with {device_name}: {str(e)}")
                retry_count += 1
                if retry_count < max_retries:
                    print(f"Retrying connection to {device_name} ({retry_count}/{max_retries})...")
                    await asyncio.sleep(2)  # Wait before retry
                else:
                    print(f"Max retries reached for {device_name}. Giving up.")
                    break
            finally:
                connected[device_name] = False
                print(f"Disconnected from {device_name}")
        
        print(f"Device connection task for {device_name} ended.")

    # Hàm đọc và hiển thị dữ liệu từ hai queue theo kiểu luân phiên
    async def process_data():
        devices_ready = False
        print("Waiting for both devices to connect before processing data...")
        
        while True:
            # Check if both devices are connected
            if connected[name1] and connected[name2]:
                if not devices_ready:
                    print("Both devices connected! Starting data processing...")
                    devices_ready = True
                
                try:
                    # Alternate reading from both queues only when both devices are connected
                    data1 = await asyncio.wait_for(queue1.get(), timeout=0.1)
                    print(data1)
                    queue1.task_done()
                    
                    data2 = await asyncio.wait_for(queue2.get(), timeout=0.1)
                    print(data2)
                    queue2.task_done()
                    
                    # Chờ một chút để tránh vòng lặp quá nhanh
                    await asyncio.sleep(0.01)
                except asyncio.TimeoutError:
                    # If timeout occurs, continue the loop
                    await asyncio.sleep(0.01)
                except asyncio.CancelledError:
                    break
            else:
                # Reset the ready flag if either device disconnects
                if devices_ready:
                    print("Device disconnected. Waiting for all devices to reconnect...")
                    devices_ready = False
                
                # Wait longer between checks when devices are not connected
                await asyncio.sleep(0.5)

    # Chạy tất cả tác vụ cùng lúc
    print("\nStarting device connections...")
    try:
        await asyncio.gather(
            connect_device(name1, address1, queue1),
            connect_device(name2, address2, queue2),
            process_data()
        )
    except asyncio.CancelledError:
        print("Operations cancelled")
    except KeyboardInterrupt:
        print("Program interrupted by user")

if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("Program terminated by user")