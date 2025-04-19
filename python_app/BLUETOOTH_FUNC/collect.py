# this program is about to check the connection of bluetooth devices to the pc 

import bleak
import asyncio
from bleak import BleakClient
from bleak import BleakScanner
# store active connections in a dictionary
active_connections = {}  

async def SINGLE_AUTO_CONNECT(address: str):
    # this function is used to connect to the single IMU automatically
    # in case: True : the device is connected
    # in case: False : the device is not connected 
    # 2 functions go with: 
        # 1. disconnect_device: disconnect the device
        # 2. disconnect_all_devices: disconnect all devices
    global active_connections
    
    try:

        client = BleakClient(address)
        await client.connect()
        
        if client.is_connected:
            # print(f"SINGLE_AUTO_CONNECT: Successfully connected to device: {address}")
            active_connections[address] = client
            return True
        else:
            # print(f"SINGLE_AUTO_CONNECT: Unable to connect to device: {address}")
            return False
            
    except Exception as e:
        # print(f"SINGLE_AUTO_CONNECT: Error occurred during connection: {e}")
        return False


async def disconnect_device(address: str):
    
    global active_connections
    
    if address in active_connections:
        client = active_connections[address]
        if client.is_connected:
            await client.disconnect()
            # print(f"DISCONNECT: Successfully disconnected from device: {address}")
        
        del active_connections[address]
        return True
    
    return False


async def disconnect_all_devices():
    
    global active_connections
    
    for address, client in list(active_connections.items()):
        try:
            if client.is_connected:
                await client.disconnect()
                # print(f"DISCONNECT_ALL: Successfully disconnected from device: {address}")
        except Exception as e:
            print(f"DISCONNECT_ALL: Error disconnecting from {address}: {e}")
    
    active_connections.clear()

#-----------------------------------------------------------------------------------------------------------------------------------

class BLUETOOTH_DETECTOR:
    # this class target is to check the connection of bluetooth devices to system 
    # 3 functions: 
        # 1. OUTSYSTEM_CHECK: check the bluetooth devices are available to the system
        # 2. INSYSTEM_CHECK: check the bluetooth devices are connected to the system
        # 3. AUTO_CONNECT: automatically connect to the unconnected IMUs but available
    
    def __init__(self):
        self.devices = []
        self.loop = asyncio.get_event_loop()

    async def OUTSYSTEM_CHECK(self, max_retries=3, retry_delay=2):
        # this function is used to check the connection of bluetooth devices are available to the pc
        # in case : None : Bluetooth or IMU is off
        # in case : List : all devices are available


        for attempt in range(max_retries):
            try:
                # print(f"Scanning for Bluetooth devices (attempt {attempt+1}/{max_retries})...")
                devices = await BleakScanner.discover()
                
                if not devices:
                    print("No Bluetooth devices found.")
                    return None
                    
                device_names = []
                # print(f"Found {len(devices)} Bluetooth devices:")
                for device in devices:
                    name = device.name or 'Unknown'
                    # print(f"Device: {name}")

                    # Store the device name in our return list
                    device_names.append(name)

                    # Still store the full device object for internal use
                    if device not in self.devices:
                        self.devices.append(device)

                return device_names
            
            except bleak.exc.BleakDBusError as e:

                # Error handling for specific Bluetooth errors

                if "Operation already in progress" in str(e):
                    remaining = max_retries - attempt - 1
                    if remaining > 0:
                        print(f"Bluetooth scan already in progress. Waiting {retry_delay}s before retry...")
                        await asyncio.sleep(retry_delay)
                    else:
                        print("Failed after multiple attempts. Try these commands:")
                        print("sudo systemctl restart bluetooth")
                        print("sudo hciconfig hci0 reset")
                else:
                    print(f"Bluetooth error: {e}")
                    return None

    async def INSYSTEM_CHECK(self):
        # This function is used to check all the IMUs are connected to the system
        # in case : None : Bluetooth or IMU is off
        # in case : List : all devices are connected

        try:
            # Run the bluetoothctl command asynchronously
            proc = await asyncio.create_subprocess_exec(
                'bluetoothctl', 'devices',
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            devices_output, stderr = await proc.communicate()
            devices_output = devices_output.decode('utf-8')
            
            all_devices = []
            
            # Extract device information
            for line in devices_output.splitlines():
                if "Device" in line:
                    parts = line.split("Device ", 1)[1].split(" ", 1)
                    if len(parts) == 2:
                        mac_address = parts[0].strip()
                        name = parts[1].strip()
                        
                        # Check connection status for each device
                        info_proc = await asyncio.create_subprocess_exec(
                            'bluetoothctl', 'info', mac_address,
                            stdout=asyncio.subprocess.PIPE,
                            stderr=asyncio.subprocess.PIPE
                        )
                        info_output, stderr = await info_proc.communicate()
                        info_output = info_output.decode('utf-8')
                        
                        # Check if device is connected
                        connected = "Connected: yes" in info_output
                        
                        # Only process connected devices
                        if connected:
                            # Only store the device name
                            all_devices.append(name)
            
            # Display results
            # print("Connected Bluetooth Devices:")
            # print("-" * 50)
            
            if not all_devices:
                print("No Bluetooth devices are currently connected.")
                return None
            # else:
            #     for device_name in all_devices:
            #         print(f"Device: {device_name}")
            #         print("-" * 50)
                
            #     print(f"Total connected devices: {len(all_devices)}")
            
            return all_devices
            
        except Exception as e:
            print(f"An unexpected error occurred during INSYSTEM_CHECK: {e}")
            return []
    
    async def AUTO_CONNECT(self, availabel_missing_devices):
        # this function is used to automatically connect to the unconnected IMUs but available
        # in case: True : all devices are connected
        # in case: list : some devices are can not be automatically connected - please recheck
       
        for device in availabel_missing_devices: 
            for d in self.devices: 
                if d.name == device: 
                    # print(f"AUTO CONNECT: Attempting to connect to {device} ({d.address})...")
                    
                    await SINGLE_AUTO_CONNECT(d.address)
                    # if check == True:
                    #     print(f"AUTO CONNECT: Successfully connected to {device} ({d.address})")           
                    # else:
                    #     print(f"AUTO CONNECT: Failed to connect to {device} ({d.address})")

        # check if all devices are connected
        
#------------------------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    bluetooth_detector = BLUETOOTH_DETECTOR()
    loop = asyncio.get_event_loop()
    devices = loop.run_until_complete(bluetooth_detector.INSYSTEM_CHECK())
    print(active_connections)
    print(devices)
