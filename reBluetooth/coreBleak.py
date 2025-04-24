import asyncio
from bleak import BleakScanner, BleakClient
import struct
import numpy as np
from collections import deque
import time

IMU_SERVICE_UUID = "2412b5cbd460800c15c39ba9ac5a8ade"

# LED Control Characteristic UUID
LED_CONTROL_CHAR_UUID = "5b02651040-88c29746d8be6c736a087a"

# IMU Notification Characteristic UUID
IMU_NOTIFY_CHAR_UUID = "61a885a441c360d09a536d652a70d29c"

#START
#-------------------------------------------------------------------------------------------------------------------------------------------------------------

# Space for global variables
characteristic_uuid = "61A885A4-41C3-60D0-9A53-6D652A70D29C"
available_devices_address = {}

# Known characteristic UUIDs for IMU devices - add your three specific UUIDs here
IMU_CHARACTERISTIC_UUIDS = [
    "61A885A4-41C3-60D0-9A53-6D652A70D29C",  # Primary UUID
    "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",  # Replace with your second UUID
    "YYYYYYYY-YYYY-YYYY-YYYY-YYYYYYYYYYYY",  # Replace with your third UUID
]

# Device-specific characteristics mapping (can be populated as devices are discovered)
DEVICE_CHARACTERISTICS = {
    # "IMU1": "61A885A4-41C3-60D0-9A53-6D652A70D29C",
    # "IMU2": "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
    # Add more as you learn which characteristic works best for each device
}

#-------------------------------------------------------------------------------------------------------------------------------------------------------------

async def DISCONNECT_DEVICE(address):
    # use bluetoothctl to disconnect the device - linux native only
    # this function is ensurely disconnected the device, not temporarily in Bleak 

    print(f"[DISCONNECT_DEVICE]: Attempting to disconnect {address} using bluetoothctl...")
    try:
        # Run the bluetoothctl command asynchronously
        proc = await asyncio.create_subprocess_exec(
            'bluetoothctl', 'disconnect', address,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await proc.communicate()

        
        if proc.returncode == 0 and b"Successful disconnected" in stdout:
             print(f"[DISCONNECT_DEVICE]: Successfully disconnected {address} via bluetoothctl.")
             return True
        else:
             # hanle for error circumstances
             error_message = stderr.decode('utf-8', errors='ignore').strip()
             output_message = stdout.decode('utf-8', errors='ignore').strip()
             print(f"[DISCONNECT_DEVICE]: Failed to disconnect {address} via bluetoothctl.")
             if output_message: print(f"[DISCONNECT_DEVICE]: Output: {output_message}")
             if error_message: print(f"[DISCONNECT_DEVICE]: Error: {error_message}")
             return False

    except FileNotFoundError:
        print("[DISCONNECT_DEVICE]: Error - 'bluetoothctl' command not found. Is it installed and in PATH?")
        return False
    except Exception as e:
        print(f"[DISCONNECT_DEVICE]: An unexpected error occurred while disconnecting {address}: {e}")
        return False
    
#-------------------------------------------------------------------------------------------------------------------------------------------------------------

class BLUETOOTH_DETECTOR:
    
    """
    This class is used to detect BLuetooth devices from out or in system (PC)
    Checking out these devices are available or not

        INSYSTEM_SCANNING: Check the devices that are connected to the system
            1. If aim_device is connected, save its info into "available_devices_address" 
            2. If aim_device is connected, disconnect it

        OUTSYSTEM_SCANNING: Check the devices that are available in the system
            1. If aim_device is potentially availabel to connect, save its info into "available_devices_address" 
    """

    def __init__(self, needy_devices):
        self.devices = {}
        self.loop = asyncio.get_event_loop()
        self.aim_devices = needy_devices

    #--------------------------------------------------------------------------------------------------------

    async def OUTSYSTEM_SCANNING(self):
        #Scan for Bluetooth devices and find matches with aim_devices.

        # print("OUTSYSTEM_SCANNING: Scanning for Bluetooth devices...")
        bleak_devices = await BleakScanner.discover()

        # print(f"OUTSYSTEM_SCANNING: Checking the self.device {self.devices}")

        for device in bleak_devices: 

            # if find devices in the aim_devices list, update the self.devices dictionary
            if device.name in self.aim_devices:
                self.devices[device.name] = device.address
                print(f"[OUTSYSTEM_SCANNING]: Found device {device.name} at {device.address}")
                # Add info into "available_devices_address" dictionary
                available_devices_address[device.name] = device.address
        
        return self.devices

    #--------------------------------------------------------------------------------------------------------

    async def INSYSTEM_SCANNING(self):
        
        global available_devices_address

        try:
            # Run the bluetoothctl command asynchronously
            proc = await asyncio.create_subprocess_exec(
                'bluetoothctl', 'devices',
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            devices_output, stderr = await proc.communicate()
            devices_output = devices_output.decode('utf-8')
            
            
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
                            # checking if the connected device name is in the aim_devices list
                            if name in self.aim_devices:

                                # self.devices[name] = mac_address
                                # Use await to properly call the async disconnect function
                                await DISCONNECT_DEVICE(mac_address)

            # by clear the available_devices_address dictionary, so we can return the connected devices match
            # in the aim device is "available_devices_address"
          
        except Exception as e:
            print(f"[INSYSTEM_SCANNING]: An unexpected error occurred during INSYSTEM_CHECK: {e}")
            return []

#-------------------------------------------------------------------------------------------------------------------------------------------------------------

class BLUETOOTH_DATA_READER():

    global characteristic_uuid, IMU_CHARACTERISTIC_UUIDS, DEVICE_CHARACTERISTICS

    def __init__(self, devices_dict=None):
        """
        Initialize the data reader with a dictionary of devices
        :param devices_dict: Dictionary where keys are device names and values are device addresses
        """
        self.devices_dict = devices_dict or available_devices_address
        self.loop = asyncio.get_event_loop()
        self.characteristic_uuid = characteristic_uuid  # Using global characteristic UUID 
        self.connection_attempts = {}  # Track connection attempts per device
        self.successful_characteristics = {}  # Track which characteristic worked for which device

    async def handle_notification(self, device_name, queue, sender, data):
        """Process incoming data from a device and put it in the device's queue"""
        if len(data) >= 28:
            try:
                values = struct.unpack('<7f', data[:28])
                ts = time.time()
                # Format output with timestamp + values
                output = f"[DATA] {device_name} {ts:.6f}: " + " ".join(f"{v:.6f}" for v in values)
                await queue.put(output)
            except struct.error:
                pass

    async def connect_device(self, device_name, device_address, queue, connected_devices):
        """Connect to a device and start receiving notifications"""
        retry_count = 0
        max_retries = 5
        connected = False
        
        while retry_count < max_retries and not connected:
            try:
                print(f"[connect_device]: Connecting to {device_name} at {device_address}...")
                async with BleakClient(device_address) as client:
                    if not client.is_connected:
                        print(f"[connect_device]: Failed to connect to {device_name}. Retrying...")
                        retry_count += 1
                        await asyncio.sleep(1)
                        continue
                    
                    connected = True
                    print(f"[connect_device]: Connected to {device_name} [{device_address}]")
                    
                    # Update the connected status to True for this device
                    connected_devices[device_name] = True
                    
                    # Check if the characteristic exists
                    services = client.services
                    characteristics = []
                    for service in services:
                        for char in service.characteristics:
                            characteristics.append(char.uuid)
                    
                    if self.characteristic_uuid not in characteristics:
                        print(f"[connect_device]: Warning: Characteristic {self.characteristic_uuid} not found on {device_name}")
                        print(f"[connect_device]: Available characteristics: {', '.join(characteristics[:5])}...")
                        
                        # If the specific characteristic is not found, try to find a notifiable one
                        characteristic_to_use = None
                        for service in services:
                            for char in service.characteristics:
                                if "notify" in char.properties:
                                    print(f"[connect_device]: Using alternative characteristic: {char.uuid}")
                                    characteristic_to_use = char.uuid
                                    break
                            if characteristic_to_use:
                                break
                        if not characteristic_to_use:
                            print(f"[connect_device]: No notifiable characteristic found on {device_name}. Disconnecting.")
                            return
                    else:
                        characteristic_to_use = self.characteristic_uuid
                    
                    # Create notification handler for this device
                    device_notification_handler = lambda s, d: self.loop.create_task(
                        self.handle_notification(device_name, queue, s, d)
                    )
                    
                    await client.start_notify(characteristic_to_use, device_notification_handler)
                    print(f"[connect_device]: Started notifications for {device_name}")
                    
                    # Reset retry count after successful connection
                    retry_count = 0
                    
                    # Keep connection alive until interrupted
                    while True:
                        await asyncio.sleep(0.5)
                        
            except Exception as e:
                print(f"[connect_device]: Error with {device_name}: {str(e)}")
                retry_count += 1
                if retry_count < max_retries:
                    print(f"[connect_device]: Retrying connection to {device_name} ({retry_count}/{max_retries})...")
                    await asyncio.sleep(2)  # Wait before retry
                else:
                    print(f"[connect_device]: Max retries reached for {device_name}. Giving up.")
                    break
            finally:
                if connected:
                    connected = False
                    # Update the connected status to False when disconnected
                    connected_devices[device_name] = False
                    print(f"[connect_device]: Disconnected from {device_name}")
        
        print(f"[connect_device]: Device connection task for {device_name} ended.")

    async def process_data(self, queues, device_names, connected_devices):
        """Process data from all device queues in a rotating manner"""
        device_count = len(device_names)
        
        print(f"[process_data]: Waiting for all {device_count} devices to connect...")
        
        while True:
            # Check if all devices are connected
            all_connected = all(connected_devices.values())
            
            if all_connected:
                try:
                    # All devices are connected, let's read data
                    if not hasattr(self, 'all_devices_connected_reported') or not self.all_devices_connected_reported:
                        print("[process_data]: All devices are now connected! Starting data processing...")
                        self.all_devices_connected_reported = True
                    
                    # Read data from all queues in order
                    for i, name in enumerate(device_names):
                        try:
                            data = await asyncio.wait_for(queues[i].get(), timeout=0.1)
                            print(data)  # Already formatted in handle_notification
                            queues[i].task_done()
                        except asyncio.TimeoutError:
                            # No data available for this device
                            pass
                    
                    # Wait a bit to prevent too rapid looping
                    await asyncio.sleep(0.01)
                except asyncio.CancelledError:
                    print("[process_data]: Task cancelled")
                    break
            else:
                # Reset the notification flag if any device disconnects
                if hasattr(self, 'all_devices_connected_reported') and self.all_devices_connected_reported:
                    self.all_devices_connected_reported = False
                    print("[process_data]: At least one device disconnected, waiting for all devices to reconnect...")
                
                # Print which devices are still connecting
                not_connected = [name for name, status in connected_devices.items() if not status]
                if not_connected:
                    print(f"[process_data]: Waiting for devices to connect: {not_connected}")
                
                # Wait longer between checks when not all devices are connected
                await asyncio.sleep(0.5)

    async def READ_DATA(self, devices_dict=None):
        """
        Main method to read data from multiple Bluetooth devices
        :param devices_dict: Optional dictionary to override the one provided at init
        """
        if devices_dict:
            self.devices_dict = devices_dict
        
        if not self.devices_dict:
            print("[READ_DATA]: No devices to connect to. Please provide a devices dictionary.")
            return
        
        print(f"[READ_DATA]: Starting data collection from {len(self.devices_dict)} devices:")
        for name, address in self.devices_dict.items():
            print(f"[READ_DATA]: - {name}: {address}")
        
        # Create a queue for each device
        device_queues = [asyncio.Queue() for _ in range(len(self.devices_dict))]
        device_names = list(self.devices_dict.keys())
        device_addresses = list(self.devices_dict.values())
        
        # Shared dictionary to track connection status - all start as False (not connected)
        connected_devices = {name: False for name in device_names}
        
        # Create connection tasks for all devices
        connection_tasks = [
            self.connect_device(name, address, queue, connected_devices)
            for name, address, queue in zip(device_names, device_addresses, device_queues)
        ]
        
        # Create data processing task with the shared connection status dictionary
        processing_task = self.process_data(device_queues, device_names, connected_devices)
        
        # Run all tasks concurrently
        try:
            print("[READ_DATA]: Starting all connection tasks and data processing...")
            await asyncio.gather(
                *connection_tasks,
                processing_task
            )
        except asyncio.CancelledError:
            print("[READ_DATA]: Operation cancelled")
        except KeyboardInterrupt:
            print("[READ_DATA]: User interrupted")
        except Exception as e:
            print(f"[READ_DATA]: Unexpected error: {e}")

    def start_reading(self, devices_dict=None):
        """Start reading data (non-async wrapper)"""
        try:
            print("[start_reading]: Beginning Bluetooth data collection")
            self.loop.run_until_complete(self.READ_DATA(devices_dict))
        except KeyboardInterrupt:
            print("[start_reading]: Data reading terminated by user")

    async def smart_connect_device(self, device_name, device_address, queue, connected_devices):
        """
        Connect to a device with improved retry logic and connection handling using multiple UUIDs
        """
        retry_count = 0
        max_retries = 8
        connected = False
        backoff_time = 1
        
        # Track connection attempts for this device
        if device_name not in self.connection_attempts:
            self.connection_attempts[device_name] = 0
        self.connection_attempts[device_name] += 1
        attempt_num = self.connection_attempts[device_name]
        
        print(f"[smart_connect_device]: Preparing to connect to {device_name} at {device_address}... (Session attempt: {attempt_num})")
        
        # Determine which characteristics to try first based on past successes
        characteristic_uuids = []
        
        # If we already know which characteristic works for this device, try it first
        if device_name in DEVICE_CHARACTERISTICS:
            characteristic_uuids.append(DEVICE_CHARACTERISTICS[device_name])
            
        # Then add the rest of the known characteristics
        for uuid in IMU_CHARACTERISTIC_UUIDS:
            if uuid not in characteristic_uuids:  # Avoid duplicates
                characteristic_uuids.append(uuid)
        
        print(f"[smart_connect_device]: Will try these characteristics for {device_name}: {characteristic_uuids}")
        
        while retry_count < max_retries and not connected:
            try:
                # Use exponential backoff for retry timing
                retry_delay = backoff_time * (1.5 ** retry_count)
                print(f"[smart_connect_device]: Connecting to {device_name} (attempt {retry_count+1}/{max_retries})...")
                
                # Connect with a timeout
                client = BleakClient(device_address, timeout=20.0)
                await client.connect()
                try:
                    await client.exchange_mtu(247)
                    print(f"[smart_connect_device]: MTU to 247 for {device_name}")
                except Exception:
                    pass
                
                if not client.is_connected:
                    print(f"[smart_connect_device]: Failed to connect to {device_name}. Retrying...")
                    retry_count += 1
                    await asyncio.sleep(retry_delay)
                    continue
                
                connected = True
                print(f"[smart_connect_device]: Connected to {device_name} [{device_address}]")
                
                # Update the connected status to True for this device
                connected_devices[device_name] = True
                
                # Find all available characteristics
                services = client.services
                available_characteristics = []
                for service in services:
                    for char in service.characteristics:
                        if "notify" in char.properties:
                            available_characteristics.append(char.uuid)
                
                # Try to use the preferred characteristics in order
                used_characteristic = None
                for char_uuid in characteristic_uuids:
                    # Normalize UUID format (uppercase/lowercase)
                    normalized_uuid = char_uuid.upper()
                    if any(normalized_uuid.upper() == avail.upper() for avail in available_characteristics):
                        try:
                            print(f"[smart_connect_device]: Trying characteristic {char_uuid} for {device_name}")
                            
                            # Create notification handler for this device
                            device_notification_handler = lambda s, d: self.loop.create_task(
                                self.handle_notification(device_name, queue, s, d)
                            )
                            
                            await client.start_notify(char_uuid, device_notification_handler)
                            print(f"[smart_connect_device]: Successfully started notifications for {device_name} using {char_uuid}")
                            used_characteristic = char_uuid
                            
                            # Remember which characteristic worked for this device
                            DEVICE_CHARACTERISTICS[device_name] = char_uuid
                            self.successful_characteristics[device_name] = char_uuid
                            
                            break
                        except Exception as e:
                            print(f"[smart_connect_device]: Error using characteristic {char_uuid} for {device_name}: {str(e)}")
                            continue
                
                # If no preferred characteristic worked, try any available notify characteristic
                if not used_characteristic and available_characteristics:
                    for avail_char in available_characteristics:
                        try:
                            print(f"[smart_connect_device]: Trying fallback characteristic {avail_char} for {device_name}")
                            
                            device_notification_handler = lambda s, d: self.loop.create_task(
                                self.handle_notification(device_name, queue, s, d)
                            )
                            
                            await client.start_notify(avail_char, device_notification_handler)
                            print(f"[smart_connect_device]: Successfully started notifications using fallback {avail_char}")
                            used_characteristic = avail_char
                            
                            # Remember this characteristic for future use
                            DEVICE_CHARACTERISTICS[device_name] = avail_char
                            self.successful_characteristics[device_name] = avail_char
                            
                            break
                        except Exception as e:
                            print(f"[smart_connect_device]: Error using fallback characteristic {avail_char}: {str(e)}")
                            continue
                
                if not used_characteristic:
                    print(f"[smart_connect_device]: Could not find any usable characteristic for {device_name}")
                    if client.is_connected:
                        await client.disconnect()
                    connected = False
                    connected_devices[device_name] = False
                    retry_count += 1
                    await asyncio.sleep(retry_delay)
                    continue
                
                # Reset retry count after successful connection
                retry_count = 0
                
                # Keep connection alive with improved heartbeat
                keep_alive_count = 0
                while client.is_connected:
                    await asyncio.sleep(0.5)
                    keep_alive_count += 1
                    if keep_alive_count % 20 == 0:  # Every 10 seconds
                        # Optional: Add a connection check here if needed
                        pass
                    
            except Exception as e:
                print(f"[smart_connect_device]: Error with {device_name}: {str(e)}")
                retry_count += 1
                if retry_count < max_retries:
                    print(f"[smart_connect_device]: Retrying connection to {device_name} ({retry_count}/{max_retries})...")
                    await asyncio.sleep(retry_delay)
                else:
                    print(f"[smart_connect_device]: Max retries reached for {device_name}. Giving up.")
                    break
            finally:
                # Ensure we disconnect cleanly if the connection was established
                if connected and client and client.is_connected:
                    try:
                        await client.disconnect()
                    except Exception as e:
                        print(f"[smart_connect_device]: Error disconnecting from {device_name}: {str(e)}")
                
                connected = False
                # Update the connected status to False when disconnected
                connected_devices[device_name] = False
                print(f"[smart_connect_device]: Disconnected from {device_name}")
        
        print(f"[smart_connect_device]: Device connection task for {device_name} ended.")

    async def batch_connect_devices(self, devices_batch, device_queues, connected_devices):
        """Connect to a batch of devices in parallel with staggered start times"""
        tasks = []
        for i, (name, address, queue) in enumerate(devices_batch):
            # giảm xuống 200ms để nhanh hơn
            await asyncio.sleep(0.2)
            tasks.append(self.smart_connect_device(name, address, queue, connected_devices))
        
        # Run all tasks in this batch concurrently
        await asyncio.gather(*tasks)

    async def READ_DATA(self, devices_dict=None):
        """
        Main method to read data from multiple Bluetooth devices with improved parallel connection
        :param devices_dict: Optional dictionary to override the one provided at init
        """
        if devices_dict:
            self.devices_dict = devices_dict
        
        if not self.devices_dict:
            print("[READ_DATA]: No devices to connect to. Please provide a devices dictionary.")
            return
        
        print(f"[READ_DATA]: Starting data collection from {len(self.devices_dict)} devices:")
        for name, address in self.devices_dict.items():
            print(f"[READ_DATA]: - {name}: {address}")
            
        # Print any known device-specific characteristics
        if DEVICE_CHARACTERISTICS:
            print("[READ_DATA]: Using these device-specific characteristics:")
            for name, uuid in DEVICE_CHARACTERISTICS.items():
                print(f"[READ_DATA]: - {name}: {uuid}")
        
        # Create queues, collect names and addresses
        device_queues = []
        device_names = list(self.devices_dict.keys())
        device_addresses = list(self.devices_dict.values())
        for _ in range(len(self.devices_dict)):
            device_queues.append(asyncio.Queue())
        
        # Shared dictionary to track connection status
        connected_devices = {name: False for name in device_names}
        
        # Tùy chỉnh batch_size dựa trên tổng thiết bị (tối đa 3)
        batch_size = min(3, len(self.devices_dict))
        device_batches = []
        current_batch = []
        
        for i, (name, address) in enumerate(zip(device_names, device_addresses)):
            current_batch.append((name, address, device_queues[i]))
            if len(current_batch) == batch_size or i == len(device_names) - 1:
                device_batches.append(current_batch)
                current_batch = []
        
        # Create the data processing task
        processing_task = self.process_data(device_queues, device_names, connected_devices)
        
        # Connection task for managing the batches with better spacing
        async def connection_manager():
            print(f"[READ_DATA]: Starting connection of {len(self.devices_dict)} devices in {len(device_batches)} batches")
            
            # First attempt - connect all batches
            for i, batch in enumerate(device_batches):
                print(f"[READ_DATA]: Connecting batch {i+1}/{len(device_batches)} with {len(batch)} devices")
                await self.batch_connect_devices(batch, device_queues, connected_devices)
                # Giảm slightly thời gian chờ nếu muốn nhanh hơn
                await asyncio.sleep(1.5)
            
            # Continuous reconnection for dropped devices
            while True:
                # Wait before checking for disconnected devices
                await asyncio.sleep(5)
                
                # Check for any disconnected devices
                disconnected_devices = [(n, self.devices_dict[n]) for n, s in connected_devices.items() if not s]
                
                if disconnected_devices:
                    print(f"[READ_DATA]: Reconnecting {len(disconnected_devices)} disconnected devices")
                    
                    # Create new batches for reconnection
                    reconnect_batches = []
                    current_batch = []
                    for i, (name, address) in enumerate(disconnected_devices):
                        current_batch.append((name, address, device_queues[device_names.index(name)]))
                        if len(current_batch) == batch_size or i == len(disconnected_devices) - 1:
                            reconnect_batches.append(current_batch)
                            current_batch = []
                    
                    # Reconnect each batch
                    for i, batch in enumerate(reconnect_batches):
                        print(f"[READ_DATA]: Reconnecting batch {i+1}/{len(reconnect_batches)}")
                        await self.batch_connect_devices(batch, device_queues, connected_devices)
                        await asyncio.sleep(1.5)  # Wait giữa batches nhẹ nhàng hơn
        
        # Run all tasks concurrently
        try:
            print("[READ_DATA]: Starting connections and data processing...")
            await asyncio.gather(
                connection_manager(),
                processing_task
            )
        except asyncio.CancelledError:
            print("[READ_DATA]: Operation cancelled")
        except KeyboardInterrupt:
            print("[READ_DATA]: User interrupted")
        except Exception as e:
            print(f"[READ_DATA]: Unexpected error: {e}")

# Example usage:
if __name__ == "__main__":
    print("[main]: Starting Bluetooth device detection and data reading program")
    
    # Example list of device names you're looking for
    target_devices = ["IMU1", "IMU2", "IMU3", "IMU4", "IMU5", "IMU6"]
    print(f"[main]: Looking for target devices: {target_devices}")
    
    # First, run the detector to find devices
    detector = BLUETOOTH_DETECTOR(target_devices)
    print("[main]: Running INSYSTEM_SCANNING...")
    detector.loop.run_until_complete(detector.INSYSTEM_SCANNING())  
    print("[main]: Running OUTSYSTEM_SCANNING...")
    detector.loop.run_until_complete(detector.OUTSYSTEM_SCANNING())
    
    print(f"[main]: Available devices: {available_devices_address}")
    
    # Then use the data reader to connect and read data
    if available_devices_address:
        print("[main]: Using discovered devices")
        reader = BLUETOOTH_DATA_READER()
        reader.start_reading()
    else:
        # For testing with hardcoded addresses if no devices found
        print("[main]: No devices discovered, using hardcoded test devices")
        test_devices = {
            "IMU1": "00:BE:44:C0:6B:3D",
            "IMU2": "58:3B:C2:56:8E:07"
        }
        reader = BLUETOOTH_DATA_READER(test_devices)
        reader.start_reading()
    
    print("[main]: Program ended")



