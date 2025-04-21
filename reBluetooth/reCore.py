import asyncio
import struct
import time # For basic timing/logging
from collections import deque
from typing import Dict, List, Tuple, Optional, Set

from bleak import BleakScanner, BleakClient, BleakError

# --- Constants ---
# UUID for the specific characteristic sending the 7 float values
# Keep the one you provided, or update if needed
CHARACTERISTIC_UUID = "61A885A4-41C3-60D0-9A53-6D652A70D29C"
# Connection parameters
CONNECT_TIMEOUT = 30.0  # Increased timeout for potentially slow connections (seconds)
MAX_CONNECT_RETRIES = 6
INITIAL_BACKOFF_DELAY = 1.0  # Seconds
MAX_BACKOFF_DELAY = 8.0     # Seconds
# Batching parameters
CONNECT_BATCH_SIZE = 3      # How many devices to try connecting simultaneously
BATCH_DELAY = 1.5           # Delay between starting connection batches (seconds)
# Data processing
DATA_QUEUE_TIMEOUT = 0.1    # Timeout for getting data from queue (seconds)


# --- Global State (minimized) ---
# Using a dictionary to store available devices found by the scanner
# Key: device name (str), Value: device address (str)
discovered_devices: Dict[str, str] = {}

# --- Helper Functions ---
def get_timestamp() -> str:
    """Returns a formatted timestamp for logging."""
    return time.strftime("%H:%M:%S", time.localtime())

# --- Device Detection Class ---
class BluetoothDetector:
    """
    Scans for nearby BLE devices and identifies target devices based on their names.
    """
    def __init__(self, target_device_names: Set[str]):
        """
        Initializes the detector.
        :param target_device_names: A set of names of the devices to look for.
        """
        self.target_names = target_device_names
        print(f"[{get_timestamp()}][Detector] Initialized to look for: {', '.join(target_device_names)}")

    async def scan_for_devices(self, scan_duration: float = 10.0) -> Dict[str, str]:
        """
        Scans for BLE devices for a specified duration and returns found target devices.
        :param scan_duration: How long to scan for (seconds).
        :return: Dictionary of found target devices {name: address}.
        """
        print(f"[{get_timestamp()}][Detector] Starting BLE scan for {scan_duration} seconds...")
        found_devices: Dict[str, str] = {}
        try:
            discovered = await BleakScanner.discover(timeout=scan_duration)
            for device in discovered:
                if device.name in self.target_names:
                    if device.name not in found_devices:
                        print(f"[{get_timestamp()}][Detector] Found target device: {device.name} ({device.address})")
                        found_devices[device.name] = device.address
                    # else: device already found, ignore duplicate detection in same scan
            
            missing_devices = self.target_names - set(found_devices.keys())
            if missing_devices:
                print(f"[{get_timestamp()}][Detector] Scan complete. Could not find: {', '.join(missing_devices)}")
            else:
                 print(f"[{get_timestamp()}][Detector] Scan complete. Found all target devices.")

        except BleakError as e:
            print(f"[{get_timestamp()}][Detector] ERROR during scanning: {e}")
        except Exception as e:
             print(f"[{get_timestamp()}][Detector] UNEXPECTED ERROR during scanning: {e}")
             
        return found_devices

# --- Data Reading Class ---
class BluetoothDataReader:
    """
    Connects to multiple BLE devices, subscribes to notifications,
    and processes incoming data.
    """
    def __init__(self, characteristic_uuid: str = CHARACTERISTIC_UUID):
        self.characteristic_uuid = characteristic_uuid
        # Create a new event loop instead of trying to get the current one
        self.loop = None
        # Shared state: key=device_address, value=asyncio.Queue
        self.data_queues: Dict[str, asyncio.Queue] = {}
        # Shared state: key=device_address, value=connection_status (bool)
        self.connection_status: Dict[str, bool] = {}
        # Add synchronization mechanism
        self.all_connected_event = None
        self.should_process_data = False
        self.expected_device_count = 0

    async def _handle_notification(self, device_name: str, device_address: str, sender: int, data: bytearray):
        """
        Callback function for processing incoming BLE notifications.
        Parses data and puts it into the corresponding device's queue.
        """
        if device_address not in self.data_queues:
            print(f"[{get_timestamp()}][{device_name}] Warning: Received data but no queue found for {device_address}. Ignoring.")
            return

        if len(data) >= 28: # Check if data has at least 28 bytes for 7 floats
            try:
                values = struct.unpack('<7f', data[:28])
                # Format output with device name and values
                output = f"[{get_timestamp()}][DATA][{device_name}] {' '.join(f'{v:.6f}' for v in values)}"
                try:
                    self.data_queues[device_address].put_nowait(output)
                except asyncio.QueueFull:
                     print(f"[{get_timestamp()}][{device_name}] Warning: Data queue full for {device_address}. Data lost.")
            except struct.error:
                 print(f"[{get_timestamp()}][{device_name}] Error: Could not unpack data (length {len(data)}).")
            except Exception as e:
                print(f"[{get_timestamp()}][{device_name}] Error processing notification: {e}")
        # else:
        #     print(f"[{get_timestamp()}][{device_name}] Received short data packet (length {len(data)}). Ignoring.")


    async def _connect_and_monitor(self, device_name: str, device_address: str):
        """
        Manages the connection lifecycle for a single device, including retries and notifications.
        """
        retry_count = 0
        backoff_delay = INITIAL_BACKOFF_DELAY
        self.connection_status[device_address] = False # Initial state

        while retry_count < MAX_CONNECT_RETRIES:
            try:
                print(f"[{get_timestamp()}][{device_name}] Attempting connection ({retry_count + 1}/{MAX_CONNECT_RETRIES})...")
                async with BleakClient(device_address, timeout=CONNECT_TIMEOUT) as client:
                    if client.is_connected:
                        print(f"[{get_timestamp()}][{device_name}] Connected successfully to {device_address}")
                        self.connection_status[device_address] = True
                        
                        # Check if all devices are now connected
                        connected_count = sum(1 for status in self.connection_status.values() if status)
                        print(f"[{get_timestamp()}][{device_name}] {connected_count}/{self.expected_device_count} devices connected")
                        
                        if connected_count == self.expected_device_count and not self.should_process_data:
                            print(f"[{get_timestamp()}][Reader] ALL {self.expected_device_count} DEVICES NOW CONNECTED! Starting data processing...")
                            self.should_process_data = True
                            self.all_connected_event.set()
                            
                        retry_count = 0  # Reset retries on successful connection
                        backoff_delay = INITIAL_BACKOFF_DELAY # Reset backoff

                        try:
                            # Check for characteristic existence more carefully
                            char_to_use = None
                            svcs = await client.get_services() # Recommended way
                            for service in svcs:
                                for char in service.characteristics:
                                    if char.uuid == self.characteristic_uuid:
                                        if "notify" in char.properties:
                                            char_to_use = char.uuid
                                            print(f"[{get_timestamp()}][{device_name}] Found target characteristic {char_to_use}")
                                            break
                                        else:
                                            print(f"[{get_timestamp()}][{device_name}] Warning: Target characteristic {self.characteristic_uuid} found but does not support notify.")
                                if char_to_use: break
                            
                            # Fallback if specific characteristic not found or unusable
                            if not char_to_use:
                                print(f"[{get_timestamp()}][{device_name}] Target characteristic not found/usable. Searching for any notifiable characteristic...")
                                for service in svcs:
                                    for char in service.characteristics:
                                        if "notify" in char.properties:
                                            char_to_use = char.uuid
                                            print(f"[{get_timestamp()}][{device_name}] Using alternative notifiable characteristic: {char_to_use}")
                                            break
                                    if char_to_use: break

                            if not char_to_use:
                                print(f"[{get_timestamp()}][{device_name}] Error: No suitable notifiable characteristic found. Disconnecting.")
                                return # Exit task for this device

                            # Start notifications
                            await client.start_notify(
                                char_to_use,
                                lambda sender, data: asyncio.create_task(self._handle_notification(device_name, device_address, sender, data))
                                # Using create_task avoids blocking the notification handler
                            )
                            print(f"[{get_timestamp()}][{device_name}] Notifications started.")

                            # Keep connection alive while client is connected
                            while client.is_connected:
                                await asyncio.sleep(1.0) # Check connection status periodically

                        except BleakError as e:
                            print(f"[{get_timestamp()}][{device_name}] BleakError during operation: {e}")
                        except Exception as e:
                            print(f"[{get_timestamp()}][{device_name}] Unexpected error during operation: {e}")
                        finally:
                             print(f"[{get_timestamp()}][{device_name}] Operation loop finished or error occurred.")
                             # Attempt to stop notifications gracefully if still connected
                             if client.is_connected and char_to_use:
                                 try:
                                     await client.stop_notify(char_to_use)
                                     print(f"[{get_timestamp()}][{device_name}] Notifications stopped.")
                                 except Exception as e:
                                     print(f"[{get_timestamp()}][{device_name}] Error stopping notifications: {e}")
                    else:
                        # This case might be less common with `async with` but handle defensively
                        print(f"[{get_timestamp()}][{device_name}] Failed to establish connection within timeout.")
                        # Let the retry logic handle this

            except BleakError as e:
                print(f"[{get_timestamp()}][{device_name}] BleakError on connection attempt {retry_count + 1}: {e}")
            except asyncio.TimeoutError:
                print(f"[{get_timestamp()}][{device_name}] Connection attempt {retry_count + 1} timed out.")
            except Exception as e:
                print(f"[{get_timestamp()}][{device_name}] Unexpected error on connection attempt {retry_count + 1}: {e}")

            # If connection failed or loop exited unexpectedly
            self.connection_status[device_address] = False
            retry_count += 1
            if retry_count < MAX_CONNECT_RETRIES:
                print(f"[{get_timestamp()}][{device_name}] Retrying in {backoff_delay:.1f} seconds...")
                await asyncio.sleep(backoff_delay)
                backoff_delay = min(backoff_delay * 1.5, MAX_BACKOFF_DELAY) # Exponential backoff
            else:
                 print(f"[{get_timestamp()}][{device_name}] Max retries reached. Giving up on {device_address}.")
                 break # Exit the while loop

        # Ensure status is False if loop finishes
        self.connection_status[device_address] = False
        print(f"[{get_timestamp()}][{device_name}] Connection task finished.")


    async def _process_device_data(self, device_name: str, device_address: str):
        """
        Continuously processes data from a single device's queue.
        Runs independently for each device.
        """
        print(f"[{get_timestamp()}][{device_name}] Data processor initialized. Waiting for all devices to connect...")
        queue = self.data_queues[device_address]
        
        # Wait for the "all connected" event before processing any data
        await self.all_connected_event.wait()
        print(f"[{get_timestamp()}][{device_name}] Starting data processor - all devices now connected.")
        
        while True:
            try:
                # Wait indefinitely until data is available or task is cancelled
                data = await queue.get()
                print(data) # Data is pre-formatted in _handle_notification
                queue.task_done()
            except asyncio.CancelledError:
                 print(f"[{get_timestamp()}][{device_name}] Data processor cancelled.")
                 break
            except Exception as e:
                 print(f"[{get_timestamp()}][{device_name}] Error in data processor: {e}")
                 # Optionally add a small sleep to prevent rapid error loops
                 await asyncio.sleep(0.1)


    async def run_data_collection(self, devices_to_connect: Dict[str, str]):
        """
        Main method to manage connections and data processing for multiple devices.
        :param devices_to_connect: Dictionary of devices {name: address} to connect to.
        """
        if not devices_to_connect:
            print(f"[{get_timestamp()}][Reader] No devices provided to connect to.")
            return

        print(f"[{get_timestamp()}][Reader] Starting data collection for {len(devices_to_connect)} devices:")
        for name, address in devices_to_connect.items():
            print(f"  - {name}: {address}")
            # Initialize queue and status for each device
            self.data_queues[address] = asyncio.Queue()
            self.connection_status[address] = False # Initial state
            
        # Initialize synchronization event and set expected device count
        self.expected_device_count = len(devices_to_connect)
        self.all_connected_event = asyncio.Event()
        self.should_process_data = False
        print(f"[{get_timestamp()}][Reader] Waiting for all {self.expected_device_count} devices to connect before processing data...")

        # Prepare list of (name, address) tuples for batching
        device_list = list(devices_to_connect.items())
        connection_tasks = []
        processing_tasks = []

        # --- Batch Connection ---
        print(f"[{get_timestamp()}][Reader] Starting connections in batches of {CONNECT_BATCH_SIZE}...")
        for i in range(0, len(device_list), CONNECT_BATCH_SIZE):
            batch = device_list[i:i + CONNECT_BATCH_SIZE]
            batch_tasks = []
            print(f"[{get_timestamp()}][Reader] Processing batch {i // CONNECT_BATCH_SIZE + 1}...")
            for name, address in batch:
                 print(f"  - Launching connection task for {name} ({address})")
                 # Create connection task
                 conn_task = asyncio.create_task(self._connect_and_monitor(name, address))
                 connection_tasks.append(conn_task)
                 # Create data processing task
                 proc_task = asyncio.create_task(self._process_device_data(name, address))
                 processing_tasks.append(proc_task)
            
            # Optional: Wait a bit between starting batches if adapter struggles
            if i + CONNECT_BATCH_SIZE < len(device_list):
                 print(f"[{get_timestamp()}][Reader] Waiting {BATCH_DELAY}s before next batch...")
                 await asyncio.sleep(BATCH_DELAY)

        print(f"[{get_timestamp()}][Reader] All connection and processing tasks launched.")

        # --- Monitor and Keep Alive ---
        try:
            # Keep the main function alive while tasks are running
            # We don't necessarily need to wait for all connection tasks here,
            # as they handle their own lifecycle. We mainly wait for processing tasks.
            # Or simply monitor connection status periodically.
            while True:
                connected_count = sum(1 for status in self.connection_status.values() if status)
                #print(f"[{get_timestamp()}][Reader] Monitoring: {connected_count}/{len(devices_to_connect)} devices connected.")
                await asyncio.sleep(5) # Check status every 5 seconds
                # Add logic here if you need to stop based on some condition
                
                # Check if any tasks are still running
                if not any(not t.done() for t in connection_tasks + processing_tasks):
                     print(f"[{get_timestamp()}][Reader] All tasks seem to have completed.")
                     break

        except asyncio.CancelledError:
            print(f"[{get_timestamp()}][Reader] Main monitoring task cancelled.")
        finally:
            print(f"[{get_timestamp()}][Reader] Shutting down...")
            # Cancel all running tasks to ensure clean exit
            for task in connection_tasks + processing_tasks:
                if not task.done():
                    task.cancel()
            # Wait for tasks to finish cancellation
            await asyncio.gather(*(connection_tasks + processing_tasks), return_exceptions=True)
            print(f"[{get_timestamp()}][Reader] All tasks cancelled or finished.")


    def start(self, devices_to_connect: Dict[str, str]):
        """Starts the data collection process (non-async wrapper)."""
        try:
            # Create a new loop for this instance
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            
            # If we have fewer devices than expected, adjust expectations
            target_count = len(target_device_names)
            found_count = len(devices_to_connect)
            
            if found_count < target_count:
                print(f"[{get_timestamp()}][Reader] WARNING: Only {found_count}/{target_count} target devices found.")
                proceed = input(f"Continue with {found_count} devices? (y/n): ").lower()
                if proceed != 'y':
                    print(f"[{get_timestamp()}][Reader] Aborting as requested.")
                    return
                print(f"[{get_timestamp()}][Reader] Proceeding with {found_count} available devices.")
                
            self.loop.run_until_complete(self.run_data_collection(devices_to_connect))
        except KeyboardInterrupt:
            print(f"\n[{get_timestamp()}][Reader] KeyboardInterrupt received. Stopping.")
            # Cancellation is handled within run_data_collection's finally block
        finally:
            if self.loop and self.loop.is_running():
                self.loop.stop()
            if self.loop:
                # Close any remaining tasks
                pending = asyncio.all_tasks(self.loop)
                if pending:
                    self.loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                self.loop.close()


# --- Main Execution ---
if __name__ == "__main__":
    print(f"[{get_timestamp()}][Main] Starting Bluetooth IMU program...")

    # Define the names of the IMU devices you want to connect to
    target_device_names: Set[str] = {"IMU1", "IMU2", "IMU3", "IMU4", "IMU5", "IMU6"}
    # target_device_names: Set[str] = {"IMU1", "IMU2"} # Example for testing with fewer devices

    # --- 1. Device Discovery ---
    detector = BluetoothDetector(target_device_names)
    # Run scan asynchronously
    try:
        # Adjust scan_duration as needed. Longer might find more devices but takes time.
        discovered_devices = asyncio.run(detector.scan_for_devices(scan_duration=15.0))
    except Exception as e:
        print(f"[{get_timestamp()}][Main] Error during device discovery: {e}")
        discovered_devices = {} # Ensure it's initialized

    print(f"[{get_timestamp()}][Main] Discovery finished. Found devices: {discovered_devices}")

    # --- 2. Data Reading ---
    devices_to_process = discovered_devices

    # Optional: Fallback to hardcoded addresses for testing if discovery fails
    if not devices_to_process:
        print(f"[{get_timestamp()}][Main] No target devices discovered via scanning.")
        use_fallback = input("Attempt to use hardcoded test addresses? (y/n): ").lower()
        if use_fallback == 'y':
             # --- !!! IMPORTANT: Replace with YOUR actual device addresses !!! ---
             test_devices = {
                 "IMU1": "XX:XX:XX:XX:XX:XX", # Replace with actual address
                 "IMU2": "YY:YY:YY:YY:YY:YY", # Replace with actual address
                 # Add other IMUs if needed
             }
             # Filter test devices based on the initial target_device_names
             devices_to_process = {name: addr for name, addr in test_devices.items() if name in target_device_names}
             print(f"[{get_timestamp()}][Main] Using hardcoded addresses for: {list(devices_to_process.keys())}")
        else:
             print(f"[{get_timestamp()}][Main] Exiting as no devices were found or selected.")
             devices_to_process = {}


    if devices_to_process:
        reader = BluetoothDataReader()
        reader.start(devices_to_process) # This blocks until KeyboardInterrupt or completion
    else:
        print(f"[{get_timestamp()}][Main] No devices to connect to. Program finished.")

    print(f"[{get_timestamp()}][Main] Program ended.")