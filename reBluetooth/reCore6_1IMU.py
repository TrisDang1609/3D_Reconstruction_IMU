import asyncio
from bleak import BleakScanner, BleakClient, BleakError, BleakGATTCharacteristic
import struct
import numpy as np
from collections import deque
import time
import logging
from enum import Enum
import csv # Import csv module
import os # Import os for flush

# Configure more detailed logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

# --- UUIDs ---
# IMU_SERVICE_UUID = "2412b5cbd460800c15c39ba9ac5a8ade" # Service UUID from Arduino code
# LED_CONTROL_CHAR_UUID = "5b02651040-88c29746d8be6c736a087a" # LED Control UUID from Arduino code
IMU_NOTIFY_CHAR_UUID = "61a885a4-41c3-60d0-9a53-6d652a70d29c" # Notify UUID from Arduino code (matches btn_report_characteristic_uuid)

# Known characteristic UUIDs for IMU devices
IMU_CHARACTERISTIC_UUIDS = [
    "61A885A4-41C3-60D0-9A53-6D652A70D29C", # Primary UUID
    IMU_NOTIFY_CHAR_UUID,
    # Add other UUIDs if needed for testing
]
# Remove duplicate UUIDs and normalize
IMU_CHARACTERISTIC_UUIDS = list(set(uuid.upper() for uuid in IMU_CHARACTERISTIC_UUIDS))

# --- Enum for device status and Phase ---
class DeviceStatus(Enum):
    DISCONNECTED = 0
    CONNECTING = 1
    CONNECTED_NO_NOTIFY = 2 # Connected, notify not yet enabled
    CONNECTED_NOTIFYING = 3 # Connected and notifying
    FAILED_PERMANENTLY = 4
    WAITING_RETRY = 5
    DISCONNECTING = 6

class ReaderPhase(Enum):
    INITIALIZING = 0
    PHASE_1_CONNECTING = 1
    PHASE_2_STARTING_NOTIFICATIONS = 2
    PHASE_2_READING_DATA = 3
    STOPPING = 4
    STOPPED = 5

# --- Rewritten BLUETOOTH_DATA_READER Class ---
class BLUETOOTH_DATA_READER:

    def __init__(self, devices_dict,
                 connection_interval_sec=5.0,
                 retry_interval_sec=10.0,
                 max_connect_retries=5,
                 bleak_timeout=20.0,
                 csv_filename="imu_data.csv"): # Add csv_filename parameter
        """
        Initialize Data Reader with 2 Phases.

        Args:
            devices_dict (dict): Dictionary {device_name: mac_address}. Should contain IMU6 for single IMU read.
            connection_interval_sec (float): Wait time between connection attempts for DIFFERENT devices.
            retry_interval_sec (float): Wait time before retrying a FAILED device connection.
            max_connect_retries (int): Maximum connection retries per device.
            bleak_timeout (float): Timeout for BleakClient operations.
            csv_filename (str): Name of the CSV file to save data.
        """
        if not devices_dict:
            raise ValueError("devices_dict cannot be empty.")

        # Modified: Now we only need one target device
        self.target_device_order = ["IMU6"]
        if not all(name in devices_dict for name in self.target_device_order):
             log.warning(f"devices_dict is missing required device IMU6. Cannot proceed with reading.")
             raise ValueError(f"devices_dict must contain IMU6 for single device mode.")

        self.devices_dict = devices_dict
        self.device_names = list(devices_dict.keys()) # All devices passed in
        self.num_target_devices = len(self.target_device_order) # Now just 1

        # Fix: Initialize loop here if not already initialized externally
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

        # Configuration
        self.connection_interval = connection_interval_sec
        self.retry_interval = retry_interval_sec
        self.max_retries = max_connect_retries
        self.bleak_timeout = bleak_timeout
        self.csv_filename = csv_filename # Store CSV filename
        self.csv_file = None # Initialize CSV file object
        self.csv_writer = None # Initialize CSV writer object

        # State and resource management
        self.device_states = {name: DeviceStatus.DISCONNECTED for name in self.device_names}
        self.device_clients = {name: None for name in self.device_names}
        # Ensure queues exist for all devices, including the target ones
        self.device_queues = {name: asyncio.Queue() for name in self.device_names}
        self.device_retries = {name: 0 for name in self.device_names}
        # Store characteristic object found in Phase 1 for use in Phase 2
        self.device_notify_chars = {name: None for name in self.device_names}
        self.notification_handlers = {} # Store handler to detach on stop
        self.monitor_tasks = {}

        # Phase management
        self.current_phase = ReaderPhase.INITIALIZING
        self.all_connected_event = asyncio.Event() # Event signaling Phase 1 completion

        self._main_tasks = []
        self._running = False

    # --- Notification Handling (Used only in Phase 2) ---
    async def _handle_notification(self, device_name, sender: BleakGATTCharacteristic, data: bytearray):
        """Handle data received from notifications and put it into the queue."""
        # Only queue data if we are in the reading phase
        if self.current_phase != ReaderPhase.PHASE_2_READING_DATA:
            # log.debug(f"[{device_name}] Ignoring notification, not in READING_DATA phase.")
            return

        queue = self.device_queues.get(device_name)
        if not queue:
            # This check might be redundant if queues are always created, but safe to keep.
            log.warning(f"[{device_name}] Queue not found when receiving notification.")
            return

        # ***** MODIFIED: Expect 6 floats = 24 bytes *****
        if len(data) >= 24:
            try:
                # ***** MODIFIED: Unpack 6 floats ('<6f') from the first 24 bytes *****
                values = struct.unpack('<6f', data[:24])
                timestamp = time.time()
                # Store the raw values or pre-formatted string in the queue
                # Storing raw values might be more flexible, but string is requested
                # The format string automatically adapts to the number of items in 'values'
                output_str = f"[DATA] {device_name} {timestamp:.6f}: {' '.join(f'{v:.6f}' for v in values)}"
                await queue.put(output_str)
                # log.debug(f"[{device_name}] Data put into queue. Queue size: {queue.qsize()}")
            except struct.error as e:
                log.warning(f"[{device_name}] Error unpacking data: {e}. Data (hex): {data.hex()}")
            except Exception as e:
                log.error(f"[{device_name}] Unknown error processing notification: {e}")
        else:
            # ***** MODIFIED: Update log message for expected length *****
            log.debug(f"[{device_name}] Notification data too short ({len(data)} bytes), expected at least 24 bytes.")

    # --- Connection (Phase 1) ---
    # [ ... _attempt_connection method remains unchanged ... ]
    # [ ... _mark_connection_failure method remains unchanged ... ]
    # [ ... _disconnect_client method remains unchanged ... ]
    # [ ... _cleanup_client_resource method remains unchanged ... ]
    # [ ... _start_monitoring_task method remains unchanged ... ]
    # [ ... _connection_monitor method remains unchanged ... ]
    # [ ... _connection_manager method remains unchanged ... ]
    # [ ... _notification_enabler method remains unchanged ... ]
    # [ ... _start_notify_for_device method remains unchanged ... ]
    async def _attempt_connection(self, device_name, address):
        """
        Phase 1: Attempt to connect to a device and identify the characteristic.
        DO NOT start notify here.
        """
        log.info(f"[{device_name}][Phase 1] Starting connection to {address} (Attempt {self.device_retries[device_name] + 1}/{self.max_retries})...")
        self.device_states[device_name] = DeviceStatus.CONNECTING

        client = BleakClient(address, timeout=self.bleak_timeout)
        found_notify_char = None

        try:
            await client.connect()
            if not client.is_connected:
                 log.warning(f"[{device_name}][Phase 1] Connection failed (client not connected).")
                 await self._mark_connection_failure(device_name, client) # Pass client for cleanup
                 return None

            log.info(f"[{device_name}][Phase 1] Successfully connected to {address}.")

            # Attempt to increase MTU (optional) - Handle AttributeError
            try:
                # Check if the method exists before calling
                if hasattr(client, "exchange_mtu") and callable(client.exchange_mtu):
                     mtu = await client.exchange_mtu(247) # Keep trying high MTU
                     log.info(f"[{device_name}][Phase 1] MTU set to: {mtu}")
                else:
                     log.warning(f"[{device_name}][Phase 1] This Bleak version lacks 'exchange_mtu'. Skipping.")
            except AttributeError: # Catch specific error if the check above is insufficient
                log.warning(f"[{device_name}][Phase 1] Cannot set MTU: AttributeError. Continuing with default MTU.")
            except Exception as e:
                log.warning(f"[{device_name}][Phase 1] Error setting MTU: {e}. Continuing with default MTU.")


            # Identify characteristic to use for notify later
            characteristics_to_try = IMU_CHARACTERISTIC_UUIDS # Use common list
            log.debug(f"[{device_name}][Phase 1] UUIDs to check: {characteristics_to_try}")

            available_chars = {} # Store {uuid_str: char_object}
            try:
                # Use services property instead of get_services() method to avoid FutureWarning
                svcs = client.services
                for service in svcs:
                    for char in service.characteristics:
                        if "notify" in char.properties:
                            available_chars[char.uuid.upper()] = char # Store the object itself
                log.debug(f"[{device_name}][Phase 1] Characteristics supporting notify: {list(available_chars.keys())}")
            except Exception as e:
                 log.error(f"[{device_name}][Phase 1] Error getting services/characteristics: {e}")
                 await self._disconnect_client(client, device_name) # Disconnect before marking as failed
                 await self._mark_connection_failure(device_name, None) # Client has been disconnected
                 return None

            # Find suitable characteristic
            for char_uuid_str in characteristics_to_try:
                 normalized_uuid = char_uuid_str.upper()
                 if normalized_uuid in available_chars:
                     found_notify_char = available_chars[normalized_uuid]
                     log.info(f"[{device_name}][Phase 1] Found suitable characteristic for notify: {found_notify_char.uuid} (Handle: {found_notify_char.handle})")
                     break

            if not found_notify_char:
                log.error(f"[{device_name}][Phase 1] No suitable characteristic supporting notify found.")
                await self._disconnect_client(client, device_name)
                await self._mark_connection_failure(device_name, None)
                return None

            # Connection successful, characteristic identified
            self.device_clients[device_name] = client
            self.device_notify_chars[device_name] = found_notify_char # Store char object
            self.device_states[device_name] = DeviceStatus.CONNECTED_NO_NOTIFY # New status
            self.device_retries[device_name] = 0
            log.info(f"[{device_name}][Phase 1] Connection setup successful. Waiting for other devices.")

            # Start connection monitoring task
            self._start_monitoring_task(device_name, client)

            return client

        except BleakError as e:
            log.error(f"[{device_name}][Phase 1] Bleak error during connection: {e}")
            await self._mark_connection_failure(device_name, client)
            return None
        except asyncio.TimeoutError:
            log.error(f"[{device_name}][Phase 1] Timeout during connection.")
            await self._mark_connection_failure(device_name, client)
            return None
        except Exception as e:
            log.exception(f"[{device_name}][Phase 1] Unknown error during connection: {e}")
            await self._mark_connection_failure(device_name, client)
            return None

    async def _mark_connection_failure(self, device_name, client_to_cleanup=None):
        """Mark the device as failed and decide the next state."""
        # Clean up client if necessary and possible
        if client_to_cleanup:
            if client_to_cleanup.is_connected:
                 await self._disconnect_client(client_to_cleanup, device_name, suppress_errors=True)
            else:
                 await self._cleanup_client_resource(client_to_cleanup)

        self.device_clients[device_name] = None # Ensure client instance is removed
        self.device_notify_chars[device_name] = None # Clear stored char
        self.device_retries[device_name] += 1

        if self.device_retries[device_name] >= self.max_retries:
            log.error(f"[{device_name}] Reached maximum connection attempts ({self.max_retries}). Giving up permanently.")
            self.device_states[device_name] = DeviceStatus.FAILED_PERMANENTLY
        else:
            log.info(f"[{device_name}] Will retry connection after {self.retry_interval} seconds.")
            self.device_states[device_name] = DeviceStatus.WAITING_RETRY

    async def _disconnect_client(self, client: BleakClient, device_name: str, suppress_errors=False):
        """Safely disconnect a client."""
        if not client:
            return

        current_state = self.device_states.get(device_name)
        log.info(f"[{device_name}] Disconnecting (current status: {current_state})...")
        # Only change status if not already disconnecting or disconnected
        if current_state not in [DeviceStatus.DISCONNECTING, DeviceStatus.DISCONNECTED]:
             self.device_states[device_name] = DeviceStatus.DISCONNECTING

        # Stop notify IF it was enabled (in Phase 2 or during stop)
        handler = self.notification_handlers.pop(device_name, None)
        notify_char = self.device_notify_chars.get(device_name) # Get stored char
        # Only stop notify if client is connected and handler exists (meaning start_notify was called before)
        if handler and notify_char and client.is_connected: # Skip state check here, always try stop if handler exists
             try:
                 log.debug(f"[{device_name}] Stopping notify for UUID {notify_char.uuid}...")
                 await client.stop_notify(notify_char) # Use char object
                 log.debug(f"[{device_name}] Notify stopped.")
             except Exception as e:
                  # Log error but do not stop the disconnect process
                  log.warning(f"[{device_name}] Error stopping notify for {notify_char.uuid} (might already be stopped): {e}")

        # Disconnect client
        try:
            if client.is_connected:
                await client.disconnect()
                log.info(f"[{device_name}] Disconnected successfully.")
            else:
                 log.info(f"[{device_name}] Client was already disconnected.")
        except BleakError as e:
            log.error(f"[{device_name}] Bleak error during disconnection: {e}")
            if suppress_errors: pass
            else: raise
        except Exception as e:
            log.error(f"[{device_name}] Unknown error during disconnection: {e}")
            if suppress_errors: pass
            else: raise
        finally:
             # Reset status to DISCONNECTED unless it's FAILED_PERMANENTLY
             # Only reset if current status is not FAILED
             if self.device_states.get(device_name) != DeviceStatus.FAILED_PERMANENTLY:
                 self.device_states[device_name] = DeviceStatus.DISCONNECTED
             # Clean up related resources
             self.device_clients[device_name] = None
             self.device_notify_chars[device_name] = None # Clean up char object
             # Handler was popped above

    async def _cleanup_client_resource(self, client: BleakClient):
        """ Clean up client's backend resources on unclean connect/disconnect. """
        if client and hasattr(client, "_backend") and client._backend and hasattr(client._backend, "disconnect"):
             try:
                 # This technique might change between Bleak versions
                 await client._backend.disconnect()
                 log.debug(f"Cleaned up underlying client backend resource for {client.address}.")
             except Exception as e:
                 log.warning(f"Exception during internal client resource cleanup: {e}")

    def _start_monitoring_task(self, device_name, client):
        """Start background task to monitor connection."""
        if device_name in self.monitor_tasks and not self.monitor_tasks[device_name].done():
            return
        log.debug(f"[{device_name}] Starting connection monitoring task.")
        task = self.loop.create_task(self._connection_monitor(device_name, client))
        self.monitor_tasks[device_name] = task

    async def _connection_monitor(self, device_name, client):
        """Task to periodically check client.is_connected."""
        # Run monitoring when device is in one of the connected states
        while self._running and self.device_states.get(device_name) in [DeviceStatus.CONNECTED_NO_NOTIFY, DeviceStatus.CONNECTED_NOTIFYING]:
            await asyncio.sleep(2.0)
            try:
                # Check is_connected safely
                is_connected = client and client.is_connected
            except BleakError: # Error might occur if the client has an underlying issue
                is_connected = False
                log.warning(f"[{device_name}] BleakError while checking is_connected.")

            if not is_connected:
                log.warning(f"[{device_name}] Disconnection detected!")
                current_state = self.device_states.get(device_name)
                # Only mark as failure if in a connected state (avoid recalling if disconnecting)
                if current_state in [DeviceStatus.CONNECTED_NO_NOTIFY, DeviceStatus.CONNECTED_NOTIFYING]:
                     # No need to disconnect client as it's already disconnected
                     # Just mark as failure to retry (if not max retries)
                     await self._mark_connection_failure(device_name, None) # Pass None as client is already disconnected
                break # End monitoring task
        log.debug(f"[{device_name}] Monitoring task finished.")
        # Clean up task from dictionary when it finishes
        task = self.monitor_tasks.pop(device_name, None)
        # Cancel task if it's not done (rare case)
        if task and not task.done():
             task.cancel()

    async def _connection_manager(self):
        """Phase 1: Main loop to manage device connections until ALL target devices connect."""
        log.info("--- Starting PHASE 1: Connection Manager ---")
        self.current_phase = ReaderPhase.PHASE_1_CONNECTING
        await asyncio.sleep(1.0) # Wait a bit

        target_devices_in_dict = {name for name in self.target_device_order if name in self.devices_dict}
        num_targets_to_connect = len(target_devices_in_dict)
        if num_targets_to_connect != len(self.target_device_order):
             log.warning(f"Manager targeting {num_targets_to_connect} devices from the target list found in devices_dict.")

        while self._running and not self.all_connected_event.is_set():
            processed_device_this_cycle = False
            num_connected_no_notify = 0
            num_failed_permanently = 0
            num_not_connected = 0 # Count devices not connected or waiting for retry

            # Check status mainly for the target devices needed for sync
            devices_to_check = list(self.devices_dict.keys()) # Check all devices given

            for device_name in devices_to_check:
                current_state = self.device_states.get(device_name)

                if current_state == DeviceStatus.CONNECTED_NO_NOTIFY:
                    if device_name in target_devices_in_dict:
                         num_connected_no_notify += 1
                elif current_state == DeviceStatus.FAILED_PERMANENTLY:
                    if device_name in target_devices_in_dict:
                         num_failed_permanently += 1
                elif current_state in [DeviceStatus.CONNECTING, DeviceStatus.DISCONNECTING]:
                    processed_device_this_cycle = True # Being processed, do nothing more
                elif current_state in [DeviceStatus.DISCONNECTED, DeviceStatus.WAITING_RETRY]:
                    num_not_connected += 1
                    # Only attempt to connect ONE device per cycle of this loop
                    if not processed_device_this_cycle:
                        log.info(f"[{device_name}][Phase 1] Status: {current_state}. Attempting connection.")
                        address = self.devices_dict[device_name]
                        await self._attempt_connection(device_name, address)
                        processed_device_this_cycle = True # Processed one device
                        log.debug(f"Pausing {self.connection_interval} seconds before checking/connecting next device...")
                        await asyncio.sleep(self.connection_interval)

            # --- Check Phase 1 Completion ---
            # Phase 1 completes ONLY when all devices *required for sync* are connected
            if num_connected_no_notify == num_targets_to_connect:
                log.info(f"--- PHASE 1 COMPLETE: All {num_targets_to_connect} target devices connected! ---")
                self.all_connected_event.set() # Trigger phase transition event
                break # Exit Phase 1 loop

            # --- Check for Deadlock or Permanent Failure (for target devices) ---
            if not processed_device_this_cycle and num_not_connected == 0 and num_failed_permanently > 0:
                 log.error(f"Connection Manager deadlocked: {num_failed_permanently} target devices failed permanently, {num_connected_no_notify} connected. Cannot complete Phase 1 for synchronization.")
                 self._running = False # Stop the entire process
                 break

            # If no device was processed this cycle and not all targets are connected, wait
            if not processed_device_this_cycle and self._running and not self.all_connected_event.is_set():
                 log.debug(f"[Phase 1] Waiting {self.retry_interval} seconds before rechecking device statuses...")
                 await asyncio.sleep(self.retry_interval)

        if self.all_connected_event.is_set():
             log.info("Connection Manager completed Phase 1 requirements.")
        else:
             log.warning("Connection Manager stopped before all target devices were connected.")

    async def _notification_enabler(self):
        """ Wait for Phase 1 completion, then enable notify for all successfully connected devices. """
        try:
            log.debug("Notification Enabler waiting for Phase 1 completion signal...")
            await self.all_connected_event.wait() # Wait for signal from Phase 1
            if not self._running:
                log.info("Notification Enabler stopped before starting Phase 2.")
                return # Stopped before starting

            log.info("--- Starting PHASE 2: Enabling Notifications ---")
            self.current_phase = ReaderPhase.PHASE_2_STARTING_NOTIFICATIONS

            tasks = []
            # Get the list of devices that are actually connected (not just targets)
            valid_device_names = [
                name for name, state in self.device_states.items()
                if state == DeviceStatus.CONNECTED_NO_NOTIFY
            ]

            if not valid_device_names:
                 log.warning("[Phase 2] No devices in CONNECTED_NO_NOTIFY state to enable notify for.")
                 pass # Allow proceeding, data_processor will wait if needed

            # Create a list of device names for which notify will actually be attempted
            notified_device_names = []
            for device_name in valid_device_names:
                 # Check if notify char was found for this device during Phase 1
                 if self.device_notify_chars.get(device_name):
                      tasks.append(self._start_notify_for_device(device_name))
                      notified_device_names.append(device_name) # Add to list if task is created
                 else:
                      log.warning(f"[{device_name}][Phase 2] Skipping notify enable, characteristic was not found in Phase 1.")
                      self.device_states[device_name] = DeviceStatus.FAILED_PERMANENTLY


            if not tasks:
                log.error("[Phase 2] No devices eligible for notification enabling.")
                # Decide if we should stop completely or just log
                # If sync is critical, stopping might be appropriate
                log.warning("[Phase 2] Cannot proceed to data reading phase without any notifying devices.")
                self.current_phase = ReaderPhase.PHASE_1_CONNECTING # Revert phase? Or set to STOPPED?
                # Consider stopping if no tasks could be started
                # if self._running: await self.stop()
                return

            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Check results and count successes
            successful_notifications = 0
            failed_devices = []
            # REMOVED: task_device_map = {task.get_coro().__name__ + str(id(task)): name for task, name in zip(tasks, valid_device_names) if self.device_notify_chars.get(name)} # Heuristic mapping

            # Use the index and the notified_device_names list which matches the order of tasks/results
            for i, result in enumerate(results):
                 # Ensure index is valid for notified_device_names
                 if i < len(notified_device_names):
                     device_name = notified_device_names[i] # Get device name based on order
                     if isinstance(result, Exception):
                          log.error(f"[{device_name}][Phase 2] Exception while enabling notify: {result}")
                          failed_devices.append(device_name)
                          # Update state only if it hasn't been updated already (e.g., by monitor)
                          if self.device_states.get(device_name) != DeviceStatus.FAILED_PERMANENTLY:
                              client = self.device_clients.get(device_name)
                              if client: await self._disconnect_client(client, device_name, suppress_errors=True)
                              self.device_states[device_name] = DeviceStatus.FAILED_PERMANENTLY
                     elif result is True:
                          successful_notifications += 1
                     else: # result is False
                          log.error(f"[{device_name}][Phase 2] Enable notify failed (function returned False).")
                          failed_devices.append(device_name)
                          # State should have been set to FAILED_PERMANENTLY inside _start_notify_for_device
                 else:
                     log.error(f"[Phase 2] Result index {i} out of bounds for notified_device_names (length {len(notified_device_names)}). Result: {result}")


            log.info(f"--- PHASE 2: Notification Enable Results ---")
            log.info(f"Successfully enabled: {successful_notifications}/{len(tasks)}")
            if failed_devices:
                 log.warning(f"Failed for devices: {failed_devices}")

            # Transition to reading phase regardless of partial success,
            # data_processor will handle waiting for required devices.
            log.info(f"--- Switching to PHASE 2: Reading Data (Synchronized Mode Check) ---")
            self.current_phase = ReaderPhase.PHASE_2_READING_DATA

        except asyncio.CancelledError:
            log.info("Notification Enabler cancelled.")
        except Exception as e:
            log.exception(f"Unexpected error in Notification Enabler: {e}")
            if self._running:
                 await self.stop()

    async def _start_notify_for_device(self, device_name):
        """Enable notify for a specific device."""
        client = self.device_clients.get(device_name)
        notify_char = self.device_notify_chars.get(device_name)

        if not client or not client.is_connected:
            log.error(f"[{device_name}][Phase 2] Client does not exist or is not connected when trying to enable notify.")
            # Cập nhật trạng thái nếu cần thiết
            if self.device_states.get(device_name) == DeviceStatus.CONNECTED_NO_NOTIFY:
                 self.device_states[device_name] = DeviceStatus.DISCONNECTED # Hoặc FAILED
            return False
        if not notify_char:
             log.error(f"[{device_name}][Phase 2] Characteristic info not found to enable notify.")
             self.device_states[device_name] = DeviceStatus.FAILED_PERMANENTLY # Lỗi cấu hình
             return False

        log.info(f"[{device_name}][Phase 2] Enabling notify for UUID: {notify_char.uuid}...")
        try:
            # Tạo handler cụ thể cho device này, đảm bảo không ghi đè handler cũ nếu gọi lại
            if device_name not in self.notification_handlers:
                 # Ensure the lambda captures the current device_name correctly
                 # Pass self._handle_notification directly
                 handler = lambda sender, data, name=device_name: self.loop.create_task(self._handle_notification(name, sender, data))
                 self.notification_handlers[device_name] = handler
            else:
                 handler = self.notification_handlers[device_name]

            await client.start_notify(notify_char, handler) # Dùng char object
            self.device_states[device_name] = DeviceStatus.CONNECTED_NOTIFYING # Cập nhật trạng thái
            log.info(f"[{device_name}][Phase 2] Notify enabled successfully.")
            return True
        except BleakError as e:
            log.error(f"[{device_name}][Phase 2] Bleak error during start_notify: {e}")
        except Exception as e:
            log.error(f"[{device_name}][Phase 2] Unknown error during start_notify: {e}")

        # If an error occurred
        log.warning(f"[{device_name}][Phase 2] Marking failure and disconnecting due to start_notify error.")
        await self._disconnect_client(client, device_name, suppress_errors=True) # Ngắt kết nối luôn
        # Trạng thái sẽ được đặt thành DISCONNECTED hoặc FAILED trong disconnect
        self.device_states[device_name] = DeviceStatus.FAILED_PERMANENTLY # Đặt thành FAILED ở đây để rõ ràng hơn
        return False


    # --- Data Processing Loop (Phase 2 - MODIFIED FOR SYNCHRONIZATION & CSV) ---

    def _parse_data_string(self, data_string):
        """Helper to parse the string format stored in the queue."""
        try:
            # Example format: "[DATA] IMU6 1713866469.962345: 0.1 0.2 ... 0.6" (Now 6 values)
            parts = data_string.split(":", 1)
            header_parts = parts[0].split(" ")
            timestamp_str = header_parts[-1]
            values_str = parts[1].strip()
            timestamp = float(timestamp_str)
            values = [float(v) for v in values_str.split()]
            # ***** MODIFIED: Check for 6 values *****
            if len(values) == 6:
                return timestamp, values
            else:
                # ***** MODIFIED: Update log message for expected number of values *****
                log.warning(f"Parsed incorrect number of values ({len(values)}), expected 6, from: {data_string}")
                return None, None
        except Exception as e:
            log.error(f"Error parsing data string '{data_string}': {e}")
            return None, None

    async def _data_processor(self):
        """
        Phase 2: Loop to wait for data in single target queue (IMU6)
                 and process data, writing to CSV.
        """
        try:
            log.info("Data Processor waiting for Phase 2 to start...")
            await self.all_connected_event.wait() # Wait for Phase 1 completion

            # Wait until phase is actually READING_DATA
            while self._running and self.current_phase != ReaderPhase.PHASE_2_READING_DATA:
                 if not self._running or self.current_phase in [ReaderPhase.STOPPING, ReaderPhase.STOPPED]:
                      log.info("Data Processor stopping wait because Reader is stopping.")
                      return
                 if self.current_phase == ReaderPhase.PHASE_2_STARTING_NOTIFICATIONS:
                      log.debug("Data Processor waiting for Notification Enabler to complete...")
                 else:
                      log.warning(f"Data Processor waiting but current Phase is {self.current_phase}. Possible prior error.")
                      if self.current_phase not in [ReaderPhase.PHASE_2_STARTING_NOTIFICATIONS, ReaderPhase.PHASE_2_READING_DATA]:
                           log.error("Data Processor cannot enter reading phase. Stopping.")
                           return
                 await asyncio.sleep(0.5)

            if not self._running:
                log.info("Data Processor stopped before entering main loop.")
                return

            # --- Open CSV File ---
            if not self._open_csv_file():
                 log.error("Failed to open CSV file. Stopping Data Processor.")
                 return # Stop if CSV cannot be opened

            log.info(f"--- PHASE 2: Starting Data Processor (Single Device Mode for {self.target_device_order[0]}) ---")
            log.info(f"Writing data to: {self.csv_filename}")

            # Get the single target device
            single_target = self.target_device_order[0]  # Now it's just IMU6
            
            # ***** MODIFIED: Expected number of values *****
            expected_values_per_imu = 6
            
            while self._running and self.current_phase == ReaderPhase.PHASE_2_READING_DATA:
                # --- Check if the target device is currently notifying ---
                if self.device_states.get(single_target) != DeviceStatus.CONNECTED_NOTIFYING:
                    log.warning(f"Waiting for target device {single_target} to be CONNECTED_NOTIFYING (current: {self.device_states.get(single_target)}).")
                    
                    # Optional: Check if recovery is possible
                    can_recover = self.device_states.get(single_target) in [
                        DeviceStatus.WAITING_RETRY, DeviceStatus.CONNECTING, DeviceStatus.CONNECTED_NO_NOTIFY
                    ]
                    
                    if not can_recover and self._running:
                        log.error(f"Target device {single_target} is not notifying and recovery seems impossible. Stopping Data Processor.")
                        break  # Exit processor loop
                    
                    await asyncio.sleep(1.0)  # Wait longer if target device is not ready
                    continue

                # --- Check if the queue has data ---
                q = self.device_queues.get(single_target)
                
                # Check state again to prevent race condition
                if self.device_states.get(single_target) != DeviceStatus.CONNECTED_NOTIFYING:
                    log.warning(f"Target device {single_target} stopped notifying while checking queue.")
                    await asyncio.sleep(0.5)
                    continue
                
                if not q or q.empty():
                    await asyncio.sleep(0.01)  # Sleep briefly and check again
                    continue

                # --- Queue has data, retrieve it ---
                try:
                    data_string = await q.get()
                    q.task_done()
                    
                    # Parse data for CSV
                    timestamp, values = self._parse_data_string(data_string)
                    
                    if timestamp is not None and values is not None:
                        # Create row for CSV with timestamp and all values
                        csv_row = [f"{timestamp:.6f}"]
                        csv_row.extend(f"{v:.6f}" for v in values)
                        
                        # Log received data
                        log.info(f"--- Data Received from {single_target} ---")
                        log.info(f"  {data_string}")
                        log.info("--------------------------------------")
                        
                        # Write to CSV
                        if self.csv_writer and self.csv_file:
                            self.csv_writer.writerow(csv_row)
                            self.csv_file.flush()  # Ensure data is written to disk
                            os.fsync(self.csv_file.fileno())  # Force write to disk
                        else:
                            log.warning("CSV writer not available, cannot write row.")
                    else:
                        log.warning("Could not parse data, skipping CSV write.")

                except asyncio.CancelledError:
                    log.info("Data retrieval cancelled during processing.")
                    raise  # Re-raise cancellation
                except Exception as e:
                    log.exception(f"Error during data retrieval/processing: {e}")

            log.info("Data Processor has stopped its main processing loop.")

        except asyncio.CancelledError:
            log.info("Data Processor task cancelled.")
        except Exception as e:
            log.exception(f"Unexpected error in Data Processor: {e}")
            # Ensure CSV is closed on error
            self._close_csv_file()
            if self._running:
                await self.stop() # Stop if there is a critical error
        finally:
            # Ensure CSV is closed when processor stops normally or is cancelled
            self._close_csv_file()

    # --- REMOVED _process_queue method as it's no longer used ---

    # --- CSV Helper Methods ---
    def _open_csv_file(self):
        """Opens the CSV file for writing and writes the header."""
        try:
            # Open file in write mode, creating it if it doesn't exist.
            # newline='' prevents extra blank rows in Windows.
            self.csv_file = open(self.csv_filename, 'w', newline='')
            self.csv_writer = csv.writer(self.csv_file)

            # Create header row - simplified for single IMU
            header = ["Timestamp"] 
            # Headers for the 6 values from IMU6
            for i in range(1, 7):
                header.append(f"IMU6_Val{i}")  # Or more specific: AccX, AccY, AccZ, GyroX, GyroY, GyroZ

            self.csv_writer.writerow(header)
            log.info(f"Opened CSV file '{self.csv_filename}' and wrote header.")
            return True
        except IOError as e:
            log.error(f"Failed to open or write header to CSV file '{self.csv_filename}': {e}")
            self.csv_file = None
            self.csv_writer = None
            return False

    def _close_csv_file(self):
        """Closes the CSV file if it's open."""
        if self.csv_file:
            try:
                self.csv_file.close()
                log.info(f"Closed CSV file '{self.csv_filename}'.")
            except IOError as e:
                log.error(f"Error closing CSV file '{self.csv_filename}': {e}")
            finally:
                self.csv_file = None
                self.csv_writer = None
        # else:
        #     log.debug("CSV file was not open, no need to close.")


    # --- Run/Stop Control ---
    # [ ... run method remains unchanged ... ]
    # [ ... stop method remains unchanged ... ]
    # [ ... get_device_status, get_all_statuses methods remain unchanged ... ]
    # [ ... start_reading, stop_reading methods remain unchanged ... ]
    async def run(self):
        """Run the tasks according to phases."""
        if self._running:
            log.warning("Already running.")
            return

        log.info(f"--- Starting 2-Phase data reading process from {len(self.devices_dict)} devices ---") # Log total devices configured
        log.info(f"Target devices for sync: {self.target_device_order}")
        log.info(f"Full device config: {self.devices_dict}")
        # ***** ADDED: Log expected data format *****
        log.info("Expected data format: 6 floats (Accel X, Y, Z, Gyro X, Y, Z) per notification (24 bytes).")


        self._running = True
        self.current_phase = ReaderPhase.INITIALIZING
        self.all_connected_event.clear() # Ensure event is reset

        # Reset state and resources
        self.device_states = {name: DeviceStatus.DISCONNECTED for name in self.device_names}
        self.device_clients = {name: None for name in self.device_names}
        self.device_retries = {name: 0 for name in self.device_names}
        self.device_notify_chars = {name: None for name in self.device_names}
        self.notification_handlers = {}
        # Cancel old monitoring tasks if any
        for task in self.monitor_tasks.values():
             if task and not task.done(): task.cancel()
        self.monitor_tasks = {}


        # Create main tasks
        manager_task = self.loop.create_task(self._connection_manager())
        enabler_task = self.loop.create_task(self._notification_enabler())
        processor_task = self.loop.create_task(self._data_processor())
        # Store tasks for later cancellation
        self._main_tasks = [manager_task, enabler_task, processor_task]

        try:
            # Use asyncio.gather to wait for ALL main tasks.
            log.info("Program running, waiting for main tasks (manager, enabler, processor)...")
            await asyncio.gather(*self._main_tasks)
            log.info("All main tasks completed naturally (unexpected).")

        except asyncio.CancelledError:
             log.info("Main tasks cancelled (e.g., by stop() or KeyboardInterrupt).")
        except Exception as e:
             log.exception(f"Unexpected error during main task execution: {e}")
        finally:
            log.info("Exiting main execution block, ensuring cleanup...")
            # Ensure CSV is closed if run finishes unexpectedly before stop is called
            self._close_csv_file()
            if self.current_phase != ReaderPhase.STOPPING and self.current_phase != ReaderPhase.STOPPED:
                 log.info("Calling self.stop() from run() finally block.")
                 await self.stop() # stop() will also call _close_csv_file
            else:
                 log.info("Stopping process already initiated or completed previously.")

    async def stop(self):
        """Stop all tasks and disconnect safely."""
        if self.current_phase == ReaderPhase.STOPPING or self.current_phase == ReaderPhase.STOPPED:
            log.debug("Stop request ignored because already stopping or stopped.")
            return # Already stopping or stopped

        log.info("Stopping BLUETOOTH_DATA_READER...")
        previous_phase = self.current_phase
        self.current_phase = ReaderPhase.STOPPING
        self._running = False # Stop signal for loops
        self.all_connected_event.set() # Ensure waiting tasks are not stuck

        # Cancel main tasks (reverse order might be better?)
        log.debug("Cancelling main tasks (processor, enabler, manager)...")
        # Cancel processor first so it stops reading queue
        processor_task = next((t for t in self._main_tasks if hasattr(t.get_coro(), '__name__') and t.get_coro().__name__ == '_data_processor'), None)
        if processor_task and not processor_task.done(): processor_task.cancel()

        enabler_task = next((t for t in self._main_tasks if hasattr(t.get_coro(), '__name__') and t.get_coro().__name__ == '_notification_enabler'), None)
        if enabler_task and not enabler_task.done(): enabler_task.cancel()

        manager_task = next((t for t in self._main_tasks if hasattr(t.get_coro(), '__name__') and t.get_coro().__name__ == '_connection_manager'), None)
        if manager_task and not manager_task.done(): manager_task.cancel()


        # Wait for all main tasks to finish after cancellation
        try:
            await asyncio.gather(*self._main_tasks, return_exceptions=True)
        except asyncio.CancelledError:
            log.debug("Main tasks gather cancelled.") # Expected during stop
        log.debug("Main tasks stopped.")

        # Cancel monitoring tasks
        log.debug("Cancelling monitoring tasks...")
        monitor_tasks_to_wait = []
        # Use list(items()) to avoid RuntimeError: dictionary changed size during iteration
        for name, task in list(self.monitor_tasks.items()):
             if task and not task.done():
                  task.cancel()
                  monitor_tasks_to_wait.append(task)
             # Remove from dict immediately
             if name in self.monitor_tasks:
                 del self.monitor_tasks[name]

        if monitor_tasks_to_wait:
            try:
                 await asyncio.gather(*monitor_tasks_to_wait, return_exceptions=True)
            except asyncio.CancelledError:
                 log.debug("Monitor tasks gather cancelled.") # Expected during stop
        log.debug("Monitoring tasks stopped.")

        # Disconnect clients
        log.debug("Disconnecting clients...")
        disconnect_tasks = []
        # Create a copy of the client dict to avoid size change errors during iteration
        clients_to_disconnect = list(self.device_clients.items())
        for name, client in clients_to_disconnect:
             if client: # Only attempt disconnection if there is a client object
                # Call _disconnect_client, this function will handle stopping notify if needed
                disconnect_tasks.append(self._disconnect_client(client, name, suppress_errors=True))
        if disconnect_tasks:
            results = await asyncio.gather(*disconnect_tasks, return_exceptions=True)
            # Log disconnect results if needed
            for i, res in enumerate(results):
                 # Use index to get name from the original list copy
                 if i < len(clients_to_disconnect):
                     dev_name = clients_to_disconnect[i][0]
                     if isinstance(res, Exception):
                          log.error(f"Error disconnecting {dev_name} during stop: {res}")
                 else:
                      log.error(f"Result index {i} out of bounds during disconnect gather") # Should not happen
        log.info("Client disconnection process completed.")

        # Close CSV file at the very end of stopping
        self._close_csv_file()

        self._main_tasks = []
        self.current_phase = ReaderPhase.STOPPED
        log.info("BLUETOOTH_DATA_READER stopped completely.")

    def get_device_status(self, device_name):
        return self.device_states.get(device_name, None)

    def get_all_statuses(self):
        return self.device_states.copy()

    def start_reading(self):
        """Synchronous method to start the reading process."""
        if self._running:
            log.warning("Reader already running.")
            return
        try:
            log.info("Calling run() from start_reading()...")
            # Ensure running on the reader object's current loop
            self.loop.run_until_complete(self.run())
        except KeyboardInterrupt:
            log.info("KeyboardInterrupt detected in start_reading. Stopping...")
            # stop() will be called in the finally block of run()
        except Exception as e:
             log.exception(f"Unknown error in start_reading: {e}")
             # Attempt clean stop if reader is running
             if self._running and self.current_phase != ReaderPhase.STOPPING and self.current_phase != ReaderPhase.STOPPED:
                  log.info("Attempting to stop reader due to error in start_reading...")
                  # Check if loop is running before calling run_until_complete
                  if self.loop.is_running():
                      # Schedule stop and wait for it
                      stop_future = asyncio.run_coroutine_threadsafe(self.stop(), self.loop)
                      stop_future.result() # Wait for stop to complete
                  else:
                      log.warning("Loop not running, cannot run stop_until_complete.")


    def stop_reading(self):
         """Synchronous method to stop the reading process."""
         if not self._running and self.current_phase != ReaderPhase.STOPPING:
              log.warning("Reader not running or already stopping.")
              return
         if self.current_phase == ReaderPhase.STOPPING or self.current_phase == ReaderPhase.STOPPED:
              log.info("Reader is stopping or already stopped.")
              return

         log.info("Stop requested from stop_reading()...")
         # Schedule stop() in event loop if it's running and not stopped
         if self.loop.is_running() and self._running:
              # Use run_coroutine_threadsafe if called from different thread,
              # or just create_task if called from the loop's thread.
              # Assuming potential call from another thread for safety:
              stop_future = asyncio.run_coroutine_threadsafe(self.stop(), self.loop)
              try:
                  # Wait for the stop() coroutine to finish
                  stop_future.result(timeout=30) # Add a timeout
                  log.info("self.stop() completed after being scheduled.")
              except TimeoutError:
                  log.error("Timeout waiting for self.stop() to complete.")
              except Exception as e:
                  log.error(f"Error waiting for self.stop() future: {e}")

         elif not self.loop.is_running():
              log.warning("Event loop not running, cannot schedule stop async.")
         else: # Loop running but self._running is False (stop likely already in progress)
             log.info("Reader is not marked as running, stop likely already initiated.")


# --- Utility Code (BLUETOOTH_DETECTOR, DISCONNECT_DEVICE, main) ---
# [ ... BLUETOOTH_DETECTOR class remains unchanged ... ]
# [ ... DISCONNECT_DEVICE function remains unchanged ... ]
class BLUETOOTH_DETECTOR:
    # ... (Keep original code of BLUETOOTH_DETECTOR, translating comments/logs) ...
    def __init__(self, needy_devices):
        self.devices = {}
        # Fix: Initialize loop here if not already initialized externally
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
        self.aim_devices = needy_devices

    async def OUTSYSTEM_SCANNING(self):
        log.info("OUTSYSTEM_SCANNING: Scanning for Bluetooth devices...")
        found_devices = {}
        try:
             # Increase timeout if necessary
             bleak_devices = await BleakScanner.discover(timeout=10.0)
             for device in bleak_devices:
                  # Check if name exists and matches one of the target devices
                  if device.name and device.name in self.aim_devices:
                       # Only take the first one found per name
                       if device.name not in found_devices:
                           log.info(f"[OUTSYSTEM_SCANNING]: Found target device {device.name} at {device.address}")
                           found_devices[device.name] = device.address
                       # else: log.debug(f"[OUTSYSTEM_SCANNING] Found duplicate {device.name}, keeping first found.")
                  # elif device.name:
                  #      log.debug(f"[OUTSYSTEM_SCANNING] Found non-target device: {device.name} ({device.address})")
                  # elif device.address:
                  #      log.debug(f"[OUTSYSTEM_SCANNING] Found unnamed device: {device.address}")

        except BleakError as e:
             log.error(f"[OUTSYSTEM_SCANNING] BleakError during scan: {e}")
        except Exception as e:
             log.exception(f"[OUTSYSTEM_SCANNING] Unexpected error during scan: {e}")

        # Ensure all target devices were found
        missing_devices = [name for name in self.aim_devices if name not in found_devices]
        if missing_devices:
            log.warning(f"[OUTSYSTEM_SCANNING] Did not find the following target devices: {missing_devices}")

        return found_devices


    async def INSYSTEM_SCANNING(self):
        log.info("INSYSTEM_SCANNING: Checking connected devices via bluetoothctl...")
        connected_target_devices = {}
        try:
            proc = await asyncio.create_subprocess_exec(
                'bluetoothctl', 'devices', 'Connected',
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate(timeout=15) # Add timeout
            devices_output = stdout.decode('utf-8', errors='ignore')
            stderr_output = stderr.decode('utf-8', errors='ignore')

            # Ignore non-critical errors like "not available"
            if stderr_output and "not available" not in stderr_output :
                log.warning(f"[INSYSTEM_SCANNING] bluetoothctl stderr: {stderr_output.strip()}")

            for line in devices_output.splitlines():
                 # Example line: "Device 58:3B:C2:56:8E:07 IMU2"
                 if "Device" in line:
                      # Split into max 3 parts: "Device", MAC, Name/Info
                      parts = line.strip().split(" ", 2)
                      if len(parts) >= 3:
                           mac_address = parts[1].strip()
                           # Device name usually here
                           device_info = parts[2].strip()
                           # Find target name within device info
                           name = None
                           for target_name in self.aim_devices:
                               # Check if the exact target name is present in the reported info
                               if target_name == device_info or target_name in device_info.split():
                                   name = target_name
                                   break

                           # If a matching target name is found
                           if name:
                                if name not in connected_target_devices: # Avoid duplicate disconnect attempts per run
                                    log.info(f"[INSYSTEM_SCANNING]: Found connected target device: {name} ({mac_address}). Attempting disconnect.")
                                    connected_target_devices[name] = mac_address
                                    # Call disconnect function asynchronously
                                    await DISCONNECT_DEVICE(mac_address)
                                # else: log.debug(f"[INSYSTEM_SCANNING] Already processed disconnect for {name} ({mac_address}) in this scan.")
                           # else:
                           #      log.debug(f"[INSYSTEM_SCANNING] Found connected device, but not a target: {device_info} ({mac_address})")

        except FileNotFoundError:
             log.error("[INSYSTEM_SCANNING]: 'bluetoothctl' command not found. Skipping check.")
        except asyncio.TimeoutError:
             log.error("[INSYSTEM_SCANNING]: Timeout waiting for 'bluetoothctl' command.")
        except Exception as e:
             log.exception(f"[INSYSTEM_SCANNING]: Error during connected devices check: {e}")
        return connected_target_devices

async def DISCONNECT_DEVICE(address):
     # ... (Keep original code of DISCONNECT_DEVICE, translating comments/logs) ...
     log.info(f"[DISCONNECT_DEVICE]: Attempting bluetoothctl disconnect for {address}...")
     try:
         proc = await asyncio.create_subprocess_exec(
             'bluetoothctl', 'disconnect', address,
             stdout=asyncio.subprocess.PIPE,
             stderr=asyncio.subprocess.PIPE
         )
         # Increased timeout slightly
         stdout, stderr = await proc.communicate(timeout=10)

         stdout_str = stdout.decode('utf-8', errors='ignore').strip()
         stderr_str = stderr.decode('utf-8', errors='ignore').strip()

         # Check both stdout and stderr to determine success more robustly
         # Success messages: "Successful disconnected", "Device has been disconnected"
         # Failure/Not connected messages: "Device not connected", "Failed to disconnect"
         success = False
         if proc.returncode == 0 and ("Successful disconnected" in stdout_str or "Device has been disconnected" in stdout_str):
             success = True
         elif "Device not connected" in stderr_str:
             success = True # Already disconnected is considered success for our purpose
             log.info(f"[DISCONNECT_DEVICE]: Device {address} was already not connected (reported via stderr).")
         # Sometimes success message is in stderr
         elif "Successful disconnected" in stderr_str or "Device has been disconnected" in stderr_str:
              success = True

         if success:
             log.info(f"[DISCONNECT_DEVICE]: Successfully disconnected or confirmed not connected: {address}")
             return True
         else:
             log.warning(f"[DISCONNECT_DEVICE]: Failed disconnect via bluetoothctl for {address}. Return code: {proc.returncode}")
             if stdout_str: log.warning(f"[DISCONNECT_DEVICE]: Output: {stdout_str}")
             if stderr_str: log.warning(f"[DISCONNECT_DEVICE]: Error: {stderr_str}")
             return False
     except FileNotFoundError:
         log.error("[DISCONNECT_DEVICE]: 'bluetoothctl' command not found. Cannot disconnect via system tool.")
         return False
     except asyncio.TimeoutError:
         log.error(f"[DISCONNECT_DEVICE]: Timeout waiting for 'bluetoothctl disconnect {address}'.")
         return False
     except Exception as e:
         log.exception(f"[DISCONNECT_DEVICE]: Unexpected error disconnecting {address}: {e}")
         return False

# --- Main execution ---
if __name__ == "__main__":
    log.info("--- Starting Bluetooth IMU Data Collector (Two-Phase, 6-Value IMU, Single Device Mode) ---")

    # Modified: Now we only target one device - IMU6
    target_devices_names = ["IMU6"]
    log.info(f"Target device for reading: {target_devices_names}")
    csv_output_filename = "imu6_data_6val.csv"  # Updated filename for single IMU6

    detector = BLUETOOTH_DETECTOR(target_devices_names)
    found_devices_map = {}
    reader = None

    # --- Run Event Loop ---
    main_loop = None
    try:
        main_loop = asyncio.get_running_loop()
    except RuntimeError:
        main_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(main_loop)

    async def main():
        global reader # Allow modification of the global reader variable
        run_reader = False
        found_devices_map = {}
        try:
            # 1. Disconnect old connections (INSYSTEM)
            log.info("--- Preliminary Step: Checking and Disconnecting Existing Connections ---")
            await detector.INSYSTEM_SCANNING()
            log.info("Finished checking existing connections. Waiting briefly...")
            await asyncio.sleep(3.0)

            # 2. Scan for devices (OUTSYSTEM)
            log.info("--- Preliminary Step: Scanning for Target Device ---")
            found_devices_map = await detector.OUTSYSTEM_SCANNING()

            # 3. Initialize Data Reader if the target device was found
            if "IMU6" in found_devices_map:
                log.info(f"--- Found target device IMU6: {found_devices_map['IMU6']} ---")
                log.info("--- Initializing Two-Phase Data Reader ---")
                reader = BLUETOOTH_DATA_READER(
                    found_devices_map,
                    connection_interval_sec=6.0,
                    retry_interval_sec=12.0,
                    max_connect_retries=5,
                    bleak_timeout=25.0,
                    csv_filename=csv_output_filename # Pass filename to reader
                )
                run_reader = True
            else:
                log.warning(f"--- Could not find IMU6. Found devices: {list(found_devices_map.keys())}. Cannot start reading. ---")

            # 4. Run Reader if initialized
            if run_reader and reader:
                 log.info("--- Starting Data Reader Execution (calling run) ---")
                 # Directly await run() here since we are in an async main function
                 await reader.run()
                 log.info("--- Data Reader Execution Finished (run returned) ---")
            else:
                 log.info("--- Reader will not start as IMU6 was not found or initialized. ---")

        except KeyboardInterrupt:
            log.info("\n--- KeyboardInterrupt detected in main async block. Initiating stop. ---")
            if reader and reader._running:
                await reader.stop()
        except Exception as e:
             log.exception("--- An unexpected error occurred in the main async execution block ---")
             if reader and reader._running:
                 log.error("Attempting to stop reader due to error...")
                 await reader.stop()
        finally:
            log.info("--- Main async function finished or interrupted. ---")

    # Run the main async function
    try:
        main_loop.run_until_complete(main())
    except KeyboardInterrupt:
         log.info("--- KeyboardInterrupt caught outside main async function. ---")
         # Ensure cleanup if reader was created and loop is still running
         if reader and main_loop.is_running() and (reader._running or reader.current_phase == ReaderPhase.STOPPING):
              log.info("Attempting final stop sequence...")
              main_loop.run_until_complete(reader.stop())

    finally:
        log.info("--- Starting Final Cleanup Sequence ---")
        if main_loop and not main_loop.is_closed():
             # Define an async function for cleanup steps requiring await
             async def async_cleanup(loop):
                 if loop.is_running():
                     log.warning("Main event loop is still running during final cleanup. Attempting graceful shutdown.")
                     try:
                         # Give pending tasks a moment to cancel/finish
                         tasks = [t for t in asyncio.all_tasks(loop=loop) if t is not asyncio.current_task(loop=loop)]
                         if tasks:
                              log.debug(f"Waiting briefly for {len(tasks)} remaining tasks...")
                              # Don't cancel again if stop was called, just wait
                              await asyncio.sleep(1.0) # Short wait
                              # Gather remaining tasks (they might be finishing)
                              await asyncio.gather(*tasks, return_exceptions=True)
                         await loop.shutdown_asyncgens()
                         log.info("Async generators shut down.")
                     except Exception as loop_shutdown_err:
                          log.error(f"Error during async cleanup: {loop_shutdown_err}")

             # Run the async cleanup function using the loop
             if main_loop.is_running():
                 main_loop.run_until_complete(async_cleanup(main_loop))

             # Close the loop if it's not already closed
             if not main_loop.is_closed():
                  main_loop.close()
                  log.info("Main event loop closed.")
             else:
                  log.info("Main event loop was already closed.")

        log.info("--- Program Finished ---")