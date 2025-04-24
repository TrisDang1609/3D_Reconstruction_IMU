import asyncio
from bleak import BleakScanner, BleakClient, BleakError, BleakGATTCharacteristic
import struct
import numpy as np
from collections import deque
import time
import logging
from enum import Enum

# Configure more detailed logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

# --- UUIDs ---
# IMU_SERVICE_UUID = "2412b5cbd460800c15c39ba9ac5a8ade" # Not used directly in this code
# LED_CONTROL_CHAR_UUID = "5b02651040-88c29746d8be6c736a087a" # Not used
IMU_NOTIFY_CHAR_UUID = "61a885a441c360d09a536d652a70d29c" # Main UUID for notify

# Known characteristic UUIDs for IMU devices
IMU_CHARACTERISTIC_UUIDS = [
    "61A885A4-41C3-60D0-9A53-6D652A70D29C", # Primary UUID
    IMU_NOTIFY_CHAR_UUID,
    # Add other UUIDs if needed for testing
    # "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
    # "YYYYYYYY-YYYY-YYYY-YYYY-YYYYYYYYYYYY",
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
                 bleak_timeout=20.0):
        """
        Initialize Data Reader with 2 Phases.

        Args:
            devices_dict (dict): Dictionary {device_name: mac_address}.
            connection_interval_sec (float): Wait time between connection attempts for DIFFERENT devices.
            retry_interval_sec (float): Wait time before retrying a FAILED device connection.
            max_connect_retries (int): Maximum connection retries per device.
            bleak_timeout (float): Timeout for BleakClient operations.
        """
        if not devices_dict:
            raise ValueError("devices_dict cannot be empty.")

        self.devices_dict = devices_dict
        self.device_names = list(devices_dict.keys())
        self.num_target_devices = len(self.device_names)
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

        # State and resource management
        self.device_states = {name: DeviceStatus.DISCONNECTED for name in self.device_names}
        self.device_clients = {name: None for name in self.device_names}
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
        """Handle data received from notifications."""
        queue = self.device_queues.get(device_name)
        if not queue:
            log.warning(f"[{device_name}] Queue not found when receiving notification.")
            return
        # Check if currently in the data reading phase
        if self.current_phase != ReaderPhase.PHASE_2_READING_DATA:
            # log.debug(f"[{device_name}] Received notification outside data reading phase, ignore.")
            return

        if len(data) >= 28:
            try:
                values = struct.unpack('<7f', data[:28])
                timestamp = time.time()
                output_str = f"[DATA] {device_name} {timestamp:.6f}: {' '.join(f'{v:.6f}' for v in values)}"
                await queue.put(output_str)
            except struct.error as e:
                log.warning(f"[{device_name}] Error unpacking data: {e}. Data (hex): {data.hex()}")
            except Exception as e:
                log.error(f"[{device_name}] Unknown error processing notification: {e}")
        # else:
        #     log.debug(f"[{device_name}] Notification data too short: {len(data)} bytes")

    # --- Connection (Phase 1) ---
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
                     mtu = await client.exchange_mtu(247)
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


    # --- Connection Monitoring (Active in both phases) ---
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


    # --- Main Management Loop (Phase 1) ---
    async def _connection_manager(self):
        """Phase 1: Main loop to manage device connections until ALL are connected."""
        log.info("--- Starting PHASE 1: Connection Manager ---")
        self.current_phase = ReaderPhase.PHASE_1_CONNECTING
        await asyncio.sleep(1.0) # Wait a bit

        while self._running and not self.all_connected_event.is_set():
            processed_device_this_cycle = False
            num_connected_no_notify = 0
            num_failed_permanently = 0
            num_not_connected = 0 # Count devices not connected or waiting for retry

            all_device_names = list(self.devices_dict.keys()) # Get the latest list each iteration

            for device_name in all_device_names:
                current_state = self.device_states.get(device_name)

                if current_state == DeviceStatus.CONNECTED_NO_NOTIFY:
                    num_connected_no_notify += 1
                elif current_state == DeviceStatus.FAILED_PERMANENTLY:
                    num_failed_permanently += 1
                elif current_state in [DeviceStatus.CONNECTING, DeviceStatus.DISCONNECTING]:
                    processed_device_this_cycle = True # Being processed, do nothing more
                elif current_state in [DeviceStatus.DISCONNECTED, DeviceStatus.WAITING_RETRY]:
                    num_not_connected += 1
                    # Only attempt to connect ONE device per cycle of this loop
                    # to avoid overloading the Bluetooth adapter
                    if not processed_device_this_cycle:
                        log.info(f"[{device_name}][Phase 1] Status: {current_state}. Attempting connection.")
                        address = self.devices_dict[device_name]
                        await self._attempt_connection(device_name, address)
                        processed_device_this_cycle = True # Processed one device
                        # Pause after attempting to connect one device
                        log.debug(f"Pausing {self.connection_interval} seconds before checking/connecting next device...")
                        await asyncio.sleep(self.connection_interval)

            # --- Check Phase 1 Completion ---
            # Phase 1 completes IF AND ONLY IF all devices are in CONNECTED_NO_NOTIFY state
            if num_connected_no_notify == self.num_target_devices:
                log.info("--- PHASE 1 COMPLETE: All devices connected! ---")
                self.all_connected_event.set() # Trigger phase transition event
                break # Exit Phase 1 loop

            # --- Check for Deadlock or Permanent Failure ---
            # If no devices are being processed (connecting/disconnecting)
            # AND no devices are waiting for connection (disconnected/waiting_retry)
            # AND required number of connections not reached -> There are FAILED_PERMANENTLY errors
            if not processed_device_this_cycle and num_not_connected == 0 and num_failed_permanently > 0:
                 log.error(f"Connection Manager deadlocked: {num_failed_permanently} devices failed permanently, {num_connected_no_notify} connected. Cannot complete Phase 1.")
                 self._running = False # Stop the entire process as the goal cannot be reached
                 break

            # If no device was processed this cycle (all are CONNECTED_NO_NOTIFY or FAILED),
            # and not enough connections yet, wait before looping again.
            if not processed_device_this_cycle and self._running and not self.all_connected_event.is_set():
                 log.debug(f"[Phase 1] Waiting {self.retry_interval} seconds before rechecking device statuses...")
                 await asyncio.sleep(self.retry_interval)
            # If one device was processed, the loop will run again immediately to check the next device

        if self.all_connected_event.is_set():
             log.info("Connection Manager completed Phase 1.")
        else:
             log.warning("Connection Manager stopped before all devices were connected.")


    # --- Task to Enable Notifications (Start of Phase 2) ---
    async def _notification_enabler(self):
        """ Wait for Phase 1 completion, then enable notify for all devices. """
        try:
            log.debug("Notification Enabler waiting for Phase 1 completion signal...")
            await self.all_connected_event.wait() # Wait for signal from Phase 1
            if not self._running:
                log.info("Notification Enabler stopped before starting Phase 2.")
                return # Stopped before starting

            log.info("--- Starting PHASE 2: Enabling Notifications ---")
            self.current_phase = ReaderPhase.PHASE_2_STARTING_NOTIFICATIONS

            tasks = []
            # Get the list of valid device names at this moment
            valid_device_names = [
                name for name, state in self.device_states.items()
                if state == DeviceStatus.CONNECTED_NO_NOTIFY
            ]

            if not valid_device_names:
                 log.warning("[Phase 2] No devices in CONNECTED_NO_NOTIFY state to enable notify for.")
                 # Decision: Should we move to reading phase? Currently no.
                 self.current_phase = ReaderPhase.PHASE_1_CONNECTING # Go back or stop?
                 log.error("Cannot continue Phase 2 because there are no valid devices.")
                 # Can call self.stop() here if wanting to stop completely
                 # await self.stop()
                 return


            for device_name in valid_device_names:
                 tasks.append(self._start_notify_for_device(device_name))

            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Check results and count successes
            successful_notifications = 0
            failed_devices = []
            for i, result in enumerate(results):
                 # Need to get the correct device_name corresponding to result[i]
                 device_name = valid_device_names[i]
                 if isinstance(result, Exception):
                      log.error(f"[{device_name}][Phase 2] Exception while enabling notify: {result}")
                      failed_devices.append(device_name)
                      # Error handling: Disconnect this failed device?
                      client = self.device_clients.get(device_name)
                      if client: await self._disconnect_client(client, device_name, suppress_errors=True)
                      # No need to call _mark_connection_failure as disconnect already set DISCONNECTED
                      # If retry is desired, different logic is needed
                      self.device_states[device_name] = DeviceStatus.FAILED_PERMANENTLY # Or another error state
                 elif result is True: # Function _start_notify_for_device returns True on success
                      successful_notifications += 1
                 else: # Function returned False
                      log.error(f"[{device_name}][Phase 2] Enable notify failed (function returned False).")
                      failed_devices.append(device_name)
                      # Status and disconnect were handled in _start_notify_for_device

            log.info(f"--- PHASE 2: Notification Enable Results ---")
            log.info(f"Success: {successful_notifications}/{len(valid_device_names)}")
            if failed_devices:
                 log.warning(f"Failed for devices: {failed_devices}")

            # Only switch to reading phase if at least one device succeeded
            if successful_notifications > 0:
                 log.info(f"--- Switching to PHASE 2: Reading Data (with {successful_notifications} devices) ---")
                 self.current_phase = ReaderPhase.PHASE_2_READING_DATA # Officially entering reading phase
            else:
                 log.error("--- No devices successfully enabled notify. Cannot enter data reading phase. ---")
                 # Decision: Stop the program?
                 if self._running:
                      await self.stop()

        except asyncio.CancelledError:
            log.info("Notification Enabler cancelled.")
        except Exception as e:
            log.exception(f"Unexpected error in Notification Enabler: {e}")
            if self._running:
                 await self.stop() # Stop if there is a critical error


    async def _start_notify_for_device(self, device_name):
        """Enable notify for a specific device."""
        client = self.device_clients.get(device_name)
        notify_char = self.device_notify_chars.get(device_name)

        if not client or not client.is_connected:
            log.error(f"[{device_name}][Phase 2] Client does not exist or is not connected when trying to enable notify.")
            # Update status if necessary
            if self.device_states.get(device_name) == DeviceStatus.CONNECTED_NO_NOTIFY:
                 self.device_states[device_name] = DeviceStatus.DISCONNECTED # Or FAILED
            return False
        if not notify_char:
             log.error(f"[{device_name}][Phase 2] Characteristic info not found to enable notify.")
             self.device_states[device_name] = DeviceStatus.FAILED_PERMANENTLY # Configuration error
             return False

        log.info(f"[{device_name}][Phase 2] Enabling notify for UUID: {notify_char.uuid}...")
        try:
            # Create specific handler for this device, ensure not overwriting old handler if called again
            if device_name not in self.notification_handlers:
                 handler = lambda sender, data: self.loop.create_task(self._handle_notification(device_name, sender, data))
                 self.notification_handlers[device_name] = handler
            else:
                 handler = self.notification_handlers[device_name]

            await client.start_notify(notify_char, handler) # Use char object
            self.device_states[device_name] = DeviceStatus.CONNECTED_NOTIFYING # Update status
            log.info(f"[{device_name}][Phase 2] Notify enabled successfully.")
            return True
        except BleakError as e:
            log.error(f"[{device_name}][Phase 2] Bleak error during start_notify: {e}")
        except Exception as e:
            log.error(f"[{device_name}][Phase 2] Unknown error during start_notify: {e}")

        # If an error occurred
        log.warning(f"[{device_name}][Phase 2] Marking failure and disconnecting due to start_notify error.")
        await self._disconnect_client(client, device_name, suppress_errors=True) # Disconnect immediately
        # Status will be set to DISCONNECTED or FAILED in disconnect
        # Set to FAILED here for clarity
        self.device_states[device_name] = DeviceStatus.FAILED_PERMANENTLY
        return False


    # --- Data Processing Loop (Phase 2) ---
    async def _data_processor(self):
        """Phase 2: Loop to process data from queues AFTER notifications are enabled."""
        try:
            log.info("Data Processor waiting for Phase 2 to start...")
            await self.all_connected_event.wait() # Wait for Phase 1 finish

            # Wait until the phase is actually READING_DATA
            while self._running and self.current_phase != ReaderPhase.PHASE_2_READING_DATA:
                 # Check if enabler finished and failed
                 if not self._running or self.current_phase == ReaderPhase.STOPPING or self.current_phase == ReaderPhase.STOPPED:
                      log.info("Data Processor stopped waiting because Reader is stopping.")
                      return
                 # If enabler hasn't finished or failed and didn't transition phase
                 if self.current_phase == ReaderPhase.PHASE_2_STARTING_NOTIFICATIONS:
                      log.debug("Data Processor waiting for Notification Enabler to complete...")
                 else: # Might have been reset to PHASE_1 due to error
                      log.warning(f"Data Processor waiting but current Phase is {self.current_phase}. There might have been a previous error.")
                      # Exit if unable to enter reading phase
                      if self.current_phase not in [ReaderPhase.PHASE_2_STARTING_NOTIFICATIONS, ReaderPhase.PHASE_2_READING_DATA]:
                           log.error("Data Processor cannot enter reading phase. Stopping.")
                           return

                 await asyncio.sleep(0.5) # Wait and recheck phase

            if not self._running: return # Stopped while waiting

            log.info("--- PHASE 2: Starting Data Processor ---")
            processed_data_since_start = False

            while self._running and self.current_phase == ReaderPhase.PHASE_2_READING_DATA:
                processed_data_this_cycle = False
                # Only process queues of devices actually notifying
                notifying_devices = [name for name, state in self.device_states.items()
                                     if state == DeviceStatus.CONNECTED_NOTIFYING]

                if not notifying_devices:
                     # If no devices are notifying anymore (due to later disconnection)
                     log.warning("No devices are notifying anymore. Data Processor waiting...")
                     # Check if any devices can potentially reconnect
                     can_recover = any(state in [DeviceStatus.WAITING_RETRY, DeviceStatus.CONNECTING, DeviceStatus.CONNECTED_NO_NOTIFY]
                                       for state in self.device_states.values())
                     if not can_recover and self._running:
                          log.error("All devices have lost connection and cannot recover. Stopping Data Processor.")
                          # Consider stopping the entire reader here if needed
                          # await self.stop()
                          break # Exit processor loop
                     await asyncio.sleep(1.0)
                     continue

                # Get data from queues
                tasks = [self._process_queue(name) for name in notifying_devices]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                # Check if any data was processed
                processed_data_this_cycle = any(res is True for res in results)
                processed_data_since_start = processed_data_since_start or processed_data_this_cycle

                # If no data was processed, short pause
                if not processed_data_this_cycle:
                    await asyncio.sleep(0.05)

            log.info("Data Processor has stopped its main loop.")
            if self.current_phase == ReaderPhase.PHASE_2_READING_DATA and not processed_data_since_start:
                 log.warning("Data Processor ran but received no data.")

        except asyncio.CancelledError:
            log.info("Data Processor cancelled.")
        except Exception as e:
            log.exception(f"Unexpected error in Data Processor: {e}")
            if self._running:
                 await self.stop() # Stop if there is a critical error

    async def _process_queue(self, device_name):
        """Get and process one item from the device's queue."""
        queue = self.device_queues[device_name]
        processed = False
        try:
            # Use non-blocking get so gather doesn't get stuck if a queue is empty
            data_item = queue.get_nowait()
            log.info(data_item) # Print data
            queue.task_done()
            processed = True
        except asyncio.QueueEmpty:
            pass # Queue empty, normal
        except Exception as e:
            log.exception(f"[{device_name}] Error processing data from queue: {e}")
        return processed


    # --- Run/Stop Control ---

    # ==========================================================================
    # === MODIFIED RUN FUNCTION ===
    # ==========================================================================
    async def run(self):
        """Run the tasks according to phases."""
        if self._running:
            log.warning("Already running.")
            return

        log.info(f"--- Starting 2-Phase data reading process from {self.num_target_devices} devices ---")
        log.info(f"Target devices: {self.devices_dict}")

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
            # Since _data_processor is designed to run long-term (while self._running is True),
            # gather will effectively wait forever until an error occurs or it's cancelled.
            log.info("Program running, waiting for main tasks (manager, enabler, processor)...")
            await asyncio.gather(*self._main_tasks)

            # This log line usually won't be reached unless ALL tasks naturally finish,
            # including _data_processor, which is not expected in normal operation.
            log.info("All main tasks completed naturally (unexpected).")

        except asyncio.CancelledError:
             # This is the normal exit path when stop() is called or Ctrl+C is pressed
             log.info("Main tasks cancelled (e.g., by stop() or KeyboardInterrupt).")
        except Exception as e:
             # Catch unexpected errors from main tasks
             log.exception(f"Unexpected error during main task execution: {e}")
        finally:
            log.info("Exiting main execution block, ensuring cleanup...")
            # Call stop() to ensure everything is cleaned up properly,
            # check if stopping process has already started to avoid recursion / unnecessary recall.
            if self.current_phase != ReaderPhase.STOPPING and self.current_phase != ReaderPhase.STOPPED:
                 log.info("Calling self.stop() from run() finally block.")
                 await self.stop()
            else:
                 log.info("Stopping process already initiated or completed previously.")
    # ==========================================================================
    # === END OF MODIFIED RUN FUNCTION ===
    # ==========================================================================


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
        processor_task = next((t for t in self._main_tasks if t.get_coro().__name__ == '_data_processor'), None)
        if processor_task and not processor_task.done(): processor_task.cancel()

        enabler_task = next((t for t in self._main_tasks if t.get_coro().__name__ == '_notification_enabler'), None)
        if enabler_task and not enabler_task.done(): enabler_task.cancel()

        manager_task = next((t for t in self._main_tasks if t.get_coro().__name__ == '_connection_manager'), None)
        if manager_task and not manager_task.done(): manager_task.cancel()

        # Wait for all main tasks to finish after cancellation
        await asyncio.gather(*self._main_tasks, return_exceptions=True)
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
             await asyncio.gather(*monitor_tasks_to_wait, return_exceptions=True)
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
                 dev_name = clients_to_disconnect[i][0]
                 if isinstance(res, Exception):
                      log.error(f"Error disconnecting {dev_name} during stop: {res}")
        log.info("Client disconnection process completed.")

        self._main_tasks = []
        self.current_phase = ReaderPhase.STOPPED
        log.info("BLUETOOTH_DATA_READER stopped completely.")

    # --- Other utility methods ---
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
                  self.loop.run_until_complete(self.stop())

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
              # Create task to run stop() but don't await here
              self.loop.create_task(self.stop())
              log.info("Scheduled self.stop() on the event loop.")
         elif not self.loop.is_running():
              log.warning("Event loop not running, cannot schedule stop async.")
              # Could try running synchronously if needed, but not safe
              # try:
              #     asyncio.run(self.stop())
              # except RuntimeError as e:
              #      log.error(f"Cannot run stop synchronously: {e}")


# --- Utility Code (BLUETOOTH_DETECTOR, DISCONNECT_DEVICE, main) ---
# --- Keep as is or modify slightly if needed ---

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
                  if device.name and device.name in self.aim_devices:
                       # Only take the first one found
                       if device.name not in found_devices:
                           log.info(f"[OUTSYSTEM_SCANNING]: Found target device {device.name} at {device.address}")
                           found_devices[device.name] = device.address
        except BleakError as e:
             log.error(f"[OUTSYSTEM_SCANNING] BleakError during scan: {e}")
        except Exception as e:
             log.exception(f"[OUTSYSTEM_SCANNING] Unexpected error during scan: {e}")
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
            stdout, stderr = await proc.communicate()
            devices_output = stdout.decode('utf-8', errors='ignore')
            stderr_output = stderr.decode('utf-8', errors='ignore')

            # Ignore non-critical errors
            if stderr_output and "not available" not in stderr_output :
                log.warning(f"[INSYSTEM_SCANNING] bluetoothctl stderr: {stderr_output.strip()}")

            for line in devices_output.splitlines():
                 # Example line: "Device 58:3B:C2:56:8E:07 IMU2"
                 if "Device" in line:
                      # Split into max 3 parts
                      parts = line.strip().split(" ", 2)
                      if len(parts) >= 3:
                           mac_address = parts[1].strip()
                           # Device name usually here
                           device_info = parts[2].strip()
                           # Find target name in device info
                           name = None
                           for target_name in self.aim_devices:
                               # Exact match or contains (depends on bluetoothctl output)
                               if target_name == device_info or target_name in device_info:
                                   name = target_name
                                   break

                           # If a matching name is found
                           if name:
                                log.info(f"[INSYSTEM_SCANNING]: Found connected target device: {name} ({mac_address}). Attempting disconnect.")
                                connected_target_devices[name] = mac_address
                                # Call disconnect function
                                await DISCONNECT_DEVICE(mac_address)
                           # else:
                           #      log.debug(f"[INSYSTEM_SCANNING] Found connected device, but not a target: {device_info} ({mac_address})")

        except FileNotFoundError:
             log.error("[INSYSTEM_SCANNING]: 'bluetoothctl' not found. Skipping check.")
        except Exception as e:
             log.exception(f"[INSYSTEM_SCANNING]: Error during check: {e}")
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
         stdout, stderr = await proc.communicate()

         stdout_str = stdout.decode('utf-8', errors='ignore').strip()
         stderr_str = stderr.decode('utf-8', errors='ignore').strip()

         # Check both stdout and stderr to determine success
         if proc.returncode == 0 and ("Successful disconnected" in stdout_str or "Device not connected" in stderr_str):
             log.info(f"[DISCONNECT_DEVICE]: Successfully disconnected or confirmed not connected: {address}")
             return True
         # Catch error case where it's actually not connected
         elif "Device not connected" in stderr_str:
              log.info(f"[DISCONNECT_DEVICE]: Device {address} was already not connected (reported via stderr).")
              return True
         else:
             log.warning(f"[DISCONNECT_DEVICE]: Failed disconnect via bluetoothctl for {address}. Return code: {proc.returncode}")
             if stdout_str: log.warning(f"[DISCONNECT_DEVICE]: Output: {stdout_str}")
             if stderr_str: log.warning(f"[DISCONNECT_DEVICE]: Error: {stderr_str}")
             return False
     except FileNotFoundError:
         log.error("[DISCONNECT_DEVICE]: 'bluetoothctl' not found. Cannot disconnect via system tool.")
         return False
     except Exception as e:
         log.exception(f"[DISCONNECT_DEVICE]: Unexpected error disconnecting {address}: {e}")
         return False


# --- Main execution ---
if __name__ == "__main__":
    log.info("--- Starting Bluetooth IMU Data Collector (Two-Phase) ---")

    target_devices_names = ["IMU1", "IMU2", "IMU3", "IMU4", "IMU5", "IMU6"]
    log.info(f"Target devices: {target_devices_names}")

    detector = BLUETOOTH_DETECTOR(target_devices_names)
    found_devices_map = {}
    reader = None

    # --- Run Event Loop ---
    # Get existing loop or create a new one if needed
    try:
        main_loop = asyncio.get_running_loop()
    except RuntimeError:
        main_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(main_loop)

    run_reader = False # Flag to decide whether to run the reader

    try:
        # 1. Disconnect old connections (INSYSTEM)
        log.info("--- Preliminary Step: Checking and Disconnecting Existing Connections ---")
        main_loop.run_until_complete(detector.INSYSTEM_SCANNING())
        log.info("Finished checking existing connections. Waiting briefly...")
        # Increase wait time
        main_loop.run_until_complete(asyncio.sleep(3.0))

        # 2. Scan for devices (OUTSYSTEM)
        log.info("--- Preliminary Step: Scanning for Target Devices ---")
        found_devices_map = main_loop.run_until_complete(detector.OUTSYSTEM_SCANNING())

        # 3. Initialize and run Data Reader if at least one device is found
        if found_devices_map:
            log.info(f"--- Found {len(found_devices_map)} target devices: {found_devices_map} ---")
            if len(found_devices_map) < len(target_devices_names):
                 log.warning(f"Could not find all {len(target_devices_names)} target devices. Found only: {list(found_devices_map.keys())}")

            log.info("--- Initializing Two-Phase Data Reader ---")
            # Create reader instance, pass loop if needed (reader gets loop by default)
            reader = BLUETOOTH_DATA_READER(
                found_devices_map,
                connection_interval_sec=6.0, # Slight increase
                retry_interval_sec=12.0,    # Slight increase
                max_connect_retries=5,      # Keep or adjust
                bleak_timeout=25.0          # Slight increase
            )
            # Set flag to run reader
            run_reader = True
        else:
            log.warning("--- No target devices found during scan. Exiting. ---")

        # 4. Run Reader if devices were found
        if run_reader and reader:
             log.info("--- Starting Data Reader Execution (calling start_reading) ---")
             # reader.start_reading() will block until stopped
             # It calls reader.run() internally using the reader's loop
             # Call the created synchronous function
             reader.start_reading()
             log.info("--- Data Reader Execution Finished (start_reading returned) ---")
        else:
             log.info("--- Reader will not start as no devices were found or initialized. ---")


    except KeyboardInterrupt:
        log.info("\n--- KeyboardInterrupt detected in main block. Exiting program. ---")
        # No need to call stop here anymore as start_reading/run handles it
    except Exception as e:
         log.exception("--- An unexpected error occurred in the main execution block ---")
         # No need to call stop here anymore as start_reading/run handles it
    finally:
        log.info("--- Starting Main Cleanup Sequence ---")
        # No need to call reader.stop() here as it's handled in reader.run() finally block
        # Or in the exception handling of start_reading().

        # Just need to ensure the event loop is closed properly if we created it
        # and it hasn't been closed yet.
        if not main_loop.is_closed():
             # Only close loop if it's not running anymore
             if not main_loop.is_running():
                  log.info("Closing main event loop...")
                  main_loop.close()
                  log.info("Main event loop closed.")
             else:
                  # If loop is still running (rare case, maybe due to previous cleanup error)
                  log.warning("Main event loop is still running during final cleanup. Attempting forceful shutdown.")
                  # Try to cancel remaining tasks and close
                  try:
                      tasks = [t for t in asyncio.all_tasks(loop=main_loop) if t is not asyncio.current_task()]
                      if tasks:
                           log.debug(f"Cancelling {len(tasks)} remaining tasks in main loop...")
                           for task in tasks: task.cancel()
                           main_loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
                      main_loop.run_until_complete(main_loop.shutdown_asyncgens())
                      main_loop.close()
                      log.info("Main event loop forcefully closed.")
                  except Exception as loop_close_err:
                       log.error(f"Error during forceful loop close: {loop_close_err}")

        log.info("--- Program Finished ---")