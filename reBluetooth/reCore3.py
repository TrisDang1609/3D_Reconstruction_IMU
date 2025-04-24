import asyncio
from bleak import BleakScanner, BleakClient, BleakError
import struct
import numpy as np
from collections import deque
import time
import logging
from enum import Enum

# Cấu hình logging chi tiết hơn
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

# --- UUIDs (Giữ nguyên từ code gốc) ---
IMU_SERVICE_UUID = "2412b5cbd460800c15c39ba9ac5a8ade"
LED_CONTROL_CHAR_UUID = "5b02651040-88c29746d8be6c736a087a"
IMU_NOTIFY_CHAR_UUID = "61a885a441c360d09a536d652a70d29c"

# Known characteristic UUIDs for IMU devices - add your three specific UUIDs here
IMU_CHARACTERISTIC_UUIDS = [
    "61A885A4-41C3-60D0-9A53-6D652A70D29C",  # Primary UUID
    IMU_NOTIFY_CHAR_UUID, # Thêm UUID này nếu khác
    "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",  # Replace with your second UUID if needed
    "YYYYYYYY-YYYY-YYYY-YYYY-YYYYYYYYYYYY",  # Replace with your third UUID if needed
]
# Loại bỏ các UUID trùng lặp và chuẩn hóa
IMU_CHARACTERISTIC_UUIDS = list(set(uuid.upper() for uuid in IMU_CHARACTERISTIC_UUIDS))

# Device-specific characteristics mapping (can be populated as devices are discovered)
DEVICE_CHARACTERISTICS = {} # Sẽ tự động cập nhật khi tìm thấy UUID hoạt động

# --- Enum trạng thái thiết bị ---
class DeviceStatus(Enum):
    DISCONNECTED = 0
    CONNECTING = 1
    CONNECTED = 2
    FAILED_PERMANENTLY = 3
    WAITING_RETRY = 4
    DISCONNECTING = 5

# --- Class BLUETOOTH_DATA_READER viết lại ---
class BLUETOOTH_DATA_READER:

    def __init__(self, devices_dict,
                 connection_interval_sec=5.0, # Thời gian nghỉ giữa các lần thử kết nối thiết bị KHÁC NHAU
                 retry_interval_sec=10.0,    # Thời gian nghỉ trước khi thử kết nối lại thiết bị THẤT BẠI
                 max_connect_retries=5,      # Số lần thử kết nối tối đa cho một thiết bị trước khi bỏ cuộc
                 bleak_timeout=20.0):        # Timeout cho thao tác của Bleak
        """
        Khởi tạo Data Reader.

        Args:
            devices_dict (dict): Dictionary {device_name: mac_address}.
            connection_interval_sec (float): Thời gian chờ (giây) trước khi thử kết nối thiết bị tiếp theo.
            retry_interval_sec (float): Thời gian chờ (giây) trước khi thử kết nối lại thiết bị đã thất bại.
            max_connect_retries (int): Số lần thử kết nối tối đa cho mỗi thiết bị.
            bleak_timeout (float): Timeout (giây) cho các thao tác của BleakClient.
        """
        if not devices_dict:
            raise ValueError("devices_dict không được để trống.")

        self.devices_dict = devices_dict
        self.device_names = list(devices_dict.keys())
        self.loop = asyncio.get_event_loop()

        # Cấu hình
        self.connection_interval = connection_interval_sec
        self.retry_interval = retry_interval_sec
        self.max_retries = max_connect_retries
        self.bleak_timeout = bleak_timeout

        # Quản lý trạng thái và tài nguyên
        self.device_states = {name: DeviceStatus.DISCONNECTED for name in self.device_names}
        self.device_clients = {name: None for name in self.device_names} # Lưu trữ BleakClient instance
        self.device_queues = {name: asyncio.Queue() for name in self.device_names}
        self.device_retries = {name: 0 for name in self.device_names}
        self.successful_characteristics = {} # Lưu UUID thành công cho từng thiết bị
        self.monitor_tasks = {} # Lưu các task giám sát kết nối
        self.notification_handlers = {} # Lưu các handler để có thể detach

        self._main_tasks = [] # Lưu các task chính (manager, processor)
        self._running = False

    # --- Xử lý Notification ---
    async def _handle_notification(self, device_name, sender_handle: int, data: bytearray):
        """Xử lý dữ liệu nhận được từ notification."""
        queue = self.device_queues.get(device_name)
        if not queue:
            log.warning(f"[{device_name}] Không tìm thấy queue khi nhận notification.")
            return

        if len(data) >= 28: # Giả sử dữ liệu gồm 7 float (4 bytes/float)
            try:
                # Unpack 7 float values (little-endian)
                values = struct.unpack('<7f', data[:28])
                timestamp = time.time()
                # Định dạng chuỗi output
                output_str = f"[DATA] {device_name} {timestamp:.6f}: {' '.join(f'{v:.6f}' for v in values)}"
                await queue.put(output_str)
            except struct.error as e:
                log.warning(f"[{device_name}] Lỗi giải nén dữ liệu: {e}. Dữ liệu nhận được (bytes): {data.hex()}")
            except Exception as e:
                log.error(f"[{device_name}] Lỗi không xác định khi xử lý notification: {e}")
        # else:
        #     log.debug(f"[{device_name}] Nhận được dữ liệu không đủ dài: {len(data)} bytes")

    # --- Kết nối và Ngắt kết nối ---
    async def _attempt_connection(self, device_name, address):
        """Cố gắng kết nối tới một thiết bị, tìm characteristic và bắt đầu notify."""
        log.info(f"[{device_name}] Bắt đầu kết nối tới {address} (Thử lần {self.device_retries[device_name] + 1}/{self.max_retries})...")
        self.device_states[device_name] = DeviceStatus.CONNECTING

        client = BleakClient(address, timeout=self.bleak_timeout)
        used_characteristic = None

        try:
            await client.connect()
            if not client.is_connected:
                 log.warning(f"[{device_name}] Kết nối thất bại (client không ở trạng thái connected).")
                 await self._mark_connection_failure(device_name)
                 return None # Kết nối thất bại

            log.info(f"[{device_name}] Kết nối thành công tới {address}.")

            # Cố gắng tăng MTU (tùy chọn, có thể gây lỗi với một số thiết bị)
            try:
                mtu = await client.exchange_mtu(247)
                log.info(f"[{device_name}] Đã đặt MTU thành công: {mtu}")
            except Exception as e:
                log.warning(f"[{device_name}] Không thể đặt MTU: {e}. Tiếp tục với MTU mặc định.")

            # Xác định characteristic để sử dụng
            characteristics_to_try = []
            # Ưu tiên UUID đã thành công trước đó cho thiết bị này
            if device_name in self.successful_characteristics:
                characteristics_to_try.append(self.successful_characteristics[device_name])
            # Thêm các UUID chung khác (đảm bảo không trùng lặp)
            for uuid in IMU_CHARACTERISTIC_UUIDS:
                if uuid not in characteristics_to_try:
                    characteristics_to_try.append(uuid)

            log.debug(f"[{device_name}] Các UUID sẽ thử: {characteristics_to_try}")

            # Lấy danh sách characteristics thực tế có trên thiết bị
            available_chars = set()
            try:
                svcs = await client.get_services()
                for service in svcs:
                    for char in service.characteristics:
                        if "notify" in char.properties:
                             available_chars.add(char.uuid.upper())
                log.debug(f"[{device_name}] Các characteristics hỗ trợ notify: {available_chars}")
            except Exception as e:
                 log.error(f"[{device_name}] Lỗi khi lấy services/characteristics: {e}")
                 await self._disconnect_client(client, device_name)
                 await self._mark_connection_failure(device_name)
                 return None


            # Thử từng characteristic ưu tiên
            for char_uuid_str in characteristics_to_try:
                 normalized_uuid = char_uuid_str.upper()
                 if normalized_uuid in available_chars:
                     log.info(f"[{device_name}] Đang thử bắt đầu notify cho UUID: {char_uuid_str}...")
                     try:
                         # Tạo handler cụ thể cho device này
                         handler = lambda s, d: self.loop.create_task(self._handle_notification(device_name, s, d))
                         await client.start_notify(char_uuid_str, handler)
                         used_characteristic = char_uuid_str
                         self.successful_characteristics[device_name] = used_characteristic # Lưu lại UUID thành công
                         self.notification_handlers[device_name] = handler # Lưu handler để stop sau
                         log.info(f"[{device_name}] Bắt đầu notify thành công với UUID: {used_characteristic}")
                         break # Đã tìm thấy và sử dụng được
                     except BleakError as e:
                         log.warning(f"[{device_name}] Lỗi khi start_notify cho {char_uuid_str}: {e}")
                     except Exception as e:
                         log.error(f"[{device_name}] Lỗi không xác định khi start_notify cho {char_uuid_str}: {e}")
                 # else:
                 #     log.debug(f"[{device_name}] UUID {normalized_uuid} không có sẵn hoặc không hỗ trợ notify.")

            if not used_characteristic:
                log.error(f"[{device_name}] Không thể bắt đầu notify với bất kỳ characteristic nào.")
                await self._disconnect_client(client, device_name)
                await self._mark_connection_failure(device_name)
                return None

            # Kết nối và notify thành công
            self.device_clients[device_name] = client
            self.device_states[device_name] = DeviceStatus.CONNECTED
            self.device_retries[device_name] = 0 # Reset số lần thử lại
            log.info(f"[{device_name}] Thiết lập thành công.")

            # Bắt đầu tác vụ giám sát kết nối cho thiết bị này
            self._start_monitoring_task(device_name, client)

            return client # Trả về client nếu thành công

        except BleakError as e:
            log.error(f"[{device_name}] Lỗi Bleak khi kết nối tới {address}: {e}")
            await self._mark_connection_failure(device_name)
            # Đảm bảo client được dọn dẹp nếu đã được tạo
            if client and client.is_connected: await self._disconnect_client(client, device_name, suppress_errors=True)
            elif client: await self._cleanup_client_resource(client) # For cases where connect fails early
            return None
        except asyncio.TimeoutError:
            log.error(f"[{device_name}] Timeout khi kết nối tới {address}.")
            await self._mark_connection_failure(device_name)
            if client and client.is_connected: await self._disconnect_client(client, device_name, suppress_errors=True)
            elif client: await self._cleanup_client_resource(client)
            return None
        except Exception as e:
            log.exception(f"[{device_name}] Lỗi không xác định khi kết nối tới {address}: {e}")
            await self._mark_connection_failure(device_name)
            if client and client.is_connected: await self._disconnect_client(client, device_name, suppress_errors=True)
            elif client: await self._cleanup_client_resource(client)
            return None

    async def _mark_connection_failure(self, device_name):
        """Đánh dấu thiết bị là thất bại và quyết định trạng thái tiếp theo."""
        self.device_clients[device_name] = None # Xóa client instance cũ nếu có
        self.device_retries[device_name] += 1
        if self.device_retries[device_name] >= self.max_retries:
            log.error(f"[{device_name}] Đã đạt số lần thử kết nối tối đa ({self.max_retries}). Bỏ cuộc.")
            self.device_states[device_name] = DeviceStatus.FAILED_PERMANENTLY
        else:
            log.info(f"[{device_name}] Sẽ thử kết nối lại sau {self.retry_interval} giây.")
            self.device_states[device_name] = DeviceStatus.WAITING_RETRY
            # Lên lịch thử lại (không cần thiết nếu vòng lặp chính xử lý việc này)

    async def _disconnect_client(self, client: BleakClient, device_name: str, suppress_errors=False):
        """Ngắt kết nối một client một cách an toàn."""
        if not client:
            log.debug(f"[{device_name}] Client không tồn tại, không cần ngắt kết nối.")
            return

        log.info(f"[{device_name}] Đang ngắt kết nối...")
        self.device_states[device_name] = DeviceStatus.DISCONNECTING

        # Dừng notify trước khi disconnect
        handler = self.notification_handlers.pop(device_name, None)
        if handler and client.is_connected:
             # Cần biết UUID đã dùng để stop_notify
             used_uuid = self.successful_characteristics.get(device_name)
             if used_uuid:
                 try:
                     log.debug(f"[{device_name}] Đang dừng notify cho UUID {used_uuid}...")
                     await client.stop_notify(used_uuid)
                     log.debug(f"[{device_name}] Đã dừng notify.")
                 except Exception as e:
                      log.warning(f"[{device_name}] Lỗi khi dừng notify cho {used_uuid}: {e}")
             # else:
             #     log.warning(f"[{device_name}] Không tìm thấy UUID đã sử dụng để dừng notify.")

        # Ngắt kết nối client
        try:
            if client.is_connected:
                await client.disconnect()
                log.info(f"[{device_name}] Đã ngắt kết nối thành công.")
            else:
                 log.info(f"[{device_name}] Client đã ở trạng thái ngắt kết nối.")
        except BleakError as e:
            log.error(f"[{device_name}] Lỗi Bleak khi ngắt kết nối: {e}")
            if suppress_errors: pass
            else: raise # Ném lại lỗi nếu không yêu cầu bỏ qua
        except Exception as e:
            log.error(f"[{device_name}] Lỗi không xác định khi ngắt kết nối: {e}")
            if suppress_errors: pass
            else: raise
        finally:
             # Đảm bảo trạng thái được cập nhật ngay cả khi có lỗi
             if self.device_states[device_name] == DeviceStatus.DISCONNECTING:
                  # Chỉ cập nhật nếu chưa bị ghi đè bởi trạng thái khác (vd: FAILED)
                  self.device_states[device_name] = DeviceStatus.DISCONNECTED
             self.device_clients[device_name] = None # Dọn dẹp client instance

    async def _cleanup_client_resource(self, client: BleakClient):
        """ Ensures underlying resources are cleaned up for a client object
            that might not have successfully connected or disconnected cleanly.
        """
        if hasattr(client, "_backend") and hasattr(client._backend, "disconnect"):
             try:
                 # This is delving into internals, might break in future Bleak versions
                 await client._backend.disconnect()
                 log.debug("Cleaned up underlying client backend resource.")
             except Exception as e:
                 log.warning(f"Exception during internal client resource cleanup: {e}")


    # --- Giám sát Kết nối ---
    def _start_monitoring_task(self, device_name, client):
        """Bắt đầu một task chạy ngầm để giám sát kết nối của thiết bị."""
        if device_name in self.monitor_tasks and not self.monitor_tasks[device_name].done():
            log.debug(f"[{device_name}] Task giám sát đã chạy.")
            return

        log.debug(f"[{device_name}] Bắt đầu task giám sát kết nối.")
        task = self.loop.create_task(self._connection_monitor(device_name, client))
        self.monitor_tasks[device_name] = task

    async def _connection_monitor(self, device_name, client):
        """Task chạy ngầm, kiểm tra client.is_connected định kỳ."""
        while self._running and self.device_states.get(device_name) == DeviceStatus.CONNECTED:
            await asyncio.sleep(2.0) # Kiểm tra mỗi 2 giây
            if not client or not client.is_connected:
                log.warning(f"[{device_name}] Phát hiện mất kết nối!")
                # Đánh dấu cần kết nối lại, vòng lặp chính sẽ xử lý
                self.device_clients[device_name] = None
                # Không cần gọi _disconnect_client vì nó đã mất kết nối rồi
                # Cần đánh dấu để vòng lặp chính thử lại
                if self.device_states[device_name] == DeviceStatus.CONNECTED: # Đảm bảo chỉ thay đổi nếu đang CONNECTED
                     await self._mark_connection_failure(device_name) # Đánh dấu thất bại để retry
                break # Kết thúc task giám sát này
        log.debug(f"[{device_name}] Kết thúc task giám sát.")
        # Dọn dẹp task khỏi dictionary khi nó kết thúc
        if device_name in self.monitor_tasks and self.monitor_tasks[device_name].done():
             del self.monitor_tasks[device_name]


    # --- Vòng lặp Quản lý Chính ---
    async def _connection_manager(self):
        """Vòng lặp chính quản lý trạng thái và kết nối các thiết bị."""
        log.info("Bắt đầu Connection Manager...")
        await asyncio.sleep(1.0) # Chờ một chút trước khi bắt đầu

        while self._running:
            all_permanently_failed = True
            processed_device_this_cycle = False

            for device_name in self.device_names:
                current_state = self.device_states[device_name]

                # Chỉ xử lý các thiết bị chưa kết nối hoặc cần thử lại
                if current_state in [DeviceStatus.DISCONNECTED, DeviceStatus.WAITING_RETRY]:
                    log.info(f"[{device_name}] Trạng thái: {current_state}. Sẽ thử kết nối.")
                    address = self.devices_dict[device_name]
                    await self._attempt_connection(device_name, address)
                    processed_device_this_cycle = True
                    # Thêm khoảng nghỉ *sau khi* thử kết nối một thiết bị
                    # để giảm tải cho adapter trước khi thử thiết bị tiếp theo.
                    log.debug(f"Nghỉ {self.connection_interval} giây trước khi kiểm tra/kết nối thiết bị tiếp theo...")
                    await asyncio.sleep(self.connection_interval)

                # Cập nhật biến kiểm tra nếu còn thiết bị chưa FAILED_PERMANENTLY
                if current_state != DeviceStatus.FAILED_PERMANENTLY:
                    all_permanently_failed = False


            # Nếu không có thiết bị nào được xử lý trong chu kỳ này (tức là tất cả đều đang connected hoặc failed),
            # thì nghỉ một khoảng thời gian dài hơn trước khi kiểm tra lại.
            if not processed_device_this_cycle and self._running:
                 await asyncio.sleep(self.retry_interval) # Dùng retry_interval để chờ lâu hơn

            # Thoát nếu tất cả thiết bị đều thất bại vĩnh viễn
            if all_permanently_failed:
                log.error("Tất cả các thiết bị đều không thể kết nối sau nhiều lần thử. Dừng Connection Manager.")
                self._running = False # Dừng các vòng lặp khác
                break

        log.info("Connection Manager đã dừng.")

    # --- Vòng lặp Xử lý Dữ liệu ---
    async def _data_processor(self):
        """Vòng lặp xử lý dữ liệu từ các queue."""
        log.info("Bắt đầu Data Processor...")
        reported_waiting = False

        while self._running:
            # Xác định các thiết bị đang ở trạng thái CONNECTED
            connected_device_names = [name for name, state in self.device_states.items()
                                      if state == DeviceStatus.CONNECTED]

            if not connected_device_names:
                if not reported_waiting:
                    log.info("Chưa có thiết bị nào được kết nối. Đang chờ...")
                    reported_waiting = True
                await asyncio.sleep(1.0) # Chờ nếu không có thiết bị nào kết nối
                continue # Quay lại đầu vòng lặp

            # Nếu có ít nhất một thiết bị đã kết nối
            if reported_waiting:
                 log.info(f"Bắt đầu xử lý dữ liệu từ các thiết bị đã kết nối: {connected_device_names}")
                 reported_waiting = False # Reset cờ báo cáo

            processed_data = False
            # Lặp qua các thiết bị đã kết nối để lấy dữ liệu từ queue
            for name in connected_device_names:
                 queue = self.device_queues[name]
                 try:
                     # Lấy dữ liệu không chặn (timeout ngắn)
                     data_item = await asyncio.wait_for(queue.get(), timeout=0.01)
                     log.info(data_item) # In dữ liệu đã định dạng
                     queue.task_done()
                     processed_data = True
                 except asyncio.TimeoutError:
                     # Không có dữ liệu trong queue này, tiếp tục với thiết bị khác
                     pass
                 except Exception as e:
                      log.exception(f"[{name}] Lỗi khi xử lý dữ liệu từ queue: {e}")

            # Nếu không xử lý được dữ liệu nào trong vòng lặp này, nghỉ một chút
            if not processed_data:
                await asyncio.sleep(0.05)

        log.info("Data Processor đã dừng.")


    # --- Điều khiển Chạy/Dừng ---
    async def run(self):
        """Chạy các tác vụ quản lý kết nối và xử lý dữ liệu."""
        if self._running:
            log.warning("Đã chạy rồi.")
            return

        log.info(f"Bắt đầu quá trình đọc dữ liệu từ {len(self.device_names)} thiết bị...")
        log.info(f"Thiết bị mục tiêu: {self.devices_dict}")
        if self.successful_characteristics:
             log.info(f"UUID đã biết hoạt động: {self.successful_characteristics}")

        self._running = True
        # Reset trạng thái trước khi chạy
        self.device_states = {name: DeviceStatus.DISCONNECTED for name in self.device_names}
        self.device_clients = {name: None for name in self.device_names}
        self.device_retries = {name: 0 for name in self.device_names}
        # Không reset successful_characteristics để tận dụng lần chạy trước

        manager_task = self.loop.create_task(self._connection_manager())
        processor_task = self.loop.create_task(self._data_processor())
        self._main_tasks = [manager_task, processor_task]

        try:
            # Chờ một trong các task chính kết thúc (hoặc có lỗi)
            done, pending = await asyncio.wait(self._main_tasks, return_when=asyncio.FIRST_COMPLETED)

            # Xử lý kết quả và lỗi
            for task in done:
                try:
                    task.result() # Lấy kết quả hoặc raise exception nếu có lỗi trong task
                except asyncio.CancelledError:
                     log.info("Một task chính đã bị hủy.")
                except Exception as e:
                    log.exception(f"Task chính kết thúc với lỗi: {e}")

            log.info("Một task chính đã hoàn thành hoặc có lỗi. Bắt đầu quá trình dừng...")

        except asyncio.CancelledError:
             log.info("Yêu cầu hủy bỏ từ bên ngoài.")
        finally:
            await self.stop() # Đảm bảo dừng sạch sẽ

    async def stop(self):
        """Dừng tất cả các tác vụ và ngắt kết nối an toàn."""
        if not self._running:
            return
        log.info("Đang dừng BLUETOOTH_DATA_READER...")
        self._running = False # Tín hiệu dừng cho các vòng lặp

        # 1. Hủy các task chính (manager, processor)
        log.debug("Hủy các task chính...")
        for task in self._main_tasks:
            if task and not task.done():
                task.cancel()
        # Chờ các task chính thực sự kết thúc sau khi cancel
        await asyncio.gather(*self._main_tasks, return_exceptions=True)
        log.debug("Các task chính đã dừng.")

        # 2. Hủy các task giám sát kết nối
        log.debug("Hủy các task giám sát...")
        monitor_tasks_to_wait = []
        for name, task in list(self.monitor_tasks.items()): # Dùng list để tránh lỗi thay đổi dict khi lặp
             if task and not task.done():
                  task.cancel()
                  monitor_tasks_to_wait.append(task)
             # Xóa luôn khỏi dict
             if name in self.monitor_tasks: del self.monitor_tasks[name]

        if monitor_tasks_to_wait:
             await asyncio.gather(*monitor_tasks_to_wait, return_exceptions=True)
        log.debug("Các task giám sát đã dừng.")

        # 3. Ngắt kết nối tất cả các client đang hoạt động
        log.debug("Ngắt kết nối các client...")
        disconnect_tasks = []
        for name, client in list(self.device_clients.items()): # Dùng list để tránh lỗi
            if client and (client.is_connected or self.device_states.get(name) == DeviceStatus.CONNECTING):
                 # Chỉ disconnect nếu client thực sự tồn tại và có vẻ đang hoạt động
                 disconnect_tasks.append(self._disconnect_client(client, name, suppress_errors=True))
            # Dọn dẹp client khỏi dict
            self.device_clients[name] = None

        if disconnect_tasks:
            await asyncio.gather(*disconnect_tasks, return_exceptions=True)
        log.info("Đã ngắt kết nối tất cả client.")

        self._main_tasks = []
        log.info("BLUETOOTH_DATA_READER đã dừng hoàn toàn.")

    # --- Phương thức tiện ích ---
    def get_device_status(self, device_name):
        """Lấy trạng thái hiện tại của một thiết bị."""
        return self.device_states.get(device_name, None)

    def get_all_statuses(self):
        """Lấy trạng thái của tất cả các thiết bị."""
        return self.device_states.copy()

    def start_reading(self):
        """Phương thức đồng bộ để bắt đầu quá trình đọc."""
        if self._running:
            log.warning("Reader đã chạy rồi.")
            return
        try:
            log.info("Chạy run() từ start_reading()...")
            self.loop.run_until_complete(self.run())
        except KeyboardInterrupt:
            log.info("Phát hiện KeyboardInterrupt. Đang dừng...")
            # stop() sẽ được gọi trong khối finally của run()
        except Exception as e:
             log.exception(f"Lỗi không xác định trong start_reading: {e}")
             # Cố gắng dừng sạch sẽ
             if self._running:
                  self.loop.run_until_complete(self.stop())

    def stop_reading(self):
         """Phương thức đồng bộ để dừng quá trình đọc."""
         if not self._running:
              log.warning("Reader chưa chạy.")
              return
         log.info("Yêu cầu dừng từ stop_reading()...")
         # Chạy hàm stop không đồng bộ trong event loop
         # Cẩn thận nếu loop đã dừng, nhưng run_until_complete thường xử lý được
         try:
            self.loop.create_task(self.stop()) # Lên lịch chạy stop
            # Có thể cần chờ loop chạy một chút để stop hoàn thành
            # self.loop.run_until_complete(asyncio.sleep(0.1)) # Chờ nhẹ nhàng
         except RuntimeError as e:
              log.warning(f"Lỗi khi lên lịch stop (có thể loop đã đóng): {e}")


# --- Phần code sử dụng (giữ nguyên hoặc sửa đổi nhẹ) ---
# (Giữ nguyên class BLUETOOTH_DETECTOR và hàm DISCONNECT_DEVICE từ code gốc)
class BLUETOOTH_DETECTOR:
    # ... (Code gốc của BLUETOOTH_DETECTOR) ...
    def __init__(self, needy_devices):
        self.devices = {}
        self.loop = asyncio.get_event_loop() # Sửa: Khởi tạo loop ở đây
        self.aim_devices = needy_devices

    async def OUTSYSTEM_SCANNING(self):
        log.info("OUTSYSTEM_SCANNING: Scanning for Bluetooth devices...")
        bleak_devices = await BleakScanner.discover()
        found_devices = {}
        for device in bleak_devices:
            if device.name and device.name in self.aim_devices:
                log.info(f"[OUTSYSTEM_SCANNING]: Found target device {device.name} at {device.address}")
                found_devices[device.name] = device.address
        return found_devices

    async def INSYSTEM_SCANNING(self):
        # ... (Code gốc, có thể thêm logging)
        log.info("INSYSTEM_SCANNING: Checking connected devices via bluetoothctl...")
        # ... phần còn lại của hàm ...
        connected_target_devices = {} # Store found devices to disconnect
        try:
            proc = await asyncio.create_subprocess_exec(
                'bluetoothctl', 'devices', 'Connected', # Chỉ lấy device đã connected
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            devices_output, stderr = await proc.communicate()
            devices_output = devices_output.decode('utf-8', errors='ignore')

            for line in devices_output.splitlines():
                 if "Device" in line:
                      parts = line.split("Device ", 1)[1].split(" ", 1)
                      if len(parts) == 2:
                           mac_address = parts[0].strip()
                           name = parts[1].strip().split(" ")[0] # Cố gắng lấy tên gọn hơn
                           if name in self.aim_devices:
                                log.info(f"[INSYSTEM_SCANNING]: Found connected target device: {name} ({mac_address}). Attempting disconnect.")
                                connected_target_devices[name] = mac_address
                                # Gọi hàm disconnect (đảm bảo hàm DISCONNECT_DEVICE tồn tại)
                                await DISCONNECT_DEVICE(mac_address) # Giả sử hàm này đã định nghĩa ở global scope

        except FileNotFoundError:
             log.error("[INSYSTEM_SCANNING]: 'bluetoothctl' not found. Skipping check.")
        except Exception as e:
             log.exception(f"[INSYSTEM_SCANNING]: Error during check: {e}")
        return connected_target_devices # Trả về các thiết bị đã tìm thấy và cố gắng ngắt kết nối


async def DISCONNECT_DEVICE(address):
     # ... (Code gốc của DISCONNECT_DEVICE, thêm logging) ...
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

         if proc.returncode == 0 and ("Successful disconnected" in stdout_str or "not connected" in stderr_str):
             log.info(f"[DISCONNECT_DEVICE]: Successfully disconnected or confirmed not connected: {address}")
             return True
         else:
             log.warning(f"[DISCONNECT_DEVICE]: Failed disconnect via bluetoothctl for {address}.")
             if stdout_str: log.warning(f"[DISCONNECT_DEVICE]: Output: {stdout_str}")
             if stderr_str: log.warning(f"[DISCONNECT_DEVICE]: Error: {stderr_str}")
             # Có thể thử lại hoặc xử lý khác ở đây
             return False
     except FileNotFoundError:
         log.error("[DISCONNECT_DEVICE]: 'bluetoothctl' not found. Cannot disconnect via system tool.")
         return False
     except Exception as e:
         log.exception(f"[DISCONNECT_DEVICE]: Unexpected error disconnecting {address}: {e}")
         return False


# --- Main execution ---
if __name__ == "__main__":
    log.info("--- Starting Bluetooth IMU Data Collector ---")

    target_devices_names = ["IMU1", "IMU2", "IMU3", "IMU4", "IMU5", "IMU6"]
    log.info(f"Target devices: {target_devices_names}")

    detector = BLUETOOTH_DETECTOR(target_devices_names)
    scanner = BleakScanner() # Dùng BleakScanner cho tiện
    found_devices_map = {}
    reader = None # Khởi tạo reader là None

    # --- Chạy Event Loop ---
    main_loop = asyncio.get_event_loop()
    try:
        # 1. Ngắt kết nối các thiết bị mục tiêu nếu đang kết nối (INSYSTEM)
        log.info("--- Phase 1: Checking and Disconnecting Existing Connections ---")
        main_loop.run_until_complete(detector.INSYSTEM_SCANNING())
        log.info("Finished checking existing connections. Waiting briefly...")
        main_loop.run_until_complete(asyncio.sleep(2.0)) # Chờ một chút để hệ thống ổn định

        # 2. Quét tìm các thiết bị mục tiêu (OUTSYSTEM)
        log.info("--- Phase 2: Scanning for Target Devices ---")
        # Sử dụng hàm quét tích hợp sẵn của bleak cho đơn giản
        async def scan_for_targets():
             global found_devices_map
             log.info("Scanning using BleakScanner...")
             devices = await BleakScanner.discover(timeout=10.0) # Quét trong 10 giây
             count = 0
             for d in devices:
                  # Ưu tiên tên, nếu không có tên thì bỏ qua hoặc xử lý khác
                  if d.name and d.name in target_devices_names:
                       if d.name not in found_devices_map: # Chỉ lấy thiết bị đầu tiên tìm thấy với tên đó
                           log.info(f"Found target: {d.name} at {d.address}")
                           found_devices_map[d.name] = d.address
                           count += 1
             log.info(f"Scan complete. Found {count} target devices.")

        main_loop.run_until_complete(scan_for_targets())

        # 3. Khởi tạo và chạy Data Reader nếu tìm thấy thiết bị
        if found_devices_map:
            log.info("--- Phase 3: Initializing and Starting Data Reader ---")
            log.info(f"Devices to connect: {found_devices_map}")
            # Tăng connection_interval nếu gặp vấn đề
            reader = BLUETOOTH_DATA_READER(found_devices_map, connection_interval_sec=7.0, retry_interval_sec=15.0)
            reader.start_reading() # Hàm này sẽ block cho đến khi bị dừng hoặc lỗi
        else:
            log.warning("--- No target devices found during scan. Exiting. ---")
            # Tùy chọn: Chạy với địa chỉ test nếu muốn
            # log.info("Using hardcoded test devices for demonstration.")
            # test_devices = {"IMU_T1": "XX:XX:XX:XX:XX:XX", "IMU_T2": "YY:YY:YY:YY:YY:YY"}
            # reader = BLUETOOTH_DATA_READER(test_devices)
            # reader.start_reading()

    except KeyboardInterrupt:
        log.info("--- KeyboardInterrupt detected in main block ---")
    except Exception as e:
         log.exception("--- An unexpected error occurred in the main execution block ---")
    finally:
        log.info("--- Starting main cleanup ---")
        if reader and reader._running:
             log.info("Requesting reader to stop...")
             # Cần chạy stop trong loop nếu nó vẫn còn chạy
             if main_loop.is_running():
                  main_loop.run_until_complete(reader.stop())
             else:
                  # Nếu loop chính đã dừng, tạo loop tạm để chạy stop
                  temp_loop = asyncio.new_event_loop()
                  asyncio.set_event_loop(temp_loop)
                  try:
                      temp_loop.run_until_complete(reader.stop())
                  finally:
                       temp_loop.close()
                       asyncio.set_event_loop(main_loop) # Khôi phục loop cũ

        # Đảm bảo event loop đóng sạch sẽ
        if main_loop.is_running():
             log.info("Closing main event loop...")
             # Hủy các task còn sót lại nếu có
             tasks = [t for t in asyncio.all_tasks(loop=main_loop) if t is not asyncio.current_task()]
             if tasks:
                  log.debug(f"Cancelling {len(tasks)} remaining tasks...")
                  for task in tasks:
                       task.cancel()
                  main_loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
             main_loop.close()
             log.info("Main event loop closed.")
        else:
             # Đôi khi loop đã tự đóng do lỗi trước đó
             if not main_loop.is_closed():
                  main_loop.close()
                  log.info("Main event loop closed.")


    log.info("--- Program finished ---")