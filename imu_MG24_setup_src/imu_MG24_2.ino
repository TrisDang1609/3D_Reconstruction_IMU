//Server Code
#define RF_SW_PW_PIN PB5
#define RF_SW_PIN PB4

#include <LSM6DS3.h>
#include <Wire.h>
#include <SPI.h>
#include <MadgwickAHRS.h>

//Create a instance of class LSM6DS3
LSM6DS3 myIMU(I2C_MODE, 0x6A);    //I2C device address 0x6A
Madgwick filter;

// IMU data variables
float aX, aY, aZ, gX, gY, gZ;
const float accelerationThreshold = 2.5; // threshold of significant in G's
const int numSamples = 119;
int samplesRead = numSamples;

// Constants for processing
const float sampleFreqHz = 100.0f;  // Sample frequency in Hz
const float gravityMss = 9.81f;     // Gravity in meters per second squared

// Variables for orientation and acceleration processing
float rotationMatrix[3][3];       // Rotation/orientation matrix
float linearAccel[3];             // Gravity-free acceleration
float q0 = 1.0f, q1 = 0.0f, q2 = 0.0f, q3 = 0.0f; // Quaternion

bool notification_enabled = false;

void setup() {
  pinMode(LED_BUILTIN, OUTPUT);
  digitalWrite(LED_BUILTIN, LED_BUILTIN_INACTIVE);
  Serial.begin(115200);
  Serial.println("Silicon Labs BLE with IMU data streaming - MG24_2");

  // turn on the antenna function
  pinMode(RF_SW_PW_PIN, OUTPUT);
  digitalWrite(RF_SW_PW_PIN, HIGH);

  delay(100);

  // HIGH -> Use external antenna / LOW -> Use built-in antenna
  pinMode(RF_SW_PIN, OUTPUT);
  digitalWrite(RF_SW_PIN, LOW);
  
  // Initialize IMU with enhanced configuration
  if (myIMU.begin() != 0) {
    Serial.println("IMU Device error");
  } else {
    Serial.println("IMU initialized successfully");
    
    // Configure IMU with specific settings
    myIMU.settings.gyroEnabled = true;
    myIMU.settings.accelEnabled = true;
    myIMU.settings.accelRange = 4;      // +/- 4g
    myIMU.settings.accelSampleRate = 104;  // 104 Hz
    myIMU.settings.accelBandWidth = 50;   // 50 Hz bandwidth
    myIMU.settings.gyroRange = 2000;     // +/- 2000 dps
    myIMU.settings.gyroSampleRate = 104;  // 104 Hz
    myIMU.settings.gyroBandWidth = 50;    // 50 Hz bandwidth
  }
  
  // Initialize Madgwick filter
  filter.begin(sampleFreqHz);
}

void loop() {
  if (notification_enabled) {
    // Process and send IMU data notification
    send_imu_notification();
  }
  delay(10); // Adjust delay to match desired frequency
}

// Calculate rotation matrix from quaternion
void calculateRotationMatrix() {
  // Convert quaternion to rotation matrix
  rotationMatrix[0][0] = 1 - 2 * q2 * q2 - 2 * q3 * q3;
  rotationMatrix[0][1] = 2 * q1 * q2 - 2 * q3 * q0;
  rotationMatrix[0][2] = 2 * q1 * q3 + 2 * q2 * q0;
  
  rotationMatrix[1][0] = 2 * q1 * q2 + 2 * q3 * q0;
  rotationMatrix[1][1] = 1 - 2 * q1 * q1 - 2 * q3 * q3;
  rotationMatrix[1][2] = 2 * q2 * q3 - 2 * q1 * q0;
  
  rotationMatrix[2][0] = 2 * q1 * q3 - 2 * q2 * q0;
  rotationMatrix[2][1] = 2 * q2 * q3 + 2 * q1 * q0;
  rotationMatrix[2][2] = 1 - 2 * q1 * q1 - 2 * q2 * q2;
}

// Calculate linear acceleration by removing gravity
void calculateLinearAcceleration() {
  // Convert acceleration from g to m/s²
  float accMss[3] = {
    aX * gravityMss,
    aY * gravityMss,
    aZ * gravityMss
  };
  
  // Define gravity vector in Earth frame (0, 0, g)
  float gravityEarth[3] = {0, 0, gravityMss};
  
  // Rotate gravity vector to body frame using transpose of rotation matrix
  float gravityBody[3] = {0, 0, 0};
  for (int i = 0; i < 3; i++) {
    for (int j = 0; j < 3; j++) {
      // Using transpose of rotation matrix (same as inverse for orthogonal matrices)
      gravityBody[i] += rotationMatrix[j][i] * gravityEarth[j];
    }
  }
  
  // Subtract gravity from acceleration to get linear acceleration
  linearAccel[0] = accMss[0] - gravityBody[0];
  linearAccel[1] = accMss[1] - gravityBody[1];
  linearAccel[2] = accMss[2] - gravityBody[2];
}

static void ble_initialize_gatt_db();
static void ble_start_advertising();

// Changed device name for unique identification
static const uint8_t advertised_name[] = "IMU2";  // Changed name for unique identification
static uint16_t gattdb_session_id;
static uint16_t generic_access_service_handle;
static uint16_t name_characteristic_handle;
static uint16_t my_service_handle;
static uint16_t led_control_characteristic_handle;
static uint16_t notify_characteristic_handle;

/**************************************************************************/ /**
 * Bluetooth stack event handler
 * Called when an event happens on BLE the stack
 *
 * @param[in] evt Event coming from the Bluetooth stack
 *****************************************************************************/
void sl_bt_on_event(sl_bt_msg_t *evt) {
  switch (SL_BT_MSG_ID(evt->header)) {
    // -------------------------------
    // This event indicates the device has started and the radio is ready.
    // Do not call any stack command before receiving this boot event!
    case sl_bt_evt_system_boot_id:
      {
        Serial.println("BLE stack booted");

        // Initialize the application specific GATT table
        ble_initialize_gatt_db();

        // Start advertising
        ble_start_advertising();
        Serial.println("BLE advertisement started");
      }
      break;

    // -------------------------------
    // This event indicates that a new connection was opened
    case sl_bt_evt_connection_opened_id:
      Serial.println("BLE connection opened");
      break;

    // -------------------------------
    // This event indicates that a connection was closed
    case sl_bt_evt_connection_closed_id:
      Serial.println("BLE connection closed");
      // Restart the advertisement
      ble_start_advertising();
      Serial.println("BLE advertisement restarted");
      break;

    // -------------------------------
    // This event indicates that the value of an attribute in the local GATT
    // database was changed by a remote GATT client
    case sl_bt_evt_gatt_server_attribute_value_id:
      // Check if the changed characteristic is the LED control
      if (led_control_characteristic_handle == evt->data.evt_gatt_server_attribute_value.attribute) {
        Serial.println("LED control characteristic data received");
        // Check the length of the received data
        if (evt->data.evt_gatt_server_attribute_value.value.len == 0) {
          break;
        }
        // Get the received byte
        uint8_t received_data = evt->data.evt_gatt_server_attribute_value.value.data[0];
        // Turn the LED on/off according to the received data
        // If we receive a '0' - turn the LED off
        // If we receive a '1' - turn the LED on
        if (received_data == 0x00) {
          digitalWrite(LED_BUILTIN, LED_BUILTIN_INACTIVE);
          Serial.println("LED off");
        } else if (received_data == 0x01) {
          Serial.println("LED on");
          digitalWrite(LED_BUILTIN, LED_BUILTIN_ACTIVE);
        }
      }
      break;

    // -------------------------------
    // This event is received when a GATT characteristic status changes
    case sl_bt_evt_gatt_server_characteristic_status_id:
      // If the 'Notify' characteristic has been changed
      if (evt->data.evt_gatt_server_characteristic_status.characteristic == notify_characteristic_handle) {
        // The client just enabled the notification - send notification of the current state
        if (evt->data.evt_gatt_server_characteristic_status.client_config_flags & sl_bt_gatt_notification) {
          Serial.println("change notification enabled");
          notification_enabled = true;
        } else {
          Serial.println("change notification disabled");
          notification_enabled = false;
        }
      }
      break;

    // -------------------------------
    // Default event handler
    default:
      break;
  }
}

/**************************************************************************/ /**
 * Sends a BLE notification with IMU data to the client if notifications are enabled 
 *****************************************************************************/
static void send_imu_notification() {
  // Read the IMU data
  aX = myIMU.readFloatAccelX();
  aY = myIMU.readFloatAccelY();
  aZ = myIMU.readFloatAccelZ();
  gX = myIMU.readFloatGyroX();
  gY = myIMU.readFloatGyroY();
  gZ = myIMU.readFloatGyroZ();
   
  // Update Madgwick filter with new data
  filter.updateIMU(gX, gY, gZ, aX, aY, aZ);
  
  // Get quaternion from filter - using the correct API
  q0 = filter.getPitch();
  q1 = filter.getRoll();
  q2 = filter.getYaw();
  q3 = 0.0f; // Since the Madgwick library might not directly expose q3
  
  // Calculate rotation matrix from quaternion
  calculateRotationMatrix();
  
  // Calculate linear acceleration (remove gravity)
  calculateLinearAcceleration();
  
  // Create a buffer to hold only linearAccel and quaternion data (28 bytes total)
  uint8_t buffer[28];
  
  // Only copy the necessary values to the buffer
  // First the quaternion data (16 bytes)
  memcpy(&buffer[0], &q0, 4);
  memcpy(&buffer[4], &q1, 4);
  memcpy(&buffer[8], &q2, 4);
  memcpy(&buffer[12], &q3, 4);
  
  // Then linear acceleration data (12 bytes)
  memcpy(&buffer[16], &linearAccel[0], 4);
  memcpy(&buffer[20], &linearAccel[1], 4);
  memcpy(&buffer[24], &linearAccel[2], 4);
  
  // Send the notification with reduced IMU data
  sl_status_t sc = sl_bt_gatt_server_notify_all(notify_characteristic_handle,
                                               sizeof(buffer),
                                               (const uint8_t *)&buffer);
  if (sc == SL_STATUS_OK) {
    // Print only quaternion and linear acceleration values in decimal format
    // Serial.print("MG24_2 | Q: ");
    Serial.print(q0, 3); Serial.print(',');
    Serial.print(q1, 3); Serial.print(',');
    Serial.print(q2, 3); Serial.print(',');
    Serial.print(q3, 3);
    
    // Serial.print(" | LinAcc: ");
    Serial.print(linearAccel[0], 3); Serial.print(',');
    Serial.print(linearAccel[1], 3); Serial.print(',');
    Serial.print(linearAccel[2], 3); Serial.println();
  }
}

/**************************************************************************/ /**
 * Starts BLE advertisement
 * Initializes advertising if it's called for the first time
 *****************************************************************************/
static void ble_start_advertising() {
  static uint8_t advertising_set_handle = 0xff;
  static bool init = true;
  sl_status_t sc;

  if (init) {
    // Create an advertising set
    sc = sl_bt_advertiser_create_set(&advertising_set_handle);
    app_assert_status(sc);

    // Set advertising interval to 100ms
    sc = sl_bt_advertiser_set_timing(
      advertising_set_handle,
      160,  // minimum advertisement interval (milliseconds * 1.6)
      160,  // maximum advertisement interval (milliseconds * 1.6)
      0,    // advertisement duration
      0);   // maximum number of advertisement events
    app_assert_status(sc);

    init = false;
  }

  // Generate data for advertising
  sc = sl_bt_legacy_advertiser_generate_data(advertising_set_handle, sl_bt_advertiser_general_discoverable);
  app_assert_status(sc);

  // Start advertising and enable connections
  sc = sl_bt_legacy_advertiser_start(advertising_set_handle, sl_bt_advertiser_connectable_scannable);
  app_assert_status(sc);
}

/**************************************************************************/ /**
 * Initializes the GATT database
 * Creates a new GATT session and adds certain services and characteristics
 *****************************************************************************/
static void ble_initialize_gatt_db() {
  sl_status_t sc;
  // Create a new GATT database
  sc = sl_bt_gattdb_new_session(&gattdb_session_id);
  app_assert_status(sc);

  // Add the Generic Access service to the GATT DB
  const uint8_t generic_access_service_uuid[] = { 0x00, 0x18 };
  sc = sl_bt_gattdb_add_service(gattdb_session_id,
                                sl_bt_gattdb_primary_service,
                                SL_BT_GATTDB_ADVERTISED_SERVICE,
                                sizeof(generic_access_service_uuid),
                                generic_access_service_uuid,
                                &generic_access_service_handle);
  app_assert_status(sc);

  // Add the Device Name characteristic to the Generic Access service
  // The value of the Device Name characteristic will be advertised
  const sl_bt_uuid_16_t device_name_characteristic_uuid = { .data = { 0x00, 0x2A } };
  sc = sl_bt_gattdb_add_uuid16_characteristic(gattdb_session_id,
                                              generic_access_service_handle,
                                              SL_BT_GATTDB_CHARACTERISTIC_READ,
                                              0x00,
                                              0x00,
                                              device_name_characteristic_uuid,
                                              sl_bt_gattdb_fixed_length_value,
                                              sizeof(advertised_name) - 1,
                                              sizeof(advertised_name) - 1,
                                              advertised_name,
                                              &name_characteristic_handle);
  app_assert_status(sc);

  // Start the Generic Access service
  sc = sl_bt_gattdb_start_service(gattdb_session_id, generic_access_service_handle);
  app_assert_status(sc);

  // Add my BLE service to the GATT DB
  // UUID is unchanged but we'll use our unique device ID
  const uuid_128 my_service_uuid = {
    .data = { 0x24, 0x12, 0xb5, 0xcb, 0xd4, 0x60, 0x80, 0x0c, 0x15, 0xc3, 0x9b, 0xa9, 0xac, 0x5a, 0x8a, 0xde }
  };
  sc = sl_bt_gattdb_add_service(gattdb_session_id,
                                sl_bt_gattdb_primary_service,
                                SL_BT_GATTDB_ADVERTISED_SERVICE,
                                sizeof(my_service_uuid),
                                my_service_uuid.data,
                                &my_service_handle);
  app_assert_status(sc);

  // Add the 'LED Control' characteristic to the Blinky service
  // UUID: 5b026510-4088-c297-46d8-be6c736a087a
  const uuid_128 led_control_characteristic_uuid = {
    .data = { 0x7a, 0x08, 0x6a, 0x73, 0x6c, 0xbe, 0xd8, 0x46, 0x97, 0xc2, 0x88, 0x40, 0x10, 0x65, 0x02, 0x5b }
  };
  uint8_t led_char_init_value = 0;
  sc = sl_bt_gattdb_add_uuid128_characteristic(gattdb_session_id,
                                               my_service_handle,
                                               SL_BT_GATTDB_CHARACTERISTIC_READ | SL_BT_GATTDB_CHARACTERISTIC_WRITE,
                                               0x00,
                                               0x00,
                                               led_control_characteristic_uuid,
                                               sl_bt_gattdb_fixed_length_value,
                                               1,                            // max length
                                               sizeof(led_char_init_value),  // initial value length
                                               &led_char_init_value,         // initial value
                                               &led_control_characteristic_handle);

  // Start the Blinky service
  sc = sl_bt_gattdb_start_service(gattdb_session_id, my_service_handle);
  app_assert_status(sc);

  // Add the 'Notify' characteristic to my BLE service
  // UUID: 61a885a4-41c3-60d0-9a53-6d652a70d29c
  const uuid_128 btn_report_characteristic_uuid = {
    .data = { 0x9c, 0xd2, 0x70, 0x2a, 0x65, 0x6d, 0x53, 0x9a, 0xd0, 0x60, 0xc3, 0x41, 0xa4, 0x85, 0xa8, 0x61 }
  };
  uint8_t notify_char_init_value = 0;
  sc = sl_bt_gattdb_add_uuid128_characteristic(gattdb_session_id,
                                               my_service_handle,
                                               SL_BT_GATTDB_CHARACTERISTIC_READ | SL_BT_GATTDB_CHARACTERISTIC_NOTIFY,
                                               0x00,
                                               0x00,
                                               btn_report_characteristic_uuid,
                                               sl_bt_gattdb_fixed_length_value,
                                               28,                              // max length - reduced to 28 bytes (7 floats × 4 bytes)
                                               sizeof(notify_char_init_value),  // initial value length
                                               &notify_char_init_value,         // initial value
                                               &notify_characteristic_handle);

  // Start my BLE service
  sc = sl_bt_gattdb_start_service(gattdb_session_id, my_service_handle);
  app_assert_status(sc);

  // Commit the GATT DB changes
  sc = sl_bt_gattdb_commit(gattdb_session_id);
  app_assert_status(sc);
}

#ifndef BLE_STACK_SILABS
#error "This example is only compatible with the Silicon Labs BLE stack. Please select 'BLE (Silabs)' in 'Tools > Protocol stack'."
#endif
