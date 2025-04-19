from .collect import BLUETOOTH_DETECTOR
import asyncio

class CHECK_BLUETOOTH:
    def __init__(self):
        self.bluetooth_detector = BLUETOOTH_DETECTOR()
        self.loop = asyncio.get_event_loop()
        self.devices_name = ["IMU1"]
        self.run_times = 1

    def insystem_find_missing_imu(self):
        # thís function is used to check if the IMU devices are not connected - return these devices in list
        # in case: False : Bluetooth or IMU is off
        # in case: True : all devices are connected
        # in case: list : some devices are not connected


        insystem_devices = self.loop.run_until_complete(self.bluetooth_detector.INSYSTEM_CHECK())
        # print (f"IFMI: {insystem_devices}")
        if insystem_devices == None:
            # print("Check the system Bluetooth & IMUs is on.")
            return False
        else: 
            # check all the missing devices & return 
            res = []
            for device in self.devices_name:
                if device not in insystem_devices:
                    res.append(device)
            
            if len(res) == 0:
                # No missing devices
                return True
            else: 
                # Some missing devices
                return res
        # print(f"Missing devices: {res}")
        return res
            

    def outsystem_find_missiing_imu(self, missing_devices):
        # this function is used to check if the unconnected IMUs are availabel 
        # in case: False : Bluetooth or IMU is off
        # in case: True : all devices are availabel
        # in case: list : some devices are availabel will be return - the others are not

        availabel_devices = self.loop.run_until_complete(self.bluetooth_detector.OUTSYSTEM_CHECK())
        if availabel_devices == None: 
            return False
        else:
            res = []
            for device in missing_devices:
                if device in availabel_devices:
                    res.append(device)       

            return res
    
    
    def run(self):
        # this function is used to run the whole system fucntion
        # step1: check insystem devices:
        #    Case 0: False: Bluetooth or IMU is off
        #    Case 1: True: all devices are connected
        #    Case 2: List: some devices are not connected

        # step2: check outsystem devices
        #    Case 0: False: Bluetooth or IMU is off
        #    Case 1: True: all devices are available
        #    Case 2: List: some devices are available - the others are not

        # step3: check the available devices and connect to them
        
        now_missing_devices = self.insystem_find_missing_imu()
        if now_missing_devices == False: 
            return "Run: Maybe the Bluetooth of system or IMUs is off! Make it on."
        elif now_missing_devices == True: 
            return "Run: All IMUs are connected!"   
        else:
            # print(f"Run: {now_missing_devices} are not connected!")

            available_missing_devices = self.outsystem_find_missiing_imu(now_missing_devices)

            if available_missing_devices == False: 
                return "Run: Maybe the Bluetooth of system or IMUs is off! Make it on."
            else: 

                if len(available_missing_devices) != 0:
                    print(f"Run: {available_missing_devices} are available!")
                    
                    if len(available_missing_devices) != len(now_missing_devices):
                        # in case some devices are not availabel Print it out to recheck the bluetooth mode
                        for device in now_missing_devices:
                            if device not in available_missing_devices:
                                print(f"Run: {device} is NOT available!")
                    else: 
                        print("Run: All IMUs are available!")
                
                # automatically connect to the unconnected devices but available
                
                self.loop.run_until_complete(self.bluetooth_detector.AUTO_CONNECT(available_missing_devices))
                
                return "In {now_missing_devices} devices, {available_missing_devices} are available and being reconnected! Retap to check result!"

                    
                

if __name__ == "__main__":
    check_bluetooth = CHECK_BLUETOOTH()
    check_bluetooth.run()