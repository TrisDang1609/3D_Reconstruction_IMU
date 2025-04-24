from coreBleak import BLUETOOTH_DETECTOR

import asyncio 



class BLUETOOTH():

    def __init__(self):
        self.needy_devices = ["IMU1", "IMU2", "IMU3", "IMU4", "IMU5", "IMU6"]
        self.bluetooth_detector = BLUETOOTH_DETECTOR(self.needy_devices)
        self.loop = asyncio.get_event_loop()
        self.final_devices = {}

    def run(self): 
        # Check all the needy devices in system & disconnect them
        self.loop.run_until_complete(self.bluetooth_detector.INSYSTEM_SCANNING())
        
        self.final_devices = self.loop.run_until_complete(self.bluetooth_detector.OUTSYSTEM_SCANNING())
        if len(self.final_devices) == len(self.needy_devices):
            print("RUN: All needy devices are available")
            return "All needy devices are available"
        
        elif len(self.final_devices) < len(self.needy_devices):
            
            output = f"Some needy devices are not available: "
            for device in self.needy_devices:
                if device not in self.final_devices.keys():
                    output += f"{device} "
            
            output += "\nPlease check the devices & system bluetooth and reconnect them"

            print (f"RUN: {output}")
            return output

if __name__ == "__main__":
    bluetooth = BLUETOOTH()
    bluetooth.run()
