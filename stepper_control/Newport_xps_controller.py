import sys
import pythonnet
import os
import clr
# add .net assembly
MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
XPS_DRIVER_DIR = os.path.join(MODULE_DIR, "x86_drivers")
if XPS_DRIVER_DIR not in sys.path:
    sys.path.append(XPS_DRIVER_DIR)
clr.AddReference("Newport.XPS.CommandInterface")
from CommandInterfaceXPS import *


class Newport_Xps_controller():
    def __init__(self, address, port):
        self.my_XPS, res = self.XPS_Open(address, port)
        print(res)

    def XPS_Open(self, address, port):
        # Create XPS interface
        myXPS = XPS()
        # Open a socket
        timeout = 1000
        result = myXPS.OpenInstrument(address, port, timeout)
        if result == 0:
            print('Open ', address, ":", port, " => Successful")
        else:
            print('Open ', address, ":", port, " => failure ", result)
        return myXPS, result

    def XPS_Close(self):
        if self.my_XPS is None:
            print("XPS instance not created\n")
            return None
        res = self.my_XPS.CloseInstrument()
        return res

    def move_abs(self, x):
        # NOTE: This API wrapper is legacy and depends on Newport's .NET method
        # signatures for MoveAbsolute(group, axis/position...).
        res = self.my_XPS.MoveAbsolute(500, x)
        print(res)

    def test_socket(self):
        res = self.my_XPS.SocketsStatusGet()
        print(res)

if __name__ == "__main__":
    cont = Newport_Xps_controller("169.254.255.255", 80)
    cont.test_socket()
    cont.move_abs(100)
    res = cont.XPS_Close()
    print(res)

