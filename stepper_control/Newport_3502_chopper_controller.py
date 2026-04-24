import sys
import pythonnet
import os
#add .net assembly
import clr
from numpy.ma.core import count

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
CHOPPER_DRIVER_DIR = os.path.join(MODULE_DIR, "x86_drivers")
if CHOPPER_DRIVER_DIR not in sys.path:
    sys.path.append(CHOPPER_DRIVER_DIR)
clr.AddReference("CmdLib3502")
from NewFocus.ChopperApp import *

import System

class newport_3502_chopper_controller():
    def __init__(self, verbose=True, auto_open=True):
        self.verbose = bool(verbose)
        self.driver = CmdLib3502(True)
        self.device_res = self.driver.DiscoverDevices()
        self.devicekey = self.driver.GetFirstDeviceKey()
        self.is_open = False

        if self.verbose:
            print(
                "Chopper discovery: "
                f"device_res={self.device_res!r}, key={self.devicekey!r}"
            )

        if auto_open:
            self.open_dr()

    def open_dr(self):
        if not self.devicekey:
            if self.verbose:
                print("No valid chopper device key; skipping Open().")
            self.is_open = False
            return False

        suc = bool(self.driver.Open(self.devicekey))
        self.is_open = suc
        if self.verbose:
            if suc:
                print(f"Open() succeeded for key {self.devicekey!r}.")
            else:
                print(f"Open() failed for key {self.devicekey!r}.")
        return suc

    def close(self):
        if not self.devicekey:
            if self.verbose:
                print("No valid chopper device key; skipping Close().")
            self.is_open = False
            return False

        # CmdLib3502 exposes 'Close' (capital C), not 'close'.
        suc = bool(self.driver.Close(self.devicekey))
        if self.verbose and (not suc):
            print("unable to close chopper device\n")
        if suc:
            self.is_open = False
        return suc

    def _ensure_ready(self):
        if not self.devicekey:
            raise RuntimeError(
                "No chopper device key discovered. Check USB/power/driver state."
            )
        if not self.is_open:
            raise RuntimeError(
                "Chopper device is not open. Call open_dr() after discovery."
            )

    def setPhase(self, phaseDelay):
        self._ensure_ready()
        self.driver.SetPhaseDelay(self.devicekey, phaseDelay)

    def setSync(self, sync):
        self._ensure_ready()
        #1-3 for sync but 
        self.driver.SetSync(self.devicekey, sync)

    def setSynthFreq(self, freq):
        self._ensure_ready()
        res = self.driver.SetSynthFrequency(self.devicekey, freq)
        return res

    def setMode(self, mode):
        self._ensure_ready()
        res = self.driver.SetMode(self.devicekey, mode)
        return res

    def getMode(self, mode_ref):
        self._ensure_ready()
        res = self.driver.GetMode(self.devicekey,mode_ref)
        return res

    def getSynthFreq(self):
        self._ensure_ready()
        result = self.driver.GetSynthFrequency(self.devicekey, 0)
        if isinstance(result, tuple):
            updated_freq = result[1]
        else:
            updated_freq = result

        return updated_freq

    def getPhase(self):
        self._ensure_ready()
        result = self.driver.GetPhaseDelay(self.devicekey, 0.0)
        updated_phase = result[1]

        return updated_phase

    def setWheelType(self, wheeltype):
        self._ensure_ready()
        result = self.driver.SetChoppingWheel(self.devicekey, wheeltype)
        return result

    def getWheelType(self):
        self._ensure_ready()
        result = self.driver.GetChoppingWheel(self.devicekey, 0)
        wheel = result[1]
        return wheel
if __name__ == "__main__":
    cont = newport_3502_chopper_controller(verbose=True, auto_open=True)
    if not cont.devicekey:
        print(
            "No chopper device discovered. If using VSCode/default terminal, "
            "run chopper.py which can auto-relaunch under a 32-bit interpreter."
        )
        sys.exit(2)

    cont.setSynthFreq(400)
    cont.setWheelType(4)
    wheel = cont.getWheelType()
    res = cont.getPhase()
    print(wheel)
