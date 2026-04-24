import os
import sys
MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
if MODULE_DIR not in sys.path:
    sys.path.append(MODULE_DIR)
from ConexPP_Functions import *

#*************************************************
# Constants
#*************************************************
ADDRESS = 1
DISPLAY_FLAG = 1


class ConexPPRasterController:
    def __init__(self, instrument_key):
        print('Instrument COM Port =>', instrument_key)

        # Create a CONEX-PP instance
        self.myPP = ConexPP()

        # Open communication with the smart motor
        self.ret = self.myPP.OpenInstrument(instrument_key);
    def raster(self, nbLoops, position1, position2, acc, vel):
        ExecuteMotionCycle(self.myPP, ADDRESS, nbLoops, position1, position2, acc, vel)
    def start_raster_window(self):
        startPosWin(self.myPP, ADDRESS)


# Backward-compatible alias (legacy class name).
Conex_pp_controller = ConexPPRasterController
