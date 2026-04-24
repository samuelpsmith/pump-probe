#=========================================================================
# Newport Proprietary and Confidential    Newport Corporation 2014
#
# No part of this file in any format, with or without modification 
# shall be used, copied or distributed without the express written 
# consent of Newport Corporation.
# 
# Description: This is a Python Script to access CONEX-PP library
#==========================================================================
import os
#==========================================================================
#Initialization Start
#The script within Initialization Start and Initialization End is needed for properly 
#initializing Command Interface DLL for CONEX-PP instrument.
#The user should copy this code as is and specify correct paths here.
import sys

# Command Interface DLL can be found here.
print("Adding location of Newport.CONEXPP.CommandInterface.dll to sys.path")
MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
CONEX_DRIVER_DIR = os.path.join(MODULE_DIR, "x86_drivers", "connex_pp_Bin")
if CONEX_DRIVER_DIR not in sys.path:
    sys.path.append(CONEX_DRIVER_DIR)

# The CLR module provide functions for interacting with the underlying 
# .NET runtime
import clr
# Add reference to assembly and import names from namespace
clr.AddReference("Newport.CONEXPP.CommandInterface")
from CommandInterfaceConexPP import *

import System
#==========================================================================
