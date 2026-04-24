#=========================================================================
# Newport Proprietary and Confidential    Newport Corporation 2014
#
# No part of this file in any format, with or without modification 
# shall be used, copied or distributed without the express written 
# consent of Newport Corporation.
#==========================================================================
import os
import sys
from threading import Thread
MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
if MODULE_DIR not in sys.path:
	sys.path.append(MODULE_DIR)
from ConexPP_Header import *

#*************************************************
# Constants
#*************************************************
INSTRUMENT_KEY = "COM3"
ADDRESS = 1
DISPLAY_FLAG = 1

_tk = None
root = None
text_var_pos = None
label_pos = None


def _ensure_gui():
	global _tk, root, text_var_pos, label_pos
	if root is not None and text_var_pos is not None:
		return True
	try:
		import tkinter as tk
	except Exception as exc:
		print("CONEX GUI disabled (tkinter unavailable):", exc)
		return False

	_tk = tk
	root = tk.Tk()
	root.geometry("400x150")
	root.title("Conex_PP_pos")

	text_var_pos = tk.StringVar()
	text_var_pos.set("Waiting")

	label_pos = tk.Label(
		root,
		textvariable=text_var_pos,
		anchor=tk.CENTER,
		bg="lightblue",
		height=3,
		width=30,
		bd=3,
		font=("Arial", 24, "bold"),
		cursor="hand2",
		fg="red",
		padx=15,
		pady=15,
		justify=tk.CENTER,
		relief=tk.RAISED,
		underline=0,
		wraplength=250,
	)
	label_pos.pack(pady=20)
	return True

def startPosWin(PP, address):
	if _ensure_gui():
		root.mainloop()
	else:
		print("startPosWin skipped: tkinter GUI unavailable in this interpreter.")

#*************************************************
# Procedure to get the controller version
#*************************************************
def GetControllerVersion (PP, axis, flag):
	result, version, errString = PP.VE(axis) 
	if flag == 1:	
		if result == 0:
			print('Controller version => ',version)
		else:
			print('Error=>',errString)
	return result, version

#*************************************************
# Procedure to get the current position
#*************************************************
def GetCurrentPosition (PP, axis, flag):
	result, position, errString = PP.TP(axis) 
	if flag == 1:
		if result == 0 :
			doNothing = None
			#print('current position=>', position)
		else:
			print('GetCurrentPosition Error=>',errString)
	return result, position
	
#*************************************************
# Procedure to get the Right limit
#*************************************************
def GetPositiveSoftwareLimit (PP, axis, flag):	
	result, limit, errString = PP.SR_Get(axis) 
	if flag == 1:
		if result == 0 :
			print('positive software limit=>', limit)
		else:
			print('GetPositiveSoftwareLimit Error=>',errString)
	return result, limit
		
#*************************************************
# Procedure to get the Left limit
#*************************************************
def GetNegativeSoftwareLimit (PP, axis, flag):	
	result, limit, errString = PP.SL_Get(axis) 
	if flag == 1:
		if result == 0 :
			print('negative software limit=>', limit)
		else:
			print('GetNegativeSoftwareLimit Error=>',errString)
	return result, limit
	
#*************************************************
# Procedure to list the connected devices  
# from NSTRUCT server
#*************************************************	
def GetDeviceList (PP):
	print('-----------------------------------------------------------')
	# Get the list of detected devices
	strDeviceArray = PP.GetDevices() 
	nbItems = len(strDeviceArray)
	if nbItems > 0 :
		for i in range(nbItems):
			print(i+1, ') ', strDeviceArray[i])
	else:
		print('No detected devices')
	print('-----------------------------------------------------------')
	

#*************************************************
# Procedure to perform an absolute motion
#*************************************************
def AbsoluteMove (PP, address, targetPosition, flag):
	if (flag == 1):
		print('Moving to position ' , targetPosition)
		
	# Execute an absolute motion	
	result, errStringMove = PP.PA_Set(address,targetPosition)	
	
	# Wait the end of motion
	WaitEndOfMotion(PP, address)
	return result
	
#*************************************************
# Procedure to check if the controller is READY
#  1 = Controller is "Not Referenced"
# -1 = Controller is not in "Not Referenced" state
#*************************************************
def IsNotReferenced (PP, address):
	# Get controller state
	result, errorCode, controllerState, errString = PP.TS(address) 
	print("controllerState = ", controllerState)
	if result == 0 :
		if  (controllerState == "0A") | \
			(controllerState == "0B") | \
			(controllerState == "0C") | \
			(controllerState == "0D") | \
			(controllerState == "0E") | \
			(controllerState == "0F") | \
			(controllerState == "10") | \
			(controllerState == "11"):
			return 1
		else:
			return -1

#*************************************************
# Procedure to check if the controller is READY
#  1 = Controller is ready
# -1 = Controller is not Ready
#*************************************************
def IsReady (PP, address):
	# Get controller state
	result, errorCode, controllerState, errString = PP.TS(address) 
	if result == 0 :
		if  (controllerState == "32") | \
			(controllerState == "33") | \
			(controllerState == "34") | \
			(controllerState == "35"):
			return 1
		else:
			return -1
			
#*************************************************
# Procedure to wait the end of current motion
#*************************************************
def WaitEndOfMotion (PP, address):
	print("WaitEndOfMotion ...")

	# Get controller status
	result, errorCode, ControllerState, errString = PP.TS(address) 
	if result != 0 :
		print('TS Error=>',errString)
		
	while ControllerState == "28":
		# Get controller status
		result, errorCode, ControllerState, errString = PP.TS(address)
		updatePosWindowText(str(GetCurrentPosition(PP, address, 1)[1]))
		if root is not None:
			root.update()

		if result != 0 :
			print('TS Error=>',errString)
			
#*************************************************
# Procedure to wait the end of current motion
#*************************************************
def WaitEndOfHomeSearch (PP, address):
	print("WaitEndOfHomeSearch ...")
	
	# Get controller status
	result, errorCode, ControllerState, errString = PP.TS(address) 
	if result != 0 :
		print('TS Error=>',errString)
	else:		
		while ControllerState == "1E":
			# Get controller status
			result, errorCode, ControllerState, errString = PP.TS(address)
			if result != 0 :
				print('TS Error=>',errString)

#*************************************************
# Procedure to execute motion cycle(s)
#*************************************************
def ExecuteMotionCycle (PP, address, nbLoops, step):
	# Initialization
	result = 0
	displayFlag = 1

	# Home search if needed
	if ((IsReady(PP, address) == -1) & (IsNotReferenced(PP, address) == 1)):
		result, errString = PP.OR(address)	
		if result == 0 :
			# Wait the end of home search
			print("waiting on home search\n")
			WaitEndOfHomeSearch(PP, address)
		
	# Controller must be READY
	if (IsReady(PP, address) == 1):

		# Get current position = first position
		result, position1 = GetCurrentPosition(PP, address, displayFlag)		
		if result == 0:		
		
			# Calculate the second position
			position2 = (float)(position1) + step
				
			print('------------- Cycle in progress --------------')
			
			# Cycles
			for i in range(nbLoops): 				
				
				# Displacement 1 : go to position #2
				result = AbsoluteMove(PP, address,position2,displayFlag)			
				
				# Get current position
				result, currentPosition = GetCurrentPosition(PP, address, displayFlag)			
				
				# Displacement 2 : go to position #1
				result = AbsoluteMove(PP, address,position1,displayFlag)
				
				# Get current position
				result, currentPosition = GetCurrentPosition(PP, address, displayFlag)
				
				print('-------------', i + 1 ,' Cycle(s) completed --------------')

			print('End of cycle')
		else:
			print("Error while getting position")
	else:
		print('Controller is not in READY state ...')


def ExecuteMotionCycle(PP, address, nbLoops, position1, position2, acc, vel):
	# Initialization
	result = 0
	displayFlag = 1

	# Home search if needed
	if ((IsReady(PP, address) == -1) & (IsNotReferenced(PP, address) == 1)):
		result, errString = PP.OR(address)
		if result == 0:
			# Wait the end of home search
			print("waiting on home search\n")
			WaitEndOfHomeSearch(PP, address)

	# Controller must be READY
	if (IsReady(PP, address) == 1):
		PP.AC_Set(address, acc)
		PP.VA_Set(address, vel)
		# Get current position = first position
		result, positionNow = GetCurrentPosition(PP, address, displayFlag)
		print("position at start: " + str(positionNow))
		AbsoluteMove(PP, address, position1, displayFlag)

		if result == 0:

			# Calculate the second position


			print('------------- Cycle in progress --------------')

			# Cycles
			for i in range(nbLoops):
				# Displacement 1 : go to position #2
				result = AbsoluteMove(PP, address, position2, displayFlag)

				# Get current position
				result, currentPosition = GetCurrentPosition(PP, address, displayFlag)

				# Displacement 2 : go to position #1
				result = AbsoluteMove(PP, address, position1, displayFlag)

				# Get current position
				result, currentPosition = GetCurrentPosition(PP, address, displayFlag)

				print('-------------', i + 1, ' Cycle(s) completed --------------')

			print('End of cycle')
		else:
			print("Error while getting position")
	else:
		print('Controller is not in READY state ...')


#*************************************************
# Start procedure to execute sample #1
#*************************************************

def StartSample1():
	# Instrument Initialization
	print('Instrument COM port =>', INSTRUMENT_KEY)

	# Create CONEX-PP object
	myPP = CONEX-PP()

	# Get list of devices
	GetDeviceList(myPP)

	# Open the communication with the smart motor
	ret = myPP.OpenInstrument(INSTRUMENT_KEY);

	# Get controller revision information
	result, version = GetControllerVersion(myPP, ADDRESS, DISPLAY_FLAG) 

	# Get positive software limit
	result, rightlimit = GetPositiveSoftwareLimit(myPP, ADDRESS, DISPLAY_FLAG)

	# Get negative software limit
	result, leftlimit = GetNegativeSoftwareLimit(myPP, ADDRESS, DISPLAY_FLAG)

	# Get current position
	result, position = GetCurrentPosition(myPP, ADDRESS, DISPLAY_FLAG)
	
	# Close communication	
	myPP.CloseInstrument();	
	print('End of script')
	
		
#*************************************************
# Start procedure to execute sample #2
#*************************************************
def StartSample2():
	# Instrument Initialization
	print('Instrument COM Port =>', INSTRUMENT_KEY)

	# Create a CONEX-PP instance
	myPP = CONEX-PP()

	# Open communication with the smart motor
	ret = myPP.OpenInstrument(INSTRUMENT_KEY);
	print('StartSample2 OpenInstrument =>', ret)

	# Execute motion cycle(s)
	nbLoops = 3
	motionStep = 2.00
	ExecuteMotionCycle(myPP, ADDRESS, nbLoops, motionStep)	

	# Close communication
	myPP.CloseInstrument();
	print('End of script')
def updatePosWindowText(text):
	if text_var_pos is not None:
		text_var_pos.set(text)
