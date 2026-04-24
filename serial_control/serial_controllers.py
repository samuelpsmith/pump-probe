from time import sleep

from pyvisa import ResourceManager

import MC2000B_COMMAND_LIB as mclib


class NewportSerialStageController:
    """
    Serial/VISA motion controller wrapper for Newport stage-style command sets.
    """

    def __init__(self, visa_address, device_number="01", timeout=2000):
        self.visa_address = visa_address
        self.device_number = device_number
        self.rm = ResourceManager()
        self.device = self.rm.open_resource(visa_address)
        self.device.timeout = timeout
        self.device.baud_rate = 115200

    def list_resources(self):
        print(self.rm.list_resources())

    def get_rm(self):
        return self.rm

    def get_device(self):
        return self.device

    def move(self, mm):
        mvt = self.get_relative_move_time(mm)
        self.device.write(self.device_number + "PR" + str(mm))
        self.sleep(mvt)

    def move_absolute(self, position):
        self.device.write(self.device_number + "PA" + str(position))

    def reset_to_move_state(self, homing_speed):
        self.device.write("01OH" + str(homing_speed))
        self.device.write(self.device_number + "OR")

    def reset_device(self):
        self.device.write(self.device_number + "RS")

    def get_state(self):
        print(self.device.query(self.device_number + "TS?"))

    def get_error(self):
        print(self.device.query(self.device_number + "TE"))

    def sleep(self, duration):
        self.device.query(self.device_number + "TS?", duration)

    def sleep_thread(self, duration):
        sleep(duration)

    def close(self):
        self.rm.close()

    def delay_write(self, command, duration):
        try:
            self.device.query(command, duration)
        except Exception:
            print("caught write timeout")

    def set_velocity(self, velocity):
        self.device.write(self.device_number + "VA" + str(velocity))

    def get_velocity(self):
        return self.convert_query(self.device.query(self.device_number + "VA?"))

    def set_acceleration(self, acceleration):
        self.device.write(self.device_number + "AC" + str(acceleration))

    def get_acceleration(self):
        return self.convert_query(self.device.query(self.device_number + "AC?"))

    def get_relative_move_time(self, mm):
        return self.convert_query(self.device.query(self.device_number + "PT" + str(mm)))

    def get_position(self):
        return self.convert_query(self.device.query(self.device_number + "TP"))

    def raster(self, accel, velocity, length, periods):
        self.set_velocity(velocity)
        self.set_acceleration(accel)
        self.move_absolute(0.0)
        pos = self.get_position()
        if pos != 0:
            sleep(self.get_relative_move_time(pos))
        for _ in range(0, periods):
            self.move(length)
            self.move(-length)

    def reset_to_zero(self):
        self.move_absolute(0.0)

    def to_config(self):
        self.reset_device()
        self.device.write(self.device_number + "PW1")

    def leave_config(self):
        self.device.write(self.device_number + "PW0")

    def set_max_velocity(self, max_velocity):
        self.set_velocity(max_velocity)

    @staticmethod
    def convert_query(q_str):
        t = q_str[3:10]
        return float(t)


class ThorlabsMC2000BChopperController:
    """
    Wrapper around the MC2000B DLL API (via ctypes in MC2000B_COMMAND_LIB).
    """

    def __init__(self, serial_num, baud_rate=115200, timeout=2):
        self.hdl = mclib.MC2000BOpen(serial_num, baud_rate, timeout)

    def set_frequency(self, freq):
        mclib.MC2000BSetFrequency(self.hdl, freq)

    def set_phase(self, phase):
        mclib.MC2000BSetPhase(self.hdl, phase)

    def set_to_external_ref(self):
        mclib.MC2000BSetReference(self.hdl, 1)

    def set_to_internal_ref(self):
        mclib.MC2000BSetReference(self.hdl, 0)

    def set_reference_out_to_actual(self):
        mclib.MC2000BSetReferenceOutput(self.hdl, 1)

    def set_reference_out_to_target(self):
        mclib.MC2000BSetReferenceOutput(self.hdl, 0)

    def set_harmonic_multiplier(self, hm):
        mclib.MC2000BSetHarmonicMultiplier(self.hdl, hm)

    def set_harmonic_divider(self, hd):
        mclib.MC2000BSetHarmonicDivider(self.hdl, hd)


# Backward-compatible aliases (legacy names used in older scripts).
newport_stepper_controller = NewportSerialStageController
thor_chopper_controller = ThorlabsMC2000BChopperController
