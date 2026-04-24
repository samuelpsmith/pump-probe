import warnings

warnings.warn(
    "serial_contorllers.py is deprecated (spelling error). "
    "Use serial_controllers.py instead.",
    DeprecationWarning,
    stacklevel=2,
)

from serial_controllers import *  # noqa: F401,F403

