"""Fake JetBot-style SDK for controlling a simulated robot.

Prints actions and maintains an internal state variable (current_state),
used by the sensor publisher to produce varying data.
"""

from robot.logger import get_logger

logger = get_logger("robot_sdk")


class RobotSDK:
    """Simulates a JetBot robot's drive system.

    Each method prints the action and updates an internal ``current_state``
    value that the sensor publisher reports.
    """

    def __init__(self) -> None:
        self.current_state: float = 50.0

    def forward(self) -> None:
        logger.info("SDK: moving FORWARD")
        self.current_state = min(100.0, self.current_state + 5.0)

    def backward(self) -> None:
        logger.info("SDK: moving BACKWARD")
        self.current_state = max(0.0, self.current_state - 5.0)

    def left(self) -> None:
        logger.info("SDK: turning LEFT")
        self.current_state = max(0.0, self.current_state - 2.0)

    def right(self) -> None:
        logger.info("SDK: turning RIGHT")
        self.current_state = min(100.0, self.current_state + 2.0)

    def stop(self) -> None:
        logger.info("SDK: STOP")
        # state drifts slowly toward center when stopped
        self.current_state = 50.0
