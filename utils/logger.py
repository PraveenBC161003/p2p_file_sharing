import sys
import datetime

class Colors:
    RESET   = "\033[0m"
    RED     = "\033[31m"
    GREEN   = "\033[32m"
    YELLOW  = "\033[33m"
    BLUE    = "\033[34m"
    CYAN    = "\033[36m"
    MAGENTA = "\033[35m"
    DIM     = "\033[2m"

def supports_color():
    if not hasattr(sys.stdout, "isatty"):
        return False
    return sys.stdout.isatty()

_COLOR_ENABLED = supports_color()
_DEBUG = False

def enable_debug():
    global _DEBUG
    _DEBUG = True

class Logger:
    def __init__(self, name: str):
        self.name = name

    def _color(self, text, color):
        return f"{color}{text}{Colors.RESET}" if _COLOR_ENABLED else text

    def _format(self, level, message, color):
        t = datetime.datetime.now().strftime("%H:%M:%S")
        return (
            f"{self._color(f'[{t}]', Colors.DIM)} "
            f"{self._color(f'[{self.name}]', Colors.BLUE)} "
            f"{self._color(f'[{level}]', color)} "
            f"{message}"
        )

    def info(self, msg):    print(self._format("INFO",  msg, Colors.CYAN))
    def success(self, msg): print(self._format("OK",    msg, Colors.GREEN))
    def warn(self, msg):    print(self._format("WARN",  msg, Colors.YELLOW))
    def warning(self, msg): self.warn(msg)   # alias so both spellings work
    def error(self, msg):   print(self._format("ERROR", msg, Colors.RED))
    def debug(self, msg):
        if _DEBUG:
            print(self._format("DEBUG", msg, Colors.MAGENTA))

def get_logger(name: str) -> Logger:
    return Logger(name)