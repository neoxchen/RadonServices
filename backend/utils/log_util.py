import datetime
import sys


def debug(message, flush=False):
    _print("DEBUG", message, flush=flush)


def info(message, flush=False):
    _print("INFO", message, flush=flush)


def warning(message, flush=False):
    _print("WARN", message, flush=flush, file=sys.stderr)


def error(message, flush=False):
    _print("ERROR", message, flush=flush, file=sys.stderr)


def request(uid, message, flush=False):
    _print("REQUEST", f"[{uid}] {message}", flush=flush)


def _print(level, message, flush=False, file=sys.stdout):
    now = datetime.datetime.now()
    date = f"{now.month}/{now.day}"
    time = f"{now.hour:02d}:{now.minute:02d}:{now.second:02d}"
    message = f"[{level}] {date} {time} >> {message}"
    # Print to console & save to cache
    print(message, flush=True, file=file)
