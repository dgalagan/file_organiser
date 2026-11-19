from collections.abc import Sized, Collection
import shutil
from utils.path import get_drive

def bytes_to_gb(n_bytes) -> int:
    return int(n_bytes / 1024 ** 3)

def ensure_not_empty(obj: Sized, name: str = '') -> None:
    if len(obj) == 0:
        raise ValueError(f"{name} is empty")

def ensure_valid(value: object, allowed: Collection) -> None:
    if value not in allowed:
        raise ValueError(f"Unknown value: {value}. Expected one of: {allowed}")

def ensure_drive_space(path: str, required: int) -> None:
    free = shutil.disk_usage(path).free
    if required >= free:
        path_drive = get_drive(path)
        raise RuntimeError(f"Not enough free space on {path_drive}: required {bytes_to_gb(required)} GB, free {bytes_to_gb(free)} GB")
