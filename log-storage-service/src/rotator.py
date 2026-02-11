"""Post-rotation operations: compression and retention enforcement."""

import gzip
import os
import shutil
from datetime import datetime, timezone, timedelta

from src.config import Config


def compress_file(filepath: str) -> str:
    """Gzip-compress a file in place. Returns the .gz path."""
    gz_path = filepath + ".gz"
    with open(filepath, "rb") as f_in, gzip.open(gz_path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    os.remove(filepath)
    return gz_path


def get_rotated_files(log_dir: str, log_filename: str) -> list[str]:
    """List rotated files sorted oldest-first (lexicographic on timestamp suffix)."""
    prefix = log_filename + "."
    rotated = []
    for name in os.listdir(log_dir):
        if name.startswith(prefix) and name != log_filename:
            rotated.append(name)
    rotated.sort()
    return rotated


def parse_rotation_timestamp(filename: str, log_filename: str) -> datetime | None:
    """Extract the rotation timestamp from a rotated filename. Returns None on failure."""
    prefix = log_filename + "."
    if not filename.startswith(prefix):
        return None
    suffix = filename[len(prefix):]
    # Strip .gz if present
    if suffix.endswith(".gz"):
        suffix = suffix[:-3]
    try:
        return datetime.strptime(suffix, "%Y%m%d_%H%M%S_%f").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def enforce_retention(config: Config, time_func=None) -> list[str]:
    """Delete files that are too old or exceed max count. Returns list of deleted filenames."""
    now_func = time_func or (lambda: datetime.now(timezone.utc))
    now = now_func()
    deleted = []

    rotated = get_rotated_files(config.log_dir, config.log_filename)

    # Age-based purge
    cutoff = now - timedelta(days=config.max_age_days)
    survivors = []
    for name in rotated:
        ts = parse_rotation_timestamp(name, config.log_filename)
        if ts is not None and ts < cutoff:
            os.remove(os.path.join(config.log_dir, name))
            deleted.append(name)
        else:
            survivors.append(name)

    # Count-based purge on survivors (oldest first, they're already sorted)
    while len(survivors) > config.max_file_count:
        name = survivors.pop(0)
        os.remove(os.path.join(config.log_dir, name))
        deleted.append(name)

    return deleted
