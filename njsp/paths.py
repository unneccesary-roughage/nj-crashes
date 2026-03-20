from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent  # go up from /njsp to root
DATA_DIR = BASE_DIR / "data"
ALERTS_DIR = BASE_DIR / "alerts"
EXPORT_DIR = BASE_DIR / "exports"
MAIN_DB = DATA_DIR / "njsp_fatal.sqlite"
REMOVED_DB = DATA_DIR / "njsp_fatal_removed.sqlite"