from from_root import from_root
import logging
from datetime import datetime
import os


LOG_FILE = f"{datetime.now().strftime('%m-%d-%Y-%H-%M-%S')}.log"

logs_dir = 'logs'

logs_path = os.path.join(from_root(), logs_dir, LOG_FILE)

os.makedirs(logs_dir, exist_ok=True)

logging.basicConfig(
    filename=logs_path,
    level=logging.DEBUG,
    format="[ %(asctime)s ] %(name)s - %(levelname)s- %(message)s)",
)