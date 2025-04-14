"""Настройки подключения"""

import os
from dotenv import load_dotenv
import logging

load_dotenv()

# --- Конфигурация ---
DATABASE_CONFIG = {
    'host': os.environ.get('DB_HOST', 'localhost'),
    'port': int(os.environ.get('DB_PORT', 5433)),
    'database': os.environ.get('DB_NAME', 'hakaton'),
    'user': os.environ.get('DB_USER', 'postgres'),
    'password': os.environ.get('DB_PASS', '347620')
}
print(DATABASE_CONFIG)

UPLOAD_LOG_DIRECTORY = './uploaded_logs'
LOCAL_LOG_DIRECTORY = 'log2db/local_logs'

EXPORT_DIR = "exported_data"

os.makedirs(EXPORT_DIR, exist_ok=True)

ALLOWED_EXTENSIONS = {'log'}
BATCH_SIZE = 1000
DEBUG_MODE = os.environ.get('DEBUG', 'False').lower() == 'true'

logging.basicConfig(level=logging.DEBUG if DEBUG_MODE else logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
