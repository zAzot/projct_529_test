import os
from datetime import datetime
from dotenv import load_dotenv

LOG_FILE_PATH = "logs/app.log"

class LogCapture:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            os.makedirs("logs", exist_ok=True)
            cls._instance.log_file = LOG_FILE_PATH
        return cls._instance

    def log(self, message, level="INFO"):
        prefixes = {"INFO": "[INFO]  ", "ERROR": "[ERROR] ", "REQW": "[REQW]  ", "WARN": "[WARN]  "}
        prefix = prefixes.get(level, f"[{level}] ")
        formatted = f"{prefix}{message}"
        print(formatted)
        try:
            with open(self.log_file, "a", encoding="utf-8") as f:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                f.write(f"{timestamp} {formatted}\n")
        except Exception:
            pass

logger = LogCapture()

class ConfigReader:
    _instance = None
    _config = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            load_dotenv()
            secret = os.getenv('SECRET_KEY')
            if not secret:
                raise ValueError("[ERROR] SECRET_KEY not found in .env file. System cannot start for security reasons.")
            allowed_extensions_str = os.getenv('ALLOWED_EXTENSIONS', '.xlsx,.db')
            allowed_extensions = [ext.strip().lower() for ext in allowed_extensions_str.split(',')]
            cls._instance._config = {
                'SECRET_KEY': secret,
                'DATABASE_PATH': os.getenv('DATABASE_PATH', 'app.db'),
                'ALGORITHM': os.getenv('ALGORITHM', 'HS256'),
                'ACCESS_TOKEN_EXPIRE_MINUTES': int(os.getenv('ACCESS_TOKEN_EXPIRE_MINUTES', '30')),
                'ADMIN_STATIC_CODE': os.getenv('ADMIN_STATIC_CODE', '111'),
                'UPLOAD_DIR': os.getenv('UPLOAD_DIR', 'uploads'),
                'ALLOWED_EXTENSIONS': allowed_extensions,
                'MAINTENANCE_MODE': False
            }
        return cls._instance

    def get(self, key, default=None):
        return self._config.get(key, default)

    def set_maintenance(self, value):
        self._config['MAINTENANCE_MODE'] = value

config = ConfigReader()