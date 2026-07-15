import os
import sys
import logging
import multiprocessing
from logging.handlers import RotatingFileHandler
from datetime import datetime
from dotenv import load_dotenv
import configparser

class LogCapture:
    _instance = None
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init_logger()
        return cls._instance
    def _init_logger(self):
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)
        self.logger = logging.getLogger("project_527")
        self.logger.setLevel(logging.INFO)
        if multiprocessing.current_process().name == 'MainProcess':
            try:
                file_handler = RotatingFileHandler(
                    'logs/app.log',
                    maxBytes=10*1024*1024,
                    backupCount=5,
                    encoding='utf-8'
                )
                console_handler = logging.StreamHandler(sys.stdout)
                formatter = logging.Formatter('%(asctime)s   [%(levelname)s]   %(message)s')
                file_handler.setFormatter(formatter)
                console_handler.setFormatter(formatter)
                self.logger.addHandler(file_handler)
                self.logger.addHandler(console_handler)
            except Exception:
                pass
    def log(self, message, level="INFO"):
        if level == "ERROR":
            log_level = logging.ERROR
        elif level == "WARN":
            log_level = logging.WARNING
        else:
            log_level = logging.INFO
        self.logger.log(log_level, message)

logger = LogCapture()

class ConfigReader:
    _instance = None
    _config = {}
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load_config()
        return cls._instance
    def _load_config(self):
        load_dotenv()
        secret = os.getenv('SECRET_KEY')
        if not secret:
            raise ValueError("[ERROR] SECRET_KEY not found in .env file. System cannot start for security reasons.")
        ini_path = os.path.join(os.path.dirname(__file__), 'server.ini')
        config = configparser.ConfigParser(default_section='DEFAULT')
        if os.path.exists(ini_path):
            config.read(ini_path)
        def get_str(key, default=''):
            return config.get('DEFAULT', key, fallback=default).strip()
        def get_int(key, default=0):
            try:
                return config.getint('DEFAULT', key, fallback=default)
            except ValueError:
                return default
        self._config = {
            'SECRET_KEY': secret,
            'DATABASE_PATH': os.getenv('DATABASE_PATH', 'app.db'),
            'ALGORITHM': os.getenv('ALGORITHM', 'HS256'),
            'ADMIN_STATIC_CODE': os.getenv('ADMIN_STATIC_CODE', '111'),
            'ACCESS_TOKEN_EXPIRE_MINUTES': get_int('ACCESS_TOKEN_EXPIRE_MINUTES', 30),
            'UPLOAD_DIR': get_str('UPLOAD_DIR', 'uploads'),
            'ALLOWED_EXTENSIONS': [ext.strip().lower() for ext in get_str('ALLOWED_EXTENSIONS', '.xlsx,.db').split(',') if ext.strip()],
            'LOG_FILE_PATH': get_str('LOG_FILE_PATH', 'logs/app.log'),
            'DEFAULT_PAGE_SIZE': get_int('DEFAULT_PAGE_SIZE', 50),
            'HOST': get_str('HOST', '0.0.0.0'),
            'PORT': get_int('PORT', 8000),
            'TIMEOUT_KEEP_ALIVE': get_int('TIMEOUT_KEEP_ALIVE', 5),
            'LIMIT_CONCURRENCY': get_int('LIMIT_CONCURRENCY', 50),
            'LIMIT_MAX_REQUESTS': get_int('LIMIT_MAX_REQUESTS', 10000),
            'MAINTENANCE_MODE': False
        }
    def get(self, key, default=None):
        return self._config.get(key, default)
    def set_maintenance(self, value):
        self._config['MAINTENANCE_MODE'] = value

config = ConfigReader()