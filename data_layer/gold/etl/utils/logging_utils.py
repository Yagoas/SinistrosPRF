import os
import time
import logging
from datetime import datetime

LOG_DIR = os.path.join("logs", "gold")
os.makedirs(LOG_DIR, exist_ok=True)

def get_etl_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(f"GOLD_ETL.{name}")
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        log_file = os.path.join(LOG_DIR, f"etl_gold_{datetime.now().strftime('%Y%m%d')}.log")

        fh = logging.FileHandler(log_file, encoding="utf-8")
        ch = logging.StreamHandler()

        fmt = logging.Formatter("[%(asctime)s] [GOLD ETL] [%(levelname)s] %(name)s: %(message)s")
        fh.setFormatter(fmt)
        ch.setFormatter(fmt)

        logger.addHandler(fh)
        logger.addHandler(ch)
    return logger


class ProcessTimer:
    def __init__(self, logger: logging.Logger, label: str):
        self.logger = logger
        self.label = label

    def __enter__(self):
        self.start = time.time()
        self.logger.info(f"⏳ Iniciando: {self.label}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = time.time() - self.start
        self.logger.info(f"✅ Finalizado: {self.label} (tempo: {elapsed:.2f}s)")


class ETLStats:
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.stats = {}

    def add_stat(self, name: str, value):
        self.stats[name] = value
        self.logger.info(f"📊 {name}: {value}")

    def summary(self):
        self.logger.info("Resumo das métricas ETL:")
        for k, v in self.stats.items():
            self.logger.info(f"  - {k}: {v}")
