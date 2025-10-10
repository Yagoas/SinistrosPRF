"""
Utilitários para logging dos processos ETL
"""

import logging
import logging.handlers
from datetime import datetime
from pathlib import Path
import sys


class ETLLogger:
    """Configurador de logs para processos ETL"""

    def __init__(self, name: str, log_dir: str = "logs", level: int = logging.INFO):
        self.name = name
        self.log_dir = Path(log_dir)
        self.level = level
        self.logger = None
        self._setup_logger()

    def _setup_logger(self):
        """Configura o logger"""
        # Criar diretório de logs se não existir
        self.log_dir.mkdir(exist_ok=True)

        # Configurar logger
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(self.level)

        # Limpar handlers existentes
        self.logger.handlers.clear()

        # Formato das mensagens
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # Handler para arquivo (rotating)
        log_file = self.log_dir / f"{self.name}_{datetime.now().strftime('%Y%m%d')}.log"
        file_handler = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"  # 10MB
        )
        file_handler.setLevel(self.level)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

        # Handler para console
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(self.level)
        console_formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s", datefmt="%H:%M:%S"
        )
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)

        # Evitar propagação para o root logger
        self.logger.propagate = False

    def get_logger(self) -> logging.Logger:
        """Retorna o logger configurado"""
        return self.logger

    def log_dataframe_info(self, df, name: str = "DataFrame"):
        """Log informações de um DataFrame"""
        self.logger.info(f"{name} - Shape: {df.shape}")
        self.logger.info(
            f"{name} - Memória: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB"
        )
        self.logger.info(f"{name} - Colunas: {list(df.columns)}")

        # Log valores nulos
        null_counts = df.isnull().sum()
        if null_counts.sum() > 0:
            null_info = null_counts[null_counts > 0].to_dict()
            self.logger.warning(f"{name} - Valores nulos: {null_info}")

    def log_process_start(self, process_name: str, **kwargs):
        """Log início de processo"""
        self.logger.info("=" * 60)
        self.logger.info(f"INICIANDO PROCESSO: {process_name}")
        self.logger.info("=" * 60)

        for key, value in kwargs.items():
            self.logger.info(f"{key}: {value}")

    def log_process_end(self, process_name: str, duration: float, **kwargs):
        """Log fim de processo"""
        self.logger.info("-" * 60)
        self.logger.info(f"FINALIZANDO PROCESSO: {process_name}")
        self.logger.info(f"Duração: {duration:.2f} segundos")

        for key, value in kwargs.items():
            self.logger.info(f"{key}: {value}")

        self.logger.info("-" * 60)

    def log_error_details(self, error: Exception, context: str = ""):
        """Log detalhado de erro"""
        self.logger.error(f"ERRO {context}: {type(error).__name__}")
        self.logger.error(f"Mensagem: {str(error)}")

        # Log stack trace se disponível
        import traceback

        stack_trace = traceback.format_exc()
        if stack_trace and stack_trace != "NoneType: None\n":
            self.logger.error(f"Stack trace:\n{stack_trace}")


class ProcessTimer:
    """Timer para processos ETL"""

    def __init__(
        self,
        logger: logging.Logger,
        process_name: str,
        show_progress: bool = False,
        total_items: int = None,
    ):
        self.logger = logger
        self.process_name = process_name
        self.start_time = None
        self.show_progress = show_progress
        self.total_items = total_items
        self.processed_items = 0

    def __enter__(self):
        self.start_time = datetime.now()
        if self.total_items:
            self.logger.info(
                f"⏱️  Iniciando '{self.process_name}' às {self.start_time.strftime('%H:%M:%S')} - {self.total_items:,} itens para processar"
            )
        else:
            self.logger.info(
                f"⏱️  Iniciando '{self.process_name}' às {self.start_time.strftime('%H:%M:%S')}"
            )
        return self

    def update_progress(self, items_processed: int = 1):
        """Atualiza progresso durante a execução"""
        if not self.show_progress or not self.total_items:
            return

        self.processed_items += items_processed

        if self.processed_items % max(1, self.total_items // 20) == 0:  # Log a cada 5%
            percentage = (self.processed_items / self.total_items) * 100
            elapsed = (datetime.now() - self.start_time).total_seconds()

            if self.processed_items > 0:
                estimated_total = (elapsed / self.processed_items) * self.total_items
                remaining = estimated_total - elapsed
                self.logger.info(
                    f"🔄 Progresso: {self.processed_items:,}/{self.total_items:,} ({percentage:.1f}%) - "
                    f"Tempo restante estimado: {remaining:.0f}s"
                )

    def __exit__(self, exc_type, exc_val, exc_tb):
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()

        if exc_type is None:
            if self.total_items:
                rate = self.total_items / duration if duration > 0 else 0
                self.logger.info(
                    f"✅ '{self.process_name}' concluído em {duration:.2f}s - {rate:.0f} itens/s"
                )
            else:
                self.logger.info(
                    f"✅ '{self.process_name}' concluído em {duration:.2f}s"
                )
        else:
            self.logger.error(f"❌ '{self.process_name}' falhou após {duration:.2f}s")
            self.logger.error(f"Erro: {exc_type.__name__}: {exc_val}")


class ETLStats:
    """Coletor de estatísticas do ETL"""

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.stats = {}

    def add_stat(self, key: str, value):
        """Adiciona estatística"""
        self.stats[key] = value
        self.logger.info(f"📊 {key}: {value}")

    def increment_counter(self, key: str, amount: int = 1):
        """Incrementa contador"""
        if key not in self.stats:
            self.stats[key] = 0
        self.stats[key] += amount
        self.logger.info(f"🔢 {key}: {self.stats[key]}")

    def log_summary(self):
        """Log resumo das estatísticas"""
        self.logger.info("📈 RESUMO DAS ESTATÍSTICAS:")
        for key, value in self.stats.items():
            self.logger.info(f"  {key}: {value}")

    def get_stats(self) -> dict:
        """Retorna estatísticas"""
        return self.stats.copy()

    def get_all_stats(self) -> dict:
        """Alias para get_stats - para compatibilidade"""
        return self.get_stats()


def setup_etl_logging(process_name: str, log_level: str = "INFO") -> ETLLogger:
    """Função helper para configurar logging de ETL"""
    level_map = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }

    level = level_map.get(log_level.upper(), logging.INFO)

    # Definir diretório de logs relativo ao projeto
    log_dir = Path(__file__).parent.parent.parent / "logs"

    return ETLLogger(process_name, str(log_dir), level)


# Função para criar logger padrão
def get_etl_logger(name: str) -> logging.Logger:
    """Retorna logger configurado para ETL (apenas console, sem arquivos)"""
    # Criar logger simples apenas para console
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Limpar handlers existentes
    logger.handlers.clear()

    # Handler apenas para console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s", datefmt="%H:%M:%S"
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # Evitar propagação para o root logger
    logger.propagate = False

    return logger