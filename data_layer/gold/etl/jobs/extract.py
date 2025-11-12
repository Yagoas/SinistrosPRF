from typing import Optional
import pandas as pd
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from utils import get_etl_logger, ProcessTimer, ETLStats, db_manager


class GoldExtractor:
    def __init__(
        self,
        silver_schema: str = "silver",
        silver_table: str = "tb_sinistros_silver",
    ):
        self.logger = get_etl_logger(self.__class__.__name__)
        self.stats = ETLStats(self.logger)
        self.db = db_manager
        self.silver_schema = silver_schema
        self.silver_table = silver_table

    def extract_silver_table(
        self, columns: Optional[list] = None, where_clause: Optional[str] = None
    ) -> pd.DataFrame:
        cols = "*" if not columns else ", ".join(columns)
        query = f"SELECT {cols} FROM {self.silver_schema}.{self.silver_table}"
        if where_clause:
            query += f" WHERE {where_clause}"

        self.logger.info(
            f"🔹 Extraindo da Silver: {self.silver_schema}.{self.silver_table}"
        )
        with ProcessTimer(self.logger, "Extração Silver"):
            df = self.db.execute_query(query)

        self.logger.info(f"✅ Extraídos {len(df):,} registros")
        self.stats.add_stat("registros_extraidos", len(df))
        return df
