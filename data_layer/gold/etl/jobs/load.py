import pandas as pd
from sqlalchemy import text
from data_layer.gold.etl.utils import db_manager, get_etl_logger, ProcessTimer

logger = get_etl_logger("GoldLoader")

class GoldLoader:
    def __init__(self):
        self.logger = logger
        self.engine = db_manager.engine

    def load_table(self, df: pd.DataFrame, schema: str, table_name: str, if_exists: str = "replace", chunksize: int = 50000):
        """
        Carrega um DataFrame para o Postgres em blocos para evitar estouro de memória.
        """
        full_name = f"{schema}.{table_name}"

        with ProcessTimer(self.logger, f"Carregando {full_name}"):
            try:
                if if_exists == "replace":
                    with self.engine.begin() as conn:
                        conn.execute(text(f"DROP TABLE IF EXISTS {full_name} CASCADE;"))

                total = len(df)
                self.logger.info(f"[GOLD LOAD] Iniciando carga de {total:,} registros em blocos de {chunksize}...")

                # Carrega em blocos
                for i, start in enumerate(range(0, total, chunksize)):
                    end = start + chunksize
                    chunk = df.iloc[start:end]
                    chunk.to_sql(table_name, self.engine, schema=schema, if_exists="append", index=False)
                    self.logger.info(f"[GOLD LOAD] → Bloco {i+1} ({len(chunk):,} registros) inserido com sucesso.")

                self.logger.info(f"Tabela {full_name} carregada com sucesso! ({total:,} registros)")

            except Exception as e:
                self.logger.error(f"❌ Erro ao carregar tabela {full_name}: {e}")
