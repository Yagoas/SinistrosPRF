import pandas as pd
import sys
from pathlib import Path
from sqlalchemy import text

sys.path.append(str(Path(__file__).parent.parent))

from utils import db_manager, get_etl_logger, ProcessTimer

logger = get_etl_logger("GoldLoader")


class GoldLoader:
    def __init__(self):
        self.logger = logger
        self.engine = db_manager.engine

    def load_table(
        self,
        df: pd.DataFrame,
        schema: str,
        table_name: str,
        if_exists: str = "replace",
        chunksize: int = 50000,
    ):
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
                self.logger.info(
                    f"[GOLD LOAD] Iniciando carga de {total:,} registros em blocos de {chunksize}..."
                )

                # Carrega em blocos
                for i, start in enumerate(range(0, total, chunksize)):
                    end = start + chunksize
                    chunk = df.iloc[start:end]
                    chunk.to_sql(
                        table_name,
                        self.engine,
                        schema=schema,
                        if_exists="append",
                        index=False,
                    )
                    self.logger.info(
                        f"[GOLD LOAD] → Bloco {i+1} ({len(chunk):,} registros) inserido com sucesso."
                    )

                self.logger.info(
                    f"Tabela {full_name} carregada com sucesso! ({total:,} registros)"
                )

            except Exception as e:
                self.logger.error(f"❌ Erro ao carregar tabela {full_name}: {e}")

    def load_all(self, dims: dict, fato: pd.DataFrame, schema: str = "dw"):
        """
        Carrega todas as dimensões e tabela fato no schema dw (Data Warehouse).
        Cria o schema se não existir.
        """
        self.logger.info("🔹 Criando schema dw (Data Warehouse) se não existir...")

        with self.engine.begin() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))

        self.logger.info(
            f"🔹 Carregando {len(dims)} dimensões e 1 tabela fato no schema {schema}..."
        )

        # Carregar dimensões
        for dim_name, dim_df in dims.items():
            table_name = dim_name  # dim_data, dim_local, etc.
            self.load_table(
                dim_df, schema=schema, table_name=table_name, if_exists="replace"
            )

        # Carregar fato
        self.load_table(
            fato, schema=schema, table_name="fato_sinistros", if_exists="replace"
        )

        self.logger.info(
            "✅ Todas as tabelas da camada gold foram carregadas com sucesso!"
        )
