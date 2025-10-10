"""
Utilitários para conexão com banco de dados PostgreSQL
"""

import os
import logging
from typing import Optional
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

class DatabaseConfig:
    """Configurações do banco de dados"""

    def __init__(self):
        # Configuração padrão do banco de dados
        self.host = os.getenv("DB_HOST", "localhost")
        self.port = os.getenv("DB_PORT", "5432")
        self.database = os.getenv("DB_NAME", "sinistros_prf")
        self.username = os.getenv("DB_USER", "admin")
        self.password = os.getenv("DB_PASSWORD", "admin123")

    @property
    def connection_string(self) -> str:
        """Retorna string de conexão para SQLAlchemy"""
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"

    @property
    def psycopg2_params(self) -> dict:
        """Retorna parâmetros para psycopg2"""
        return {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.username,
            "password": self.password,
        }


class DatabaseManager:
    """Gerenciador de conexões com banco de dados"""

    def __init__(self, config: Optional[DatabaseConfig] = None):
        self.config = config or DatabaseConfig()
        self.engine: Optional[Engine] = None
        self.logger = logging.getLogger(__name__)

    def get_engine(self) -> Engine:
        """Retorna engine SQLAlchemy (singleton)"""
        if self.engine is None:
            try:
                self.engine = create_engine(
                    self.config.connection_string,
                    pool_size=10,
                    max_overflow=20,
                    pool_timeout=30,
                    pool_recycle=3600,
                    echo=False,  # Set True para debug SQL
                )
                self.logger.info("Engine SQLAlchemy criada com sucesso")
            except Exception as e:
                self.logger.error(f"Erro ao criar engine: {e}")
                raise
        return self.engine

    def test_connection(self) -> bool:
        """Testa conexão com o banco"""
        try:
            engine = self.get_engine()
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                self.logger.info("Teste de conexão bem-sucedido")
                return True
        except Exception as e:
            self.logger.error(f"Erro na conexão: {e}")
            return False

    def execute_query(self, query: str, params: Optional[dict] = None) -> pd.DataFrame:
        """Executa query e retorna DataFrame"""
        try:
            engine = self.get_engine()
            with engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                # Converter resultado para DataFrame
                columns = result.keys()
                data = result.fetchall()
                df = pd.DataFrame(data, columns=columns)

            self.logger.info(
                f"Query executada com sucesso. {len(df)} registros retornados"
            )
            return df
        except Exception as e:
            self.logger.error(f"Erro ao executar query: {e}")
            raise

    def execute_sql(self, sql: str, params: Optional[dict] = None) -> int:
        """Executa SQL (INSERT, UPDATE, DELETE) e retorna linhas afetadas"""
        try:
            engine = self.get_engine()
            with engine.connect() as conn:
                with conn.begin():
                    result = conn.execute(text(sql), params or {})
                    affected_rows = result.rowcount
                    self.logger.info(f"SQL executado. {affected_rows} linhas afetadas")
                    return affected_rows
        except Exception as e:
            self.logger.error(f"Erro ao executar SQL: {e}")
            raise

    def bulk_insert_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str = "sinistros",
        if_exists: str = "append",
        chunksize: int = 1000,
    ) -> int:
        """Insere DataFrame em lote no banco"""
        try:
            engine = self.get_engine()
            rows_inserted = df.to_sql(
                name=table_name,
                con=engine,
                schema=schema,
                if_exists=if_exists,
                index=False,
                chunksize=chunksize,
                method="multi",
            )
            self.logger.info(f"DataFrame inserido na tabela {schema}.{table_name}")
            return len(df)
        except Exception as e:
            self.logger.error(f"Erro ao inserir DataFrame: {e}")
            raise

    def get_table_count(self, table_name: str, schema: str = "sinistros") -> int:
        """Retorna quantidade de registros na tabela"""
        try:
            query = f"SELECT COUNT(*) as count FROM {schema}.{table_name}"
            result = self.execute_query(query)
            count = result.iloc[0]["count"]
            self.logger.info(f"Tabela {schema}.{table_name} possui {count} registros")
            return count
        except Exception as e:
            self.logger.error(f"Erro ao contar registros: {e}")
            return 0

    def truncate_table(self, table_name: str, schema: str = "sinistros") -> bool:
        """Limpa todos os dados da tabela"""
        try:
            sql = f"TRUNCATE TABLE {schema}.{table_name} RESTART IDENTITY CASCADE"
            self.execute_sql(sql)
            self.logger.info(f"Tabela {schema}.{table_name} truncada com sucesso")
            return True
        except Exception as e:
            self.logger.error(f"Erro ao truncar tabela: {e}")
            return False

    def get_connection_info(self) -> dict:
        """Retorna informações da conexão"""
        try:
            query = """
            SELECT 
                current_database() as database,
                current_user as user,
                version() as version,
                now() as current_time
            """
            result = self.execute_query(query)
            return result.to_dict("records")[0]
        except Exception as e:
            self.logger.error(f"Erro ao obter informações da conexão: {e}")
            return {}

    def close(self):
        """Fecha conexões"""
        if self.engine:
            self.engine.dispose()
            self.logger.info("Conexões fechadas")


# Instância global
db_manager = DatabaseManager()


if __name__ == "__main__":
    # Teste de conexão
    import sys

    logging.basicConfig(level=logging.INFO)

    print("Testando conexão com PostgreSQL...")

    if db_manager.test_connection():
        print("✅ Conexão bem-sucedida!")

        # Exibir informações da conexão
        info = db_manager.get_connection_info()
        print(f"Database: {info.get('database')}")
        print(f"User: {info.get('user')}")
        print(f"Version: {info.get('version', 'N/A')[:50]}...")

    else:
        print("❌ Falha na conexão!")
        sys.exit(1)
