"""
ETL Load Job - Carga de Dados: Silver Layer Lakehouse
"""

import sys
import pandas as pd
from pathlib import Path
import warnings
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# Adicionar o diretório utils ao path
sys.path.append(str(Path(__file__).parent.parent))

from utils import get_etl_logger, ProcessTimer, ETLStats, db_manager

warnings.filterwarnings("ignore")


class SilverLakehouseLoader:
    """Carregador de dados para a camada Silver Lakehouse (tabela única)"""

    def __init__(self):
        self.logger = get_etl_logger(self.__class__.__name__)
        self.stats = ETLStats(self.logger)
        self.db = db_manager

        # Nome da tabela única
        self.table_name = "tb_sinistros_silver"
        self.schema_name = "sinistros"

        # Verificar conexão
        if not self.db.test_connection():
            raise ConnectionError("Não foi possível conectar ao PostgreSQL")

        self.logger.info("SilverLakehouseLoader inicializado")

    def _prepare_for_postgres(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepara DataFrame para inserção no PostgreSQL
        Converte tipos pandas para tipos nativos Python
        """
        total_cols = len(df.columns)
        self.logger.info(
            f"⚙️  Preparando {len(df):,} registros com {total_cols} colunas para PostgreSQL"
        )

        with ProcessTimer(
            self.logger,
            "Preparação para PostgreSQL",
            show_progress=True,
            total_items=total_cols,
        ):
            df_prepared = df.copy()
            processed_cols = 0

            # Converter tipos pandas para tipos compatíveis com PostgreSQL
            for col in df_prepared.columns:
                processed_cols += 1
                if processed_cols % 10 == 0 or processed_cols == total_cols:
                    self.logger.info(
                        f"📊 Processando colunas: {processed_cols}/{total_cols}"
                    )

                if df_prepared[col].dtype == "Int64":
                    # Converter pandas Int64 para int nativo (ou None para NaN)
                    df_prepared[col] = df_prepared[col].apply(
                        lambda x: int(x) if pd.notna(x) else None
                    )
                elif df_prepared[col].dtype == "Float64":
                    # Converter pandas Float64 para float nativo
                    df_prepared[col] = df_prepared[col].apply(
                        lambda x: float(x) if pd.notna(x) else None
                    )
                elif df_prepared[col].dtype == "string":
                    # Converter pandas string para str nativo
                    df_prepared[col] = df_prepared[col].apply(
                        lambda x: str(x) if pd.notna(x) else None
                    )
                elif df_prepared[col].dtype == "object":
                    # Para objetos diversos (incluindo time), converter adequadamente
                    if col == "horario":
                        # Manter como objeto time
                        continue
                    elif col == "data":
                        # Converter date para string no formato PostgreSQL
                        df_prepared[col] = df_prepared[col].apply(
                            lambda x: x.strftime("%Y-%m-%d") if pd.notna(x) else None
                        )
                    else:
                        # Outros objetos para string
                        df_prepared[col] = df_prepared[col].apply(
                            lambda x: (
                                str(x) if pd.notna(x) and str(x) != "nan" else None
                            )
                        )

            self.logger.info("Dados preparados para PostgreSQL")
            return df_prepared

    def truncate_table(self, confirm: bool = False) -> bool:
        """
        Limpa a tabela Silver
        """
        if not confirm:
            self.logger.warning("Truncate não executado - confirmação necessária")
            return False

        self.logger.info(f"Truncando tabela {self.schema_name}.{self.table_name}")

        try:
            with ProcessTimer(self.logger, "Truncate table"):
                sql = f"TRUNCATE TABLE {self.schema_name}.{self.table_name} RESTART IDENTITY"
                self.db.execute_sql(sql)

            self.logger.info("✅ Tabela truncada com sucesso")
            return True

        except Exception as e:
            self.logger.error(f"❌ Erro ao truncar tabela: {e}")
            return False

    def load_data_batch(
        self, df: pd.DataFrame, batch_size: int = 10000, mode: str = "append"
    ) -> bool:
        """
        Carrega dados em lotes na tabela Silver

        Args:
            df: DataFrame com dados preparados
            batch_size: Tamanho do lote para inserção
            mode: Modo de inserção ('append', 'replace')
        """
        if df.empty:
            self.logger.warning("DataFrame vazio - nenhum dado para carregar")
            return True

        total_records = len(df)
        self.logger.info(
            f"🔄 Iniciando carga de {total_records:,} registros na tabela {self.schema_name}.{self.table_name}"
        )
        self.logger.info(f"📝 Modo: {mode}, Tamanho do lote: {batch_size:,}")

        try:
            with ProcessTimer(self.logger, f"Carga de {total_records:,} registros"):
                # Preparar dados para PostgreSQL
                self.logger.info("⚙️  Preparando dados para PostgreSQL...")
                df_prepared = self._prepare_for_postgres(df)

                # Calcular número de lotes
                num_batches = (total_records + batch_size - 1) // batch_size
                self.logger.info(
                    f"📦 Dados serão carregados em {num_batches} lotes de até {batch_size:,} registros"
                )

                # Determinar if_exists baseado no mode
                if_exists = "replace" if mode == "replace" else "append"

                # Inserir dados usando pandas to_sql com progresso
                self.logger.info("💾 Iniciando inserção no banco de dados...")

                # Inserir em lotes com progresso
                total_inserted = 0
                start_time = datetime.now()

                for i in range(0, len(df_prepared), batch_size):
                    batch_end = min(i + batch_size, len(df_prepared))
                    batch_df = df_prepared.iloc[i:batch_end]
                    batch_num = (i // batch_size) + 1

                    # Inserir o lote
                    batch_df.to_sql(
                        name=self.table_name,
                        con=self.db.get_engine(),
                        schema=self.schema_name,
                        if_exists=if_exists if i == 0 else "append",
                        index=False,
                        method="multi",
                    )

                    total_inserted += len(batch_df)

                    # Log de progresso a cada 10 lotes
                    if batch_num % 10 == 0 or batch_end == len(df_prepared):
                        progress_pct = (total_inserted / total_records) * 100
                        elapsed = (datetime.now() - start_time).total_seconds()

                        if total_inserted > 0:
                            rate = total_inserted / elapsed
                            remaining_records = total_records - total_inserted
                            eta_seconds = remaining_records / rate if rate > 0 else 0

                            self.logger.info(
                                f"🔄 Progresso: {total_inserted:,}/{total_records:,} ({progress_pct:.1f}%) - "
                                f"Lote {batch_num}/{num_batches} - "
                                f"Taxa: {rate:.0f} reg/s - "
                                f"ETA: {eta_seconds:.0f}s"
                            )

                self.logger.info(f"✅ {len(df)} registros carregados com sucesso")
                self.stats.add_stat("registros_carregados", len(df))

                return True

        except Exception as e:
            self.logger.error(f"❌ Erro ao carregar dados: {e}")
            return False

    def validate_loaded_data(self) -> Dict[str, any]:
        """
        Valida dados carregados na tabela Silver
        """
        self.logger.info("Validando dados carregados")

        validation_results = {}

        try:
            with ProcessTimer(self.logger, "Validação de dados"):
                # 1. Contagem total de registros
                query_total = f"SELECT COUNT(*) as total FROM {self.schema_name}.{self.table_name}"
                result = self.db.execute_query(query_total)
                total_records = result.iloc[0]["total"] if not result.empty else 0
                validation_results["total_records"] = total_records

                # 2. Contagem de sinistros únicos
                query_sinistros = f"SELECT COUNT(DISTINCT sinistro_id) as sinistros_unicos FROM {self.schema_name}.{self.table_name}"
                result = self.db.execute_query(query_sinistros)
                sinistros_unicos = (
                    result.iloc[0]["sinistros_unicos"] if not result.empty else 0
                )
                validation_results["sinistros_unicos"] = sinistros_unicos

                # 3. Contagem de pessoas envolvidas
                query_pessoas = f"SELECT COUNT(DISTINCT id_envolvido) as pessoas_envolvidas FROM {self.schema_name}.{self.table_name} WHERE id_envolvido IS NOT NULL"
                result = self.db.execute_query(query_pessoas)
                pessoas_envolvidas = (
                    result.iloc[0]["pessoas_envolvidas"] if not result.empty else 0
                )
                validation_results["pessoas_envolvidas"] = pessoas_envolvidas

                # 4. Contagem de veículos envolvidos
                query_veiculos = f"SELECT COUNT(DISTINCT veiculo_id) as veiculos_envolvidos FROM {self.schema_name}.{self.table_name} WHERE veiculo_id IS NOT NULL"
                result = self.db.execute_query(query_veiculos)
                veiculos_envolvidos = (
                    result.iloc[0]["veiculos_envolvidos"] if not result.empty else 0
                )
                validation_results["veiculos_envolvidos"] = veiculos_envolvidos

                # 5. Período dos dados
                query_periodo = f"""
                SELECT 
                    MIN(data) as data_inicio,
                    MAX(data) as data_fim,
                    COUNT(DISTINCT uf) as ufs_distintas,
                    COUNT(DISTINCT municipio) as municipios_distintos
                FROM {self.schema_name}.{self.table_name}
                WHERE data IS NOT NULL
                """
                result = self.db.execute_query(query_periodo)
                if not result.empty:
                    validation_results["periodo_inicio"] = result.iloc[0]["data_inicio"]
                    validation_results["periodo_fim"] = result.iloc[0]["data_fim"]
                    validation_results["ufs_distintas"] = result.iloc[0][
                        "ufs_distintas"
                    ]
                    validation_results["municipios_distintos"] = result.iloc[0][
                        "municipios_distintos"
                    ]

                # 6. Estatísticas de gravidade
                query_gravidade = f"""
                SELECT 
                    gravidade,
                    COUNT(DISTINCT sinistro_id) as count
                FROM {self.schema_name}.{self.table_name}
                WHERE gravidade IS NOT NULL
                GROUP BY gravidade
                ORDER BY count DESC
                """
                result = self.db.execute_query(query_gravidade)
                if not result.empty:
                    validation_results["distribuicao_gravidade"] = result.to_dict(
                        "records"
                    )

                # 7. Totalizadores
                query_totalizadores = f"""
                SELECT 
                    SUM(DISTINCT mortos) as total_mortos,
                    SUM(DISTINCT feridos) as total_feridos,
                    SUM(DISTINCT ilesos) as total_ilesos
                FROM {self.schema_name}.{self.table_name}
                """
                result = self.db.execute_query(query_totalizadores)
                if not result.empty:
                    validation_results["total_mortos"] = (
                        result.iloc[0]["total_mortos"] or 0
                    )
                    validation_results["total_feridos"] = (
                        result.iloc[0]["total_feridos"] or 0
                    )
                    validation_results["total_ilesos"] = (
                        result.iloc[0]["total_ilesos"] or 0
                    )

                # Log dos resultados
                self.logger.info("📊 RESULTADOS DA VALIDAÇÃO:")
                for key, value in validation_results.items():
                    if key != "distribuicao_gravidade":
                        self.logger.info(f"  {key}: {value}")

                if "distribuicao_gravidade" in validation_results:
                    self.logger.info("  Distribuição por gravidade:")
                    for item in validation_results["distribuicao_gravidade"]:
                        self.logger.info(
                            f"    {item['gravidade']}: {item['count']} sinistros"
                        )

        except Exception as e:
            self.logger.error(f"❌ Erro na validação: {e}")
            validation_results["erro"] = str(e)

        return validation_results

    def load_silver_data(self, df: pd.DataFrame, mode: str = "auto") -> bool:
        """
        Carrega dados completos na Silver Lakehouse

        Args:
            df: DataFrame com dados transformados
            mode: Estratégia de carga:
                - 'auto': Detecta automaticamente
                - 'truncate': Limpa tabela antes de carregar
                - 'append': Adiciona aos dados existentes
                - 'replace': Substitui todos os dados
        """
        self.logger.info("🚀 INICIANDO CARGA NA SILVER LAKEHOUSE")

        start_time = datetime.now()

        try:
            # Verificar se tabela existe
            table_exists_query = f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = '{self.schema_name}' 
                AND table_name = '{self.table_name}'
            )
            """
            result = self.db.execute_query(table_exists_query)
            table_exists = result.iloc[0]["exists"] if not result.empty else False

            if not table_exists:
                raise Exception(
                    f"Tabela {self.schema_name}.{self.table_name} não existe. Execute o init_silver_lakehouse.sql primeiro."
                )

            # Verificar dados existentes para modo auto
            if mode == "auto":
                count_query = f"SELECT COUNT(*) as count FROM {self.schema_name}.{self.table_name}"
                result = self.db.execute_query(count_query)
                existing_count = result.iloc[0]["count"] if not result.empty else 0

                if existing_count > 0:
                    mode = "append"
                    self.logger.info(
                        f"Modo AUTO: {existing_count} registros existentes, usando modo APPEND"
                    )
                else:
                    mode = "truncate"
                    self.logger.info("Modo AUTO: Tabela vazia, usando modo TRUNCATE")

            # Executar truncate se necessário
            if mode == "truncate":
                self.truncate_table(confirm=True)
                actual_mode = "append"
            else:
                actual_mode = mode

            # Carregar dados
            success = self.load_data_batch(df, batch_size=10000, mode=actual_mode)

            if success:
                # Validar dados carregados
                validation_results = self.validate_loaded_data()

                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()

                # Obter registros processados
                registros_processados = validation_results.get("total_records", len(df))

                self.logger.info("✅ CARGA SILVER LAKEHOUSE CONCLUÍDA")
                self.logger.info(f"Duração: {duration:.2f} segundos")
                self.logger.info(f"Registros carregados: {registros_processados:,}")

                return True

            else:
                self.logger.error("❌ Falha na carga de dados")
                return False

        except Exception as e:
            self.logger.error(f"❌ Erro na carga Silver Lakehouse: {e}")
            return False

    def get_statistics(self) -> Dict[str, any]:
        """
        Retorna estatísticas da tabela Silver Lakehouse
        """
        try:
            self.logger.info("Obtendo estatísticas da Silver Lakehouse")

            # Usar função PostgreSQL criada no schema
            query = "SELECT * FROM get_silver_stats()"
            result = self.db.execute_query(query)

            if not result.empty:
                stats = result.iloc[0].to_dict()
                self.logger.info("📊 Estatísticas Silver Lakehouse:")
                for key, value in stats.items():
                    self.logger.info(f"  {key}: {value}")
                return stats
            else:
                return {}

        except Exception as e:
            self.logger.error(f"Erro ao obter estatísticas: {e}")
            return {}


def main():
    """Função principal para teste"""
    try:
        # Teste básico de conexão e estrutura
        loader = SilverLakehouseLoader()

        # Testar conexão
        info = loader.db.get_connection_info()
        print(f"[OK] Conexão estabelecida com sucesso!")
        print(f"Database: {info.get('database')}")
        print(f"User: {info.get('user')}")

        # Verificar se tabela Silver existe
        table_exists_query = f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = '{loader.schema_name}' 
            AND table_name = '{loader.table_name}'
        )
        """
        result = loader.db.execute_query(table_exists_query)
        table_exists = result.iloc[0]["exists"] if not result.empty else False

        if table_exists:
            print(f"[OK] Tabela {loader.schema_name}.{loader.table_name} encontrada")

            # Obter estatísticas se existirem dados
            stats = loader.get_statistics()
            if stats:
                print("\n[INFO] Estatísticas atuais:")
                for key, value in stats.items():
                    print(f"  {key}: {value}")
            else:
                print("[INFO] Tabela vazia ou sem dados")
        else:
            print(
                f"[AVISO] Tabela {loader.schema_name}.{loader.table_name} não encontrada"
            )
            print("Execute o script init_silver_lakehouse.sql primeiro")

        print("\nPara usar o carregador:")
        print("1. Importe: from load_silver_lakehouse import SilverLakehouseLoader")
        print("2. Instancie: loader = SilverLakehouseLoader()")
        print("3. Use: success = loader.load_silver_data(df_transformed)")

    except Exception as e:
        print(f"❌ Erro: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
