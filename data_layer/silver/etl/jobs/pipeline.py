"""
Pipeline ETL - Bronze to Silver (Lakehouse)
"""

import sys
from pathlib import Path
import pandas as pd
from typing import Optional, List
import warnings

# Adicionar o diretório utils ao path
sys.path.append(str(Path(__file__).parent.parent))

from utils import get_etl_logger, ProcessTimer, ETLStats, db_manager
from silver.etl.jobs.extract import DataSourceExtractor
from silver.etl.jobs.transform import SilverDataTransformer
from silver.etl.jobs.load import SilverLakehouseLoader

warnings.filterwarnings("ignore")


class SilverPipeline:
    """Pipeline ETL Bronze → Silver (Lakehouse)"""

    def __init__(self, bronze_path: str, output_dir: Optional[str] = None):
        self.bronze_path = Path(bronze_path)
        self.output_dir = (
            Path(output_dir) if output_dir else Path(__file__).parent.parent / "data"
        )
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.logger = get_etl_logger(self.__class__.__name__)
        self.stats = ETLStats(self.logger)

        # Inicializar componentes de transformação e carga
        self.transformer = SilverDataTransformer()
        self.loader = SilverLakehouseLoader()

        self.logger.info("SilverPipeline inicializado")
        self.logger.info(f"Bronze path: {self.bronze_path}")
        self.logger.info(f"Output dir: {self.output_dir}")

    def extract_from_source(self) -> bool:
        """
        Extrai dados da fonte oficial PRF e salva na camada Bronze
        """
        self.logger.info("=" * 60)
        self.logger.info("🌐 EXTRAINDO DADOS DA FONTE OFICIAL")
        self.logger.info("=" * 60)

        try:
            # Inicializar extrator apontando para pasta bronze/data
            bronze_data_path = self.bronze_path / "data"
            extractor = DataSourceExtractor(str(bronze_data_path))

            # Realizar extração
            success = extractor.extract_all(force_download=False)

            if success:
                self.logger.info("✅ Extração da fonte concluída com sucesso!")
                return True
            else:
                self.logger.error("❌ Falha na extração da fonte")
                return False

        except Exception as e:
            self.logger.error(f"Erro na extração da fonte: {e}")
            return False

    def get_csv_files(self) -> List[Path]:
        """Retorna lista de arquivos CSV na camada Bronze"""
        data_dir = self.bronze_path / "data"

        if not data_dir.exists():
            raise FileNotFoundError(f"Diretório de dados não encontrado: {data_dir}")

        csv_files = list(data_dir.glob("*.csv"))

        if not csv_files:
            raise FileNotFoundError(f"Nenhum arquivo CSV encontrado em: {data_dir}")

        self.logger.info(f"Encontrados {len(csv_files)} arquivos CSV")
        for file in csv_files:
            self.logger.info(f"  - {file.name}")

        return csv_files

    def read_and_combine_csvs(self) -> pd.DataFrame:
        """
        Lê e combina todos os CSVs da Bronze em um único DataFrame
        """
        self.logger.info("=" * 60)
        self.logger.info("📥 LENDO E COMBINANDO CSVs DA BRONZE")
        self.logger.info("=" * 60)

        csv_files = self.get_csv_files()
        dataframes = []

        total_files = len(csv_files)
        total_rows_processed = 0

        for i, file_path in enumerate(csv_files, 1):
            try:
                self.logger.info(
                    f"📄 [{i}/{total_files}] Processando: {file_path.name}"
                )

                with ProcessTimer(self.logger, f"Leitura {file_path.name}"):
                    # Configurações de leitura do csv
                    df = pd.read_csv(
                        file_path,
                        sep=",",
                        encoding="utf-8",
                    )

                self.logger.info(
                    f"Arquivo lido: {df.shape[0]:,} linhas, {df.shape[1]} colunas"
                )
                self.logger.info(
                    f"📊 Total acumulado: {total_rows_processed:,} registros"
                )

                # Validação básica
                if df.empty:
                    self.logger.warning(
                        f"⚠️  Arquivo {file_path.name} está vazio, pulando..."
                    )
                    continue

                dataframes.append(df)
                self.stats.add_stat(f"linhas_{file_path.stem}", df.shape[0])
                self.stats.increment_counter("arquivos_processados")

            except Exception as e:
                self.logger.error(f"Erro ao processar {file_path.name}: {e}")
                self.stats.increment_counter("arquivos_erro")
                continue

        if not dataframes:
            raise ValueError("Nenhum arquivo foi processado com sucesso")

        # Combinar todos os DataFrames
        self.logger.info("Combinando todos os DataFrames...")

        with ProcessTimer(self.logger, "Combinação de DataFrames"):
            combined_df = pd.concat(dataframes, ignore_index=True)

        self.logger.info(
            f"✅ DataFrame combinado: {combined_df.shape[0]:,} linhas, {combined_df.shape[1]} colunas"
        )

        # Estatísticas básicas
        self.stats.add_stat("total_registros_bronze", len(combined_df))
        self.stats.add_stat("total_colunas_bronze", len(combined_df.columns))

        memory_mb = combined_df.memory_usage(deep=True).sum() / 1024**2
        self.stats.add_stat("memoria_bronze_mb", round(memory_mb, 2))
        self.logger.info(f"Memória utilizada: {memory_mb:.2f} MB")

        # Verificações básicas
        self._validate_combined_data(combined_df)

        return combined_df

    def _validate_combined_data(self, df: pd.DataFrame):
        """Validações no DataFrame combinado"""
        self.logger.info("Validando dados combinados...")

        # Verificar linhas duplicadas
        if "id" in df.columns:
            duplicate_register = df.duplicated().sum()
            if duplicate_register > 0:
                self.stats.add_stat("duplicatas_registros", duplicate_register)

        # Verificar dados nulos em colunas críticas
        critical_columns = ["id", "data_inversa", "uf"]
        for col in critical_columns:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                if null_count > 0:
                    self.logger.warning(
                        f"Coluna '{col}' possui {null_count:,} valores nulos"
                    )

    def run_pipeline(self) -> bool:
        """
        Executa o pipeline completo: extrai, transforma e carrega
        """
        try:
            self.logger.info("🚀 INICIANDO PIPELINE ETL")
            self.logger.info("=" * 80)

            with ProcessTimer(self.logger, "Pipeline completo"):

                # 1. Extração das fontes
                self.logger.info("=" * 60)
                self.logger.info("EXTRAÇÃO DOS DADOS DA FONTE PRF")
                self.logger.info("=" * 60)

                if not self.extract_from_source():
                    raise RuntimeError("Falha na extração dos dados")

                self.logger.info("✅ Extração dos dados concluída!")

                # 2. Ler e combinar CSVs da Bronze
                df_bronze = self.read_and_combine_csvs()

                # 3. Transformar dados para Silver
                self.logger.info("=" * 60)
                self.logger.info("TRANSFORMANDO DADOS PARA SILVER LAKEHOUSE")
                self.logger.info("=" * 60)

                with ProcessTimer(self.logger, "Transformação"):
                    df_silver = self.transformer.transform_data(df_bronze)

                self.stats.add_stat("registros_silver", len(df_silver))
                self.logger.info(
                    f"✅ Transformação concluída: {len(df_silver):,} registros"
                )

                # 4. Salvar dados transformados na Silver
                self.logger.info("=" * 60)
                self.logger.info("SALVANDO DADOS TRANSFORMADOS NA SILVER")
                self.logger.info("=" * 60)

                # Salvar CSV na pasta silver/etl/data
                silver_csv_path = self.output_dir / "sinistros_tratado.csv"

                with ProcessTimer(self.logger, "Salvamento CSV Silver"):
                    df_silver.to_csv(
                        silver_csv_path, index=False, encoding="utf-8", sep=","
                    )

                self.logger.info(f"✅ Dados salvos em: {silver_csv_path}")

                # 5. Carregar dados no banco
                self.logger.info("=" * 60)
                self.logger.info("CARREGANDO DADOS NO BANCO (LAKEHOUSE)")
                self.logger.info("=" * 60)

                with ProcessTimer(self.logger, "Carga no banco"):
                    load_success = self.loader.load_silver_data(df_silver)

                if not load_success:
                    raise RuntimeError("Falha na carga dos dados no banco")

                # 6. Finalizar processo
                self.logger.info("📊 RESUMO DO PROCESSAMENTO:")
                for key, value in self.stats.get_all_stats().items():
                    self.logger.info(
                        f"   {key}: {value:,}"
                        if isinstance(value, (int, float))
                        else f"   {key}: {value}"
                    )

                self.logger.info("=" * 80)
                self.logger.info("🎉 PIPELINE ETL CONCLUÍDO COM SUCESSO!")
                self.logger.info("=" * 80)

                return True

        except Exception as e:
            self.logger.error(f"❌ Erro no pipeline: {e}")
            return False


def main():
    """
    Executa o pipeline ETL: extrai, transforma e carrega
    """
    # Configurar logging do script principal
    logger = get_etl_logger("PipelineMain")
    logger.info("Iniciando pipeline ETL")

    try:
        # Verificar conectividade com banco
        if not db_manager.test_connection():
            logger.error("❌ Não foi possível conectar ao banco de dados")
            sys.exit(1)

        # Executar pipeline
        pipeline = SilverPipeline(
            bronze_path="/usr/src/bronze", output_dir="/usr/src/silver/etl/data"
        )

        success = pipeline.run_pipeline()

        if success:
            logger.info("✅ Script concluído com sucesso")
            logger.info("🎉 Pipeline ETL concluído!")
            sys.exit(0)
        else:
            logger.error("❌ Script finalizado com erro")
            sys.exit(1)

    except Exception as e:
        logger.error(f"❌ Erro crítico no script: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
