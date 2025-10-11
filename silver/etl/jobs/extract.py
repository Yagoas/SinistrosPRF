"""
ETL Extract Job - Download de dados de sinistros da PRF
"""

import sys
import requests
import zipfile
from pathlib import Path
from typing import Optional
import pandas as pd
import time

# Adicionar o diretório utils ao path
sys.path.append(str(Path(__file__).parent.parent))

from utils import get_etl_logger, ProcessTimer, ETLStats


class DataSourceExtractor:
    """Extrator de dados das fontes oficiais PRF"""

    # URLs das fontes de dados
    DATA_SOURCES = {
        # "2017": {
        #     "url": "https://drive.usercontent.google.com/uc?id=1Kv5mNgZvxtl0xwqsmDrxcaLY2KELxR-3&export=download",
        #     "filename": "acidentes2017_todas_causas_tipos.csv",
        #     "description": "Sinistros PRF 2017"
        # },
        # "2018": {
        #     "url": "https://drive.usercontent.google.com/uc?id=1J-012nSnIafOASNFvIYY_vDKKpM51w5_&export=download",
        #     "filename": "acidentes2018_todas_causas_tipos.csv",
        #     "description": "Sinistros PRF 2018"
        # },
        # "2019": {
        #     "url": "https://drive.usercontent.google.com/uc?id=1DAJYKVfkTcPhQodSmHp9rsG1Q8XJW-m3&export=download",
        #     "filename": "acidentes2019_todas_causas_tipos.csv",
        #     "description": "Sinistros PRF 2019"
        # },
        # "2020": {
        #     "url": "https://drive.usercontent.google.com/uc?id=1yQtVOsAlupPHQVVTmbJo0NR3XMzgHANO&export=download",
        #     "filename": "acidentes2020_todas_causas_tipos.csv",
        #     "description": "Sinistros PRF 2020"
        # },
        # "2021": {
        #     "url": "https://drive.usercontent.google.com/uc?id=1Gk3U6cMOZIevsDZHLi6J503xoCRS_lnI&export=download",
        #     "filename": "acidentes2021_todas_causas_tipos.csv",
        #     "description": "Sinistros PRF 2021"
        # },
        # "2022": {
        #     "url": "https://drive.usercontent.google.com/uc?id=1wskEgRC3ame7rncSDQ7qWhKsoKw1lohY&export=download",
        #     "filename": "acidentes2022_todas_causas_tipos.csv",
        #     "description": "Sinistros PRF 2022"
        # },
        # "2023": {
        #     "url": "https://drive.usercontent.google.com/uc?id=1-caam_dahYOf2eorq4mez04Om6DD5d_3&export=download",
        #     "filename": "acidentes2023_todas_causas_tipos.csv",
        #     "description": "Sinistros PRF 2023"
        # },
        "2024": {
            "url": "https://drive.usercontent.google.com/uc?id=14qBOhrE1gioVtuXgxkCJ9kCA8YtUGXKA&export=download",
            "filename": "acidentes2024_todas_causas_tipos.csv",
            "description": "Sinistros PRF 2024",
        },
        "2025": {
            "url": "https://drive.usercontent.google.com/uc?id=1-PJGRbfSe7PVjU37A3wTCls_NRXyVGRD&export=download",
            "filename": "acidentes2025_todas_causas_tipos.csv",
            "description": "Sinistros PRF 2025",
        },
    }

    def __init__(self, bronze_path: str = None):
        """
        Inicializa o extrator
        """
        if bronze_path is None:
            # Usar o caminho padrão relativo
            current_file = Path(__file__).resolve()
            project_root = (
                current_file.parent.parent.parent.parent
            )  # jobs -> etl -> silver -> SDB2-Projeto
            bronze_path = project_root / "bronze" / "data"

        self.bronze_path = Path(bronze_path)
        self.bronze_path.mkdir(parents=True, exist_ok=True)

        self.logger = get_etl_logger("DataExtractor")
        self.stats = ETLStats("DataExtractor")

        # Configurar requests session com timeout e retries
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
        )

    def download_file(self, url: str, description: str) -> Optional[bytes]:
        """
        Realiza download de um arquivo
        """
        try:
            self.logger.info(f"Baixando {description}...")

            with ProcessTimer(
                self.logger, f"Download {description}", show_progress=True
            ):
                response = self.session.get(url, timeout=120, stream=True)
                response.raise_for_status()

                # Verificar tamanho do arquivo
                content_length = response.headers.get("content-length")
                if content_length:
                    total_size = int(content_length)
                    self.logger.info(
                        f"Tamanho do arquivo: {total_size / (1024*1024):.1f} MB"
                    )

                # Download com progress
                content = b""
                downloaded = 0
                chunk_size = 8192

                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        content += chunk
                        downloaded += len(chunk)

                        # Log progress a cada 10MB
                        if downloaded % (10 * 1024 * 1024) == 0:
                            mb_downloaded = downloaded / (1024 * 1024)
                            self.logger.info(f"Baixados: {mb_downloaded:.1f} MB")

            self.logger.info(f"Download concluído: {len(content) / (1024*1024):.1f} MB")
            return content

        except requests.exceptions.RequestException as e:
            self.logger.warning(f"Erro no download: {e}")

        except Exception as e:
            self.logger.error(f"Erro inesperado no download: {e}")

        return None

    def extract_csv_from_zip(self, zip_content: bytes) -> Optional[pd.DataFrame]:
        """
        Extrai CSV de um arquivo ZIP
        """
        try:
            # Usar diretório temporário na pasta do projeto
            temp_dir = self.bronze_path.parent / "temp"
            temp_dir.mkdir(exist_ok=True)

            temp_zip_path = temp_dir / f"temp_download_{int(time.time())}.zip"

            try:
                # Escrever ZIP temporário
                with open(temp_zip_path, "wb") as temp_zip:
                    temp_zip.write(zip_content)

                with zipfile.ZipFile(temp_zip_path, "r") as zip_ref:
                    # Listar arquivos no ZIP
                    zip_files = zip_ref.namelist()
                    self.logger.info(f"Arquivos no ZIP: {zip_files}")

                    # Procurar arquivo CSV
                    csv_file = None
                    for file in zip_files:
                        if file.lower().endswith(".csv"):
                            csv_file = file
                            break

                    if not csv_file:
                        self.logger.error("Nenhum arquivo CSV encontrado no ZIP")
                        return None

                    self.logger.info(f"Extraindo CSV: {csv_file}")

                    # Extrair e ler CSV
                    with zip_ref.open(csv_file) as csv_content:
                        # Configurações de leitura do csv
                        df = pd.read_csv(
                            csv_content,
                            sep=",",
                            encoding="latin-1",
                            quotechar='"',
                            low_memory=False,
                        )

                        if df is None:
                            self.logger.error(
                                "Não foi possível ler o CSV"
                            )
                            return None

                    self.logger.info(
                        f"CSV extraído: {len(df):,} linhas, {len(df.columns)} colunas"
                    )
                    return df

            finally:
                # Limpar arquivo temporário
                if temp_zip_path.exists():
                    temp_zip_path.unlink()

        except zipfile.BadZipFile:
            self.logger.error("Arquivo ZIP corrompido ou inválido")
        except pd.errors.EmptyDataError:
            self.logger.error("Arquivo CSV vazio")
        except Exception as e:
            self.logger.error(f"Erro ao extrair CSV do ZIP: {e}")

        return None

    def save_dataframe(self, df: pd.DataFrame, filename: str) -> bool:
        """
        Salva DataFrame como CSV na pasta bronze
        """
        try:
            output_path = self.bronze_path / filename

            with ProcessTimer(self.logger, f"Salvando {filename}"):
                df.to_csv(output_path, index=False, encoding="utf-8")

            # Verificar arquivo salvo
            if output_path.exists():
                file_size = output_path.stat().st_size / (1024 * 1024)
                self.logger.info(f"Arquivo salvo: {output_path} ({file_size:.1f} MB)")
                return True
            else:
                self.logger.error(f"Arquivo não foi criado: {output_path}")
                return False

        except Exception as e:
            self.logger.error(f"Erro ao salvar arquivo {filename}: {e}")
            return False

    def validate_data(self, df: pd.DataFrame, year: str) -> bool:
        """
        Valida básica dos dados extraídos
        """
        try:
            # Verificar se não está vazio
            if df.empty:
                self.logger.error(f"DataFrame vazio para {year}")
                return False

            # Verificar colunas mínimas esperadas
            required_columns = ["data_inversa", "br", "km", "municipio", "uf"]
            missing_columns = [col for col in required_columns if col not in df.columns]

            if missing_columns:
                self.logger.warning(f"Colunas esperadas ausentes: {missing_columns}")

            # Estatísticas básicas
            self.logger.info(f"Validação {year}:")
            self.logger.info(f"  - Linhas: {len(df):,}")
            self.logger.info(f"  - Colunas: {len(df.columns)}")
            self.logger.info(
                f"  - Memória: {df.memory_usage(deep=True).sum() / (1024*1024):.1f} MB"
            )

            # Verificar algumas colunas básicas
            if "data_inversa" in df.columns:
                non_null_dates = df["data_inversa"].notna().sum()
                self.logger.info(
                    f"  - Datas válidas: {non_null_dates:,} ({non_null_dates/len(df)*100:.1f}%)"
                )

            if "uf" in df.columns:
                unique_ufs = df["uf"].nunique()
                self.logger.info(f"  - UFs únicas: {unique_ufs}")

            return True

        except Exception as e:
            self.logger.error(f"Erro na validação dos dados {year}: {e}")
            return False

    def extract_year(self, year: str, force_download: bool = False) -> bool:
        """
        Extrai dados de um ano específico
        """
        if year not in self.DATA_SOURCES:
            self.logger.error(
                f"Ano inválido: {year}. Disponíveis: {list(self.DATA_SOURCES.keys())}"
            )
            return False

        source = self.DATA_SOURCES[year]
        output_file = self.bronze_path / source["filename"]

        # Verificar se arquivo já existe
        if output_file.exists() and not force_download:
            file_size = output_file.stat().st_size / (1024 * 1024)
            self.logger.info(
                f"Arquivo {source['filename']} já existe ({file_size:.1f} MB)"
            )
            self.logger.info("Use force_download=True para baixar novamente")
            return True

        # Download
        self.logger.info(f"Iniciando extração: {source['description']}")
        zip_content = self.download_file(source["url"], source["description"])

        if not zip_content:
            self.logger.error(f"Falha no download de {year}")
            return False

        # Extrair CSV
        df = self.extract_csv_from_zip(zip_content)

        if df is None:
            self.logger.error(f"Falha na extração do CSV de {year}")
            return False

        # Validar dados
        if not self.validate_data(df, year):
            self.logger.error(f"Dados inválidos para {year}")
            return False

        # Salvar
        if not self.save_dataframe(df, source["filename"]):
            self.logger.error(f"Falha ao salvar dados de {year}")
            return False

        self.logger.info(f"Extração de {year} concluída com sucesso!")
        return True

    def extract_all(self, force_download: bool = False) -> bool:
        """
        Extrai todos os anos disponíveis
        """
        self.logger.info("Iniciando extração de todos os dados...")

        success_count = 0
        total_years = len(self.DATA_SOURCES)
        start_time = time.time()

        with ProcessTimer(self.logger, "Extração completa"):
            for year in self.DATA_SOURCES.keys():
                if self.extract_year(year, force_download):
                    success_count += 1
                else:
                    self.logger.error(f"Falha na extração de {year}")

        self.total_extraction_time = time.time() - start_time

        # Resumo final
        self.logger.info(
            f"Extração concluída: {success_count}/{total_years} anos processados"
        )

        if success_count == total_years:
            self.logger.info("Todos os dados extraídos com sucesso!")
            self.show_summary()
            return True
        else:
            self.logger.error(f"Falha em {total_years - success_count} extrações")
            return False

    def show_summary(self):
        """Mostra resumo dos arquivos extraídos"""
        self.logger.info("=" * 50)
        self.logger.info("RESUMO DOS DADOS EXTRAÍDOS")
        self.logger.info("=" * 50)

        total_size = 0
        total_files = 0

        for year, source in self.DATA_SOURCES.items():
            file_path = self.bronze_path / source["filename"]
            if file_path.exists():
                file_size = file_path.stat().st_size / (1024 * 1024)
                total_size += file_size
                total_files += 1

                # Contar linhas rapidamente
                try:
                    df = pd.read_csv(file_path, nrows=0)  # Só cabeçalho
                    line_count = (
                        sum(1 for _ in open(file_path, "r", encoding="utf-8")) - 1
                    )

                    self.logger.info(f"{year}: {source['filename']}")
                    self.logger.info(f"  - Tamanho: {file_size:.1f} MB")
                    self.logger.info(f"  - Linhas: {line_count:,}")
                    self.logger.info(f"  - Colunas: {len(df.columns)}")

                except Exception as e:
                    self.logger.warning(f"Erro ao ler estatísticas de {year}: {e}")

        self.logger.info("-" * 30)
        self.logger.info(f"Total: {total_files} arquivos, {total_size:.1f} MB")
        self.logger.info("=" * 50)

        # Tempo total da extração
        total_time = getattr(self, "total_extraction_time", 0)
        self.logger.info(f"Tempo total: {total_time:.2f}s")


def main():
    """Função principal para execução standalone - extrai todos os dados disponíveis"""
    print("🌐 Iniciando extração de dados da PRF...")
    print("📋 Extraindo todos os anos disponíveis")

    try:
        # Usar caminho padrão bronze/data
        extractor = DataSourceExtractor()

        # Extrair todos os dados disponíveis
        success = extractor.extract_all(force_download=False)

        if success:
            print("✅ Extração concluída com sucesso!")
            return 0
        else:
            print("❌ Falha na extração")
            return 1

    except KeyboardInterrupt:
        print("\n⚠️ Operação cancelada pelo usuário")
        return 1
    except Exception as e:
        print(f"❌ Erro inesperado: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
