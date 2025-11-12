# data_layer/gold/etl/pipeline.py
import pandas as pd
from data_layer.gold.etl.jobs.extract import GoldExtractor
from data_layer.gold.etl.jobs.transform import GoldTransformer
from data_layer.gold.etl.jobs.load import GoldLoader
from data_layer.gold.etl.utils import get_etl_logger, ProcessTimer

logger = get_etl_logger("GoldPipeline")

class GoldPipeline:
    def __init__(self, where_clause: str = None):
        self.extractor = GoldExtractor()
        self.transformer = GoldTransformer()
        self.loader = GoldLoader()
        self.where_clause = where_clause

    def run(self):
        logger.info("🚀 Iniciando pipeline Gold ETL")

        with ProcessTimer(logger, "Extração"):
            df_silver = self.extractor.extract_silver_table(where_clause=self.where_clause)

        if df_silver.empty:
            logger.warning("⚠️ Nenhum dado retornado da camada Silver.")
            return

        with ProcessTimer(logger, "Transformação"):
            dims, fato = self.transformer.build_dimensions_and_fato(df_silver)

        with ProcessTimer(logger, "Carga"):
            self.loader.load_all(dims, fato)

        logger.info("✅ Pipeline Gold concluído com sucesso!")


if __name__ == "__main__":
    import sys
    where_clause = sys.argv[1] if len(sys.argv) > 1 else None
    GoldPipeline(where_clause).run()
