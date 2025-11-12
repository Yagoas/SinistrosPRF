import pandas as pd
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from utils import get_etl_logger, ProcessTimer, ETLStats

logger = get_etl_logger("GoldTransformer")

class GoldTransformer:
    def __init__(self):
        self.logger = logger
        self.stats = ETLStats(self.logger)

    def build_dimensions_and_fato(self, df: pd.DataFrame):
        """
        Cria dimensões e fato a partir do DataFrame da Silver.
        Compatível com os campos da tabela silver.tb_sinistros_silver.
        """
        with ProcessTimer(self.logger, "Transformação - criação de dimensões"):

            # DIMENSÃO DATA
            dim_data = (
                df[["data", "ano", "hora", "dia_semana", "periodo", "periodo_semana"]]
                .drop_duplicates()
                .reset_index(drop=True)
                .assign(data_id=lambda x: x.index + 1)
            )

            # DIMENSÃO LOCAL
            dim_local = (
                df[
                    [
                        "uf",
                        "localidade",
                        "regiao",
                        "municipio",
                        "rodovia",
                        "rodovia_numero",
                        "quilometro",
                        "latitude",
                        "longitude",
                    ]
                ]
                .drop_duplicates()
                .reset_index(drop=True)
                .assign(local_id=lambda x: x.index + 1)
            )

            # DIMENSÃO TIPO DE SINISTRO
            dim_tipo = (
                df[
                    [
                        "sinistro_tipo",
                        "sinistro_causa",
                        "sinistro_causa_principal",
                        "sinistro_ordem_tipo",
                        "gravidade",
                    ]
                ]
                .drop_duplicates()
                .reset_index(drop=True)
                .assign(tipo_id=lambda x: x.index + 1)
            )

            # DIMENSÃO CONDIÇÃO DA VIA
            dim_via = (
                df[
                    [
                        "condicao_meteorologica",
                        "via_tipo",
                        "via_tracado",
                        "via_sentido",
                        "uso_solo",
                    ]
                ]
                .drop_duplicates()
                .reset_index(drop=True)
                .assign(via_id=lambda x: x.index + 1)
            )

            # DIMENSÃO ENVOLVIDO
            dim_envolvido = (
                df[
                    [
                        "envolvido_tipo",
                        "envolvido_sexo",
                        "envolvido_idade",
                        "faixa_etaria_ano",
                        "faixa_etaria_classe",
                        "estado_fisico",
                    ]
                ]
                .drop_duplicates()
                .reset_index(drop=True)
                .assign(envolvido_id=lambda x: x.index + 1)
            )

            # DIMENSÃO VEÍCULO
            dim_veiculo = (
                df[
                    [
                        "veiculo_id",
                        "veiculo_tipo",
                        "veiculo_marca_modelo",
                        "veiculo_ano_fabricacao",
                    ]
                ]
                .drop_duplicates()
                .reset_index(drop=True)
                .assign(veiculo_dim_id=lambda x: x.index + 1)
            )

            # CONSTRUÇÃO DA FATO
            with ProcessTimer(self.logger, "Construção da Fato"):
                fato = (
                    df.merge(
                        dim_data,
                        on=[
                            "data",
                            "ano",
                            "hora",
                            "dia_semana",
                            "periodo",
                            "periodo_semana",
                        ],
                        how="left",
                    )
                    .merge(
                        dim_local,
                        on=[
                            "uf",
                            "localidade",
                            "regiao",
                            "municipio",
                            "rodovia",
                            "rodovia_numero",
                            "quilometro",
                            "latitude",
                            "longitude",
                        ],
                        how="left",
                    )
                    .merge(
                        dim_tipo,
                        on=[
                            "sinistro_tipo",
                            "sinistro_causa",
                            "sinistro_causa_principal",
                            "sinistro_ordem_tipo",
                            "gravidade",
                        ],
                        how="left",
                    )
                    .merge(
                        dim_via,
                        on=[
                            "condicao_meteorologica",
                            "via_tipo",
                            "via_tracado",
                            "via_sentido",
                            "uso_solo",
                        ],
                        how="left",
                    )
                    .merge(
                        dim_envolvido,
                        on=[
                            "envolvido_tipo",
                            "envolvido_sexo",
                            "envolvido_idade",
                            "faixa_etaria_ano",
                            "faixa_etaria_classe",
                            "estado_fisico",
                        ],
                        how="left",
                    )
                    .merge(
                        dim_veiculo,
                        on=[
                            "veiculo_id",
                            "veiculo_tipo",
                            "veiculo_marca_modelo",
                            "veiculo_ano_fabricacao",
                        ],
                        how="left",
                    )
                )

                # Seleciona colunas finais da fato
                fato_final = fato[
                    [
                        "sinistro_id",
                        "data_id",
                        "local_id",
                        "tipo_id",
                        "via_id",
                        "envolvido_id",
                        "veiculo_dim_id",
                        "ilesos",
                        "feridos_leves",
                        "feridos_graves",
                        "feridos",
                        "mortos",
                    ]
                ]

            # MÉTRICAS E RETORNO
            self.stats.add_stat("total_registros_fato", len(fato_final))
            self.stats.summary()

            return {
                "dim_data": dim_data,
                "dim_local": dim_local,
                "dim_tipo": dim_tipo,
                "dim_via": dim_via,
                "dim_envolvido": dim_envolvido,
                "dim_veiculo": dim_veiculo,
            }, fato_final
