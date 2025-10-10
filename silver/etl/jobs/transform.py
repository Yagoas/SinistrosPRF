"""
ETL Transform Job - Transformação de Dados para o Silver Layer
"""

import sys
import pandas as pd
import numpy as np
from pathlib import Path
import warnings
from datetime import datetime

# Adicionar o diretório utils ao path
sys.path.append(str(Path(__file__).parent.parent))

from utils import get_etl_logger, ProcessTimer, ETLStats

warnings.filterwarnings("ignore")


class SilverDataTransformer:
    """
    Transformador de dados para Silver Layer (Lakehouse)
    Implementa as transformações do notebook tratamento_raw.ipynb
    Resultado: Tabela única
    """

    def __init__(self):
        self.logger = get_etl_logger(self.__class__.__name__)
        self.stats = ETLStats(self.logger)

        # Mapeamentos das UFs para nome por extenso, região e dias da semana
        self.uf_to_localidade = {
            "AC": "Acre",
            "AL": "Alagoas",
            "AP": "Amapá",
            "AM": "Amazonas",
            "BA": "Bahia",
            "CE": "Ceará",
            "DF": "Distrito Federal",
            "ES": "Espírito Santo",
            "GO": "Goiás",
            "MA": "Maranhão",
            "MT": "Mato Grosso",
            "MS": "Mato Grosso do Sul",
            "MG": "Minas Gerais",
            "PA": "Pará",
            "PB": "Paraíba",
            "PR": "Paraná",
            "PE": "Pernambuco",
            "PI": "Piauí",
            "RJ": "Rio de Janeiro",
            "RN": "Rio Grande do Norte",
            "RS": "Rio Grande do Sul",
            "RO": "Rondônia",
            "RR": "Roraima",
            "SC": "Santa Catarina",
            "SP": "São Paulo",
            "SE": "Sergipe",
            "TO": "Tocantins",
        }

        self.uf_to_regiao = {
            "AC": "Norte",
            "AL": "Nordeste",
            "AP": "Norte",
            "AM": "Norte",
            "BA": "Nordeste",
            "CE": "Nordeste",
            "DF": "Centro-Oeste",
            "ES": "Sudeste",
            "GO": "Centro-Oeste",
            "MA": "Nordeste",
            "MT": "Centro-Oeste",
            "MS": "Centro-Oeste",
            "MG": "Sudeste",
            "PA": "Norte",
            "PB": "Nordeste",
            "PR": "Sul",
            "PE": "Nordeste",
            "PI": "Nordeste",
            "RJ": "Sudeste",
            "RN": "Nordeste",
            "RS": "Sul",
            "RO": "Norte",
            "RR": "Norte",
            "SC": "Sul",
            "SP": "Sudeste",
            "SE": "Nordeste",
            "TO": "Norte",
        }

        self.dias_semana = {
            0: "Segunda-feira",
            1: "Terça-feira",
            2: "Quarta-feira",
            3: "Quinta-feira",
            4: "Sexta-feira",
            5: "Sábado",
            6: "Domingo",
        }

        self.logger.info("SilverDataTransformer inicializado")

    def remove_irrelevant_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove colunas irrelevantes
        """
        self.logger.info("Removendo colunas irrelevantes")

        with ProcessTimer(self.logger, "Remoção de colunas irrelevantes"):
            cols_to_drop = [
                c
                for c in [
                    "regional",
                    "uop",
                    "delegacia",
                    "classificacao_acidente",
                    "dia_semana",
                    "fase_dia",
                ]
                if c in df.columns
            ]

            if cols_to_drop:
                df.drop(cols_to_drop, axis=1, inplace=True)
                self.logger.info(
                    f"Removidas {len(cols_to_drop)} colunas: {cols_to_drop}"
                )
            else:
                self.logger.info("Nenhuma coluna irrelevante encontrada para remover")

        return df

    def normalize_strings(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalização de strings
        """
        self.logger.info("Aplicando normalização de strings")

        with ProcessTimer(self.logger, "Normalização de strings"):

            def normalize(s):
                # Transforma para string e remove espaços em branco
                s_str = s.astype(str).str.strip()
                # Trata valores vazios explícitos
                s = s_str.replace(
                    {
                        "NaN": "",
                        "None": "",
                        "NoneType": "",
                        "(null)": "",
                        "na": "",
                        "n/a": "",
                        "N/A": "",
                        "NULL": "",
                        "null": "",
                        "nan": "",
                    }
                )
                # Troca vírgula decimal por ponto
                s = s.str.replace(",", ".", regex=False)
                s = s.replace("", pd.NA)
                return s

            # Aplica normalização a todas as colunas
            for col in df.columns.tolist():
                df[col] = normalize(df[col])

            self.stats.add_stat("colunas_normalizadas", len(df.columns))

        return df

    def convert_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Conversão de tipos de dados
        """
        self.logger.info("Convertendo tipos de dados")

        with ProcessTimer(self.logger, "Conversão de tipos"):
            # Colunas Int
            int_cols = [
                "id",
                "pesid",
                "id_veiculo",
                "idade",
                "ano_fabricacao_veiculo",
                "ordem_tipo_acidente",
                "br",
                "ilesos",
                "feridos_leves",
                "feridos_graves",
                "mortos",
            ]

            for col in int_cols:
                if col in df.columns:
                    try:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                        df[col] = df[col].astype("Int64")
                        self.logger.debug(f"Convertido {col} para Int64")
                    except Exception as e:
                        self.logger.warning(f"Erro ao converter {col} para Int64: {e}")

            # Colunas Float
            float_cols = ["km", "latitude", "longitude"]
            for col in float_cols:
                if col in df.columns:
                    try:
                        df[col] = df[col].astype("Float64")
                        self.logger.debug(f"Convertido {col} para Float64")
                    except Exception as e:
                        self.logger.warning(
                            f"Erro ao converter {col} para Float64: {e}"
                        )

            # Colunas String
            str_cols = [
                "dia_semana",
                "uf",
                "municipio",
                "br",
                "causa_principal",
                "causa_acidente",
                "tipo_acidente",
                "fase_dia",
                "sentido_via",
                "condicao_metereologica",
                "tipo_pista",
                "tracado_via",
                "uso_solo",
                "veiculo_tipo",
                "veiculo_marca_modelo",
                "tipo_veiculo",
                "estado_fisico",
                "sexo",
            ]
            for col in str_cols:
                if col in df.columns:
                    try:
                        df[col] = df[col].astype("string")
                        self.logger.debug(f"Convertido {col} para string")
                    except Exception as e:
                        self.logger.warning(f"Erro ao converter {col} para string: {e}")

            # Conversão da coluna datetime
            if "data_inversa" in df.columns:
                try:
                    df["data_inversa"] = pd.to_datetime(
                        df["data_inversa"].astype(str).str.strip(),
                        format="%Y-%m-%d",
                        errors="coerce",
                    )
                    self.logger.debug("Convertido data_inversa para datetime")
                except Exception as e:
                    self.logger.warning(f"Erro ao converter data_inversa: {e}")

            # Conversão da coluna time
            if "horario" in df.columns:
                try:
                    s = df["horario"].astype(str).str.strip()
                    parsed = pd.to_datetime(s, format="%H:%M:%S", errors="coerce")
                    df["horario"] = parsed.dt.time
                    self.logger.debug("Convertido horario para time")
                except Exception as e:
                    self.logger.warning(f"Erro ao converter horario: {e}")

        return df

    def rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Renomeação de colunas para padrão snake_case
        """
        self.logger.info("Renomeando colunas")

        with ProcessTimer(self.logger, "Renomeação de colunas"):
            rename_map = {
                "ano_fabricacao_veiculo": "veiculo_ano_fabricacao",
                "br": "rodovia",
                "causa_acidente": "sinistro_causa",
                "causa_principal": "sinistro_causa_principal",
                "condicao_metereologica": "condicao_meteorologica",
                "data_inversa": "data",
                "estado_fisico": "estado_fisico",
                "feridos_graves": "feridos_graves",
                "feridos_leves": "feridos_leves",
                "horario": "horario",
                "id_veiculo": "veiculo_id",
                "id": "sinistro_id",
                "idade": "envolvido_idade",
                "ilesos": "ilesos",
                "km": "quilometro",
                "latitude": "latitude",
                "longitude": "longitude",
                "marca": "veiculo_marca_modelo",
                "mortos": "mortos",
                "municipio": "municipio",
                "pesid": "id_envolvido",
                "ordem_tipo_acidente": "sinistro_ordem_tipo",
                "sentido_via": "via_sentido",
                "sexo": "envolvido_sexo",
                "tipo_acidente": "sinistro_tipo",
                "tipo_envolvido": "envolvido_tipo",
                "tipo_pista": "via_tipo",
                "tipo_veiculo": "veiculo_tipo",
                "tracado_via": "via_tracado",
                "uf": "uf",
                "uso_solo": "uso_solo",
            }

            existing_renames = {k: v for k, v in rename_map.items() if k in df.columns}
            if existing_renames:
                df.rename(columns=existing_renames, inplace=True)
                self.logger.info(f"Renomeadas {len(existing_renames)} colunas")

        return df

    def apply_de_para_transformations(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplicar transformações De-Para
        """
        self.logger.info("Aplicando transformações De-Para")

        with ProcessTimer(self.logger, "Transformações De-Para"):
            # Conversão do uso do solo: Sim/Não -> Urbano/Rural
            if "uso_solo" in df.columns:
                df["uso_solo"] = (
                    df["uso_solo"]
                    .replace({"Sim": "Urbano", "Não": "Rural"})
                    .astype("string")
                )

            # Conversão da condição metereológica
            if "condicao_meteorologica" in df.columns:
                df["condicao_meteorologica"] = df["condicao_meteorologica"].replace(
                    {"Ceu": "Céu"}
                )

        return df

    def create_derived_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Criar colunas derivadas
        """
        self.logger.info("Criando colunas derivadas")

        with ProcessTimer(self.logger, "Criação de colunas derivadas"):
            # 1. Feridos (somatória de feridos leves e graves)
            if all(col in df.columns for col in ["feridos_leves", "feridos_graves"]):
                parts = []
                parts.append(df["feridos_leves"].astype("Float64").fillna(0))
                parts.append(df["feridos_graves"].astype("Float64").fillna(0))
                df["feridos"] = sum(parts).astype("Int64")

            # 2. Rodovia formatada (BR-XXX)
            if "rodovia" in df.columns:

                def mescla_rodovia(v):
                    if pd.isna(v):
                        return pd.NA
                    s = str(v).strip()
                    if len(s) < 3:
                        return "BR-0" + s
                    return "BR-" + s

                df["rodovia"] = df["rodovia"].apply(mescla_rodovia).astype("string")

                # Número da rodovia
                def extrair_numero_rodovia(v):
                    if pd.isna(v):
                        return pd.NA
                    s = str(v).strip()
                    parts = s.split("-")
                    if len(parts) > 1:
                        return parts[-1]
                    return pd.NA

                df["rodovia_numero"] = (
                    df["rodovia"].apply(extrair_numero_rodovia).astype("string")
                )

            # 3. Data/Hora e campos temporais
            if all(col in df.columns for col in ["data", "horario"]):
                df["data_hora"] = pd.to_datetime(
                    df["data"].astype(str) + " " + df["horario"].astype(str),
                    errors="coerce",
                )

                # Dia da semana
                df["dia_semana"] = df["data_hora"].dt.weekday.map(self.dias_semana)

                # Data apenas (sem horário)
                df["data"] = df["data_hora"].dt.date

                # Ano e hora
                df["ano"] = df["data_hora"].dt.year.astype("Int64")
                df["hora"] = df["data_hora"].dt.hour.astype("Int64")

            # 4. Período do dia
            if "hora" in df.columns:

                def period_of_day(h):
                    if pd.isna(h):
                        return pd.NA
                    h = int(h)
                    if 0 <= h <= 5:
                        return "Madrugada"
                    if 6 <= h <= 11:
                        return "Manhã"
                    if 12 <= h <= 17:
                        return "Tarde"
                    return "Noite"

                df["periodo"] = df["hora"].apply(period_of_day).astype("string")

            # 5. Período da semana
            if "dia_semana" in df.columns:
                df["periodo_semana"] = (
                    df["dia_semana"]
                    .apply(
                        lambda d: (
                            "Final de semana"
                            if d in ["Domingo", "Sábado"]
                            else (
                                "Segunda à Sexta"
                                if d
                                in [
                                    "Segunda-feira",
                                    "Terça-feira",
                                    "Quarta-feira",
                                    "Quinta-feira",
                                    "Sexta-feira",
                                ]
                                else pd.NA
                            )
                        )
                    )
                    .astype("string")
                )

            # 6. Localidade (UF para nome completo)
            if "uf" in df.columns:
                df["localidade"] = df["uf"].map(self.uf_to_localidade).astype("string")

            # 7. Região
            if "uf" in df.columns:
                df["regiao"] = df["uf"].map(self.uf_to_regiao).astype("string")

            # 8. Faixa etária em anos (0-9, 10-19, etc.)
            if "envolvido_idade" in df.columns:

                def idade_to_faixa(idade):
                    if pd.isna(idade):
                        return pd.NA
                    idade = int(idade)
                    if idade < 0:
                        return pd.NA
                    if idade <= 9:
                        return "0-9"
                    elif idade <= 19:
                        return "10-19"
                    elif 20 <= idade <= 29:
                        return "20-29"
                    elif 30 <= idade <= 39:
                        return "30-39"
                    elif 40 <= idade <= 49:
                        return "40-49"
                    elif 50 <= idade <= 59:
                        return "50-59"
                    elif 60 <= idade <= 69:
                        return "60-69"
                    elif 70 <= idade <= 79:
                        return "70-79"
                    elif 80 <= idade <= 89:
                        return "80-89"
                    elif 90 <= idade <= 99:
                        return "90-99"
                    return "100+"

                df["faixa_etaria_ano"] = (
                    df["envolvido_idade"].apply(idade_to_faixa).astype("string")
                )

            # 9. Faixa etária por classificação conforme ECA
            if "envolvido_idade" in df.columns:

                def idade_to_classe(idade):
                    if pd.isna(idade):
                        return pd.NA
                    idade = int(idade)
                    if idade < 0:
                        return pd.NA
                    if idade <= 11:
                        return "Criança"
                    if 12 <= idade <= 17:
                        return "Adolescente"
                    if 18 <= idade <= 59:
                        return "Adulto"
                    return "Idoso"

                df["faixa_etaria_classe"] = (
                    df["envolvido_idade"].apply(idade_to_classe).astype("string")
                )

            # 10. Gravidade do sinistro
            if all(col in df.columns for col in ["mortos", "feridos"]):
                mortos = pd.to_numeric(df.get("mortos"), errors="coerce").fillna(0)
                feridos = pd.to_numeric(df.get("feridos"), errors="coerce").fillna(0)

                cond_morto = mortos > 0
                cond_ferido = feridos.notna() & (feridos > 0)
                cond_sem_vitima = feridos.notna() & (feridos == 0)

                choices = ["Com morto", "Com ferido", "Sem vítima"]
                conds = [cond_morto, cond_ferido, cond_sem_vitima]

                df["gravidade"] = pd.Series(
                    np.select(conds, choices, default="Não informado"), index=df.index
                ).astype("string")

            # 11. UPS (Unidade Padrão de Severidade)
            if all(col in df.columns for col in ["mortos", "feridos", "sinistro_tipo"]):
                mortos = pd.to_numeric(df.get("mortos"), errors="coerce").fillna(0)
                feridos = pd.to_numeric(df.get("feridos"), errors="coerce").fillna(0)
                tipo_acidente = df.get(
                    "sinistro_tipo", pd.Series("", index=df.index)
                ).fillna("")

                # UPS: 13 se houver morto, 6 se envolveu pedestre, 4 se houve feridos, 1 somente danos materiais
                ups = np.where(
                    mortos > 0,
                    13,
                    np.where(
                        tipo_acidente == "Atropelamento", 6, np.where(feridos > 0, 4, 1)
                    ),
                )
                df["ups"] = pd.Series(ups, index=df.index).astype("Int64")

        return df

    def treat_outliers(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Tratamento de outliers exatamente
        """
        self.logger.info("Tratando outliers")

        with ProcessTimer(self.logger, "Tratamento de outliers"):
            # Tratamento Idade - transformar idade > 200 em nulo
            if "envolvido_idade" in df.columns:
                outliers_idade = (df["envolvido_idade"] > 200).sum()
                if outliers_idade > 0:
                    df.loc[df["envolvido_idade"] > 200, "envolvido_idade"] = pd.NA
                    self.logger.info(
                        f"Tratados {outliers_idade} outliers de idade > 200"
                    )

            # Tratamento Ano de fabricação do veículo
            if "veiculo_ano_fabricacao" in df.columns:
                ano_atual = datetime.now().year
                outliers_ano = (
                    (df["veiculo_ano_fabricacao"] > ano_atual)
                    | (df["veiculo_ano_fabricacao"] < 1920)
                ).sum()
                if outliers_ano > 0:
                    df.loc[
                        (df["veiculo_ano_fabricacao"] > ano_atual)
                        | (df["veiculo_ano_fabricacao"] < 1920),
                        "veiculo_ano_fabricacao",
                    ] = pd.NA
                    self.logger.info(
                        f"Tratados {outliers_ano} outliers de ano de fabricação"
                    )

        return df

    def clean_and_normalize_final(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Limpeza e normalização final
        """
        self.logger.info("Aplicando limpeza e normalização final")

        with ProcessTimer(self.logger, "Limpeza final"):
            # Trim de várias colunas textuais e normalizar nulos
            trim_cols = [
                "uf",
                "via_sentido",
                "uso_solo",
                "sinistro_tipo",
                "sinistro_causa",
                "gravidade",
                "municipio",
                "condicao_meteorologica",
                "via_tipo",
                "via_tracado",
                "veiculo_marca_modelo",
            ]
            for c in trim_cols:
                if c in df.columns:
                    df[c] = (
                        df[c]
                        .astype(str)
                        .str.strip()
                        .replace(
                            {
                                "nan": pd.NA,
                                "None": pd.NA,
                                "(null)": pd.NA,
                                "NA/NA": pd.NA,
                            }
                        )
                    )

            # Substituir strings vazias por 'Não informado' em colunas-chave
            for c in ["sinistro_causa", "sinistro_tipo"]:
                if c in df.columns:
                    df[c] = df[c].replace({pd.NA: "Não informado", "": "Não informado"})

            # Deduplicate global
            before = len(df)
            df = df.drop_duplicates()
            after = len(df)
            if before != after:
                self.logger.info(
                    f"Deduplicated: {before} -> {after} (removidas {before - after} duplicatas)"
                )

        return df

    def prepare_final_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Preparar colunas finais para match com schema Silver
        """
        self.logger.info("Preparando colunas finais")

        with ProcessTimer(self.logger, "Preparação colunas finais"):
            # Garantir que todas as colunas necessárias existem (criar com NA se não existir)
            required_columns = [
                "sinistro_id",
                "id_envolvido",
                "veiculo_id",
                "data",
                "horario",
                "data_hora",
                "ano",
                "hora",
                "dia_semana",
                "periodo",
                "periodo_semana",
                "uf",
                "localidade",
                "regiao",
                "municipio",
                "rodovia",
                "rodovia_numero",
                "quilometro",
                "latitude",
                "longitude",
                "sinistro_tipo",
                "sinistro_causa",
                "sinistro_causa_principal",
                "sinistro_ordem_tipo",
                "condicao_meteorologica",
                "via_tipo",
                "via_tracado",
                "via_sentido",
                "uso_solo",
                "envolvido_idade",
                "envolvido_sexo",
                "envolvido_tipo",
                "estado_fisico",
                "faixa_etaria_ano",
                "faixa_etaria_classe",
                "veiculo_tipo",
                "veiculo_marca_modelo",
                "veiculo_ano_fabricacao",
                "ilesos",
                "feridos_leves",
                "feridos_graves",
                "feridos",
                "mortos",
                "gravidade",
                "ups",
            ]

            for col in required_columns:
                if col not in df.columns:
                    df[col] = pd.NA
                    self.logger.debug(f"Criada coluna ausente: {col}")

            # Selecionar apenas as colunas necessárias na ordem correta
            df = df[required_columns]

        return df

    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica todas as transformações em sequência (pipeline completo)
        """
        self.logger.info("🔄 INICIANDO TRANSFORMAÇÃO SILVER LAKEHOUSE")

        start_time = datetime.now()
        original_shape = df.shape

        try:
            # 1. Remover colunas irrelevantes
            df = self.remove_irrelevant_columns(df)
            self.stats.add_stat("apos_remocao_colunas", df.shape)

            # 2. Normalizar strings
            df = self.normalize_strings(df)
            self.stats.add_stat("apos_normalizacao", df.shape)

            # 3. Converter tipos de dados
            df = self.convert_data_types(df)
            self.stats.add_stat("apos_conversao_tipos", df.shape)

            # 4. Renomear colunas
            df = self.rename_columns(df)
            self.stats.add_stat("apos_renomeacao", df.shape)

            # 5. Aplicar transformações De-Para
            df = self.apply_de_para_transformations(df)
            self.stats.add_stat("apos_de_para", df.shape)

            # 6. Criar colunas derivadas
            df = self.create_derived_columns(df)
            self.stats.add_stat("apos_colunas_derivadas", df.shape)

            # 7. Tratar outliers
            df = self.treat_outliers(df)
            self.stats.add_stat("apos_outliers", df.shape)

            # 8. Limpeza e normalização final
            df = self.clean_and_normalize_final(df)
            self.stats.add_stat("apos_limpeza_final", df.shape)

            # 9. Preparar colunas finais
            df = self.prepare_final_columns(df)
            self.stats.add_stat("shape_final", df.shape)

            # Estatísticas finais
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            self.stats.add_stat("duracao_segundos", round(duration, 2))
            self.stats.add_stat("registros_originais", original_shape[0])
            self.stats.add_stat("registros_finais", df.shape[0])
            self.stats.add_stat("colunas_originais", original_shape[1])
            self.stats.add_stat("colunas_finais", df.shape[1])

            # Log de informações sobre dados únicos
            if "sinistro_id" in df.columns:
                sinistros_unicos = df["sinistro_id"].nunique()
                self.stats.add_stat("sinistros_unicos", sinistros_unicos)

            if "id_envolvido" in df.columns:
                pessoas_envolvidas = df["id_envolvido"].nunique()
                self.stats.add_stat("pessoas_envolvidas", pessoas_envolvidas)

            if "veiculo_id" in df.columns:
                veiculos_envolvidos = df["veiculo_id"].nunique()
                self.stats.add_stat("veiculos_envolvidos", veiculos_envolvidos)

            self.logger.info(f"✅ Transformação concluída em {duration:.2f}s")
            self.logger.info(f"Shape: {original_shape} → {df.shape}")

            return df

        except Exception as e:
            self.logger.error(f"❌ Erro na transformação: {e}")
            raise
