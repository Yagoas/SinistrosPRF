# Dicionário de Mnemônicos - Camada Gold

Este documento descreve os mnemônicos utilizados na modelagem dimensional da camada Gold (Data Warehouse).

## 📋 Convenções

- **SRK**: Surrogate Key (Chave Substituta) - Identificador único sequencial gerado automaticamente
- **Formato SRK**: `srk_xxx` onde xxx é o prefixo da tabela
- **Prefixo da Coluna**: 3 letras correspondentes ao atributo

---

## 🗂️ Tabelas Dimensionais

### dim_temporal - Dimensão Temporal

Armazena informações de data e tempo dos sinistros.

| Mnemônico | Significado             |
| --------- | ----------------------- |
| `srk_tmp` | Temporal Surrogate Key  |
| `tmp_dta` | Temporal Data           |
| `tmp_ano` | Temporal Ano            |
| `tmp_hra` | Temporal Hora           |
| `tmp_dsm` | Temporal Dia Semana     |
| `tmp_per` | Temporal Período        |
| `tmp_psm` | Temporal Período Semana |

---

### dim_localizacao - Dimensão Localização

Armazena informações geográficas dos sinistros.

| Mnemônico | Significado                |
| --------- | -------------------------- |
| `srk_loc` | Localização Surrogate Key  |
| `loc_uf`  | Localização UF             |
| `loc_ldd` | Localização Localidade     |
| `loc_reg` | Localização Região         |
| `loc_mun` | Localização Município      |
| `loc_rod` | Localização Rodovia        |
| `loc_nrd` | Localização Número Rodovia |
| `loc_km`  | Localização Quilômetro     |
| `loc_lat` | Localização Latitude       |
| `loc_lng` | Localização Longitude      |

---

### dim_sinistro - Dimensão Sinistro

Armazena características e classificações dos sinistros.

| Mnemônico | Significado              |
| --------- | ------------------------ |
| `srk_sns` | Sinistro Surrogate Key   |
| `sns_tip` | Sinistro Tipo            |
| `sns_cau` | Sinistro Causa           |
| `sns_cap` | Sinistro Causa Principal |
| `sns_ord` | Sinistro Ordem Tipo      |
| `sns_grv` | Sinistro Gravidade       |

---

### dim_via - Dimensão Via

Armazena condições da via e ambiente no momento do sinistro.

| Mnemônico | Significado                |
| --------- | -------------------------- |
| `srk_via` | Via Surrogate Key          |
| `via_cmt` | Via Condição Meteorológica |
| `via_tip` | Via Tipo                   |
| `via_tra` | Via Traçado                |
| `via_sen` | Via Sentido                |
| `via_uso` | Via Uso Solo               |

---

### dim_pessoa - Dimensão Pessoa

Armazena informações sobre pessoas envolvidas nos sinistros.

| Mnemônico | Significado                |
| --------- | -------------------------- |
| `srk_pes` | Pessoa Surrogate Key       |
| `pes_tip` | Pessoa Tipo                |
| `pes_sex` | Pessoa Sexo                |
| `pes_idd` | Pessoa Idade               |
| `pes_fxa` | Pessoa Faixa Etária Ano    |
| `pes_fxc` | Pessoa Faixa Etária Classe |
| `pes_esf` | Pessoa Estado Físico       |

---

### dim_veiculo - Dimensão Veículo

Armazena informações sobre veículos envolvidos nos sinistros.

| Mnemônico | Significado            |
| --------- | ---------------------- |
| `srk_vei` | Veículo Surrogate Key  |
| `vei_id`  | Veículo Identificador  |
| `vei_tip` | Veículo Tipo           |
| `vei_mrc` | Veículo Marca/Modelo   |
| `vei_ano` | Veículo Ano Fabricação |

---

## 📊 Tabela Fato

### fat_sinistro - Fato Sinistros

Tabela central que armazena métricas e relacionamentos com as dimensões.

| Mnemônico | Significado               |
| --------- | ------------------------- |
| `sns_id`  | Sinistro Identificador    |
| `srk_tmp` | Temporal Surrogate Key    |
| `srk_loc` | Localização Surrogate Key |
| `srk_sns` | Sinistro Surrogate Key    |
| `srk_via` | Via Surrogate Key         |
| `srk_pes` | Pessoa Surrogate Key      |
| `srk_vei` | Veículo Surrogate Key     |
| `fat_ils` | Fato Ilesos               |
| `fat_fle` | Fato Feridos Leves        |
| `fat_fgr` | Fato Feridos Graves       |
| `fat_fer` | Fato Feridos              |
| `fat_mrt` | Fato Mortos               |