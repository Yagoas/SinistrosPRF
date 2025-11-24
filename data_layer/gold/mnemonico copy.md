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

| Mnemônico | Significado             | Tipo        | Descrição                                       |
| --------- | ----------------------- | ----------- | ----------------------------------------------- |
| `srk_tmp` | Temporal Surrogate Key  | INTEGER     | Chave substituta única                          |
| `tmp_dta` | Temporal Data           | DATE        | Data do sinistro                                |
| `tmp_ano` | Temporal Ano            | INTEGER     | Ano do sinistro                                 |
| `tmp_hra` | Temporal Hora           | INTEGER     | Hora do sinistro (0-23)                         |
| `tmp_dsm` | Temporal Dia Semana     | VARCHAR(13) | Nome do dia da semana                           |
| `tmp_per` | Temporal Período        | VARCHAR(9)  | Período do dia (Manhã, Tarde, Noite, Madrugada) |
| `tmp_psm` | Temporal Período Semana | VARCHAR(15) | Período da semana (Dia útil, Fim de semana)     |

---

### dim_localizacao - Dimensão Localização

Armazena informações geográficas dos sinistros.

| Mnemônico | Significado                | Tipo           | Descrição                            |
| --------- | -------------------------- | -------------- | ------------------------------------ |
| `srk_loc` | Localização Surrogate Key  | INTEGER        | Chave substituta única               |
| `loc_uf`  | Localização UF             | VARCHAR(2)     | Sigla da Unidade Federativa          |
| `loc_ldd` | Localização Localidade     | VARCHAR(19)    | Nome completo do estado              |
| `loc_reg` | Localização Região         | VARCHAR(12)    | Região geográfica (Norte, Sul, etc.) |
| `loc_mun` | Localização Município      | VARCHAR(100)   | Nome do município                    |
| `loc_rod` | Localização Rodovia        | VARCHAR(6)     | Código da rodovia (ex: BR-040)       |
| `loc_nrd` | Localização Número Rodovia | VARCHAR(5)     | Número da rodovia                    |
| `loc_km`  | Localização Quilômetro     | DECIMAL(10,2)  | Quilômetro da rodovia                |
| `loc_lat` | Localização Latitude       | DECIMAL(12,10) | Coordenada de latitude               |
| `loc_lng` | Localização Longitude      | DECIMAL(12,10) | Coordenada de longitude              |

---

### dim_sinistro - Dimensão Sinistro

Armazena características e classificações dos sinistros.

| Mnemônico | Significado              | Tipo         | Descrição                                       |
| --------- | ------------------------ | ------------ | ----------------------------------------------- |
| `srk_sns` | Sinistro Surrogate Key   | INTEGER      | Chave substituta única                          |
| `sns_tip` | Sinistro Tipo            | VARCHAR(50)  | Tipo do sinistro (Colisão, Atropelamento, etc.) |
| `sns_cau` | Sinistro Causa           | VARCHAR(100) | Causa do sinistro                               |
| `sns_cap` | Sinistro Causa Principal | VARCHAR(5)   | Código da causa principal                       |
| `sns_ord` | Sinistro Ordem Tipo      | INTEGER      | Ordem de classificação do tipo                  |
| `sns_grv` | Sinistro Gravidade       | VARCHAR(20)  | Gravidade (Com morto, Com ferido, etc.)         |

---

### dim_via - Dimensão Via

Armazena condições da via e ambiente no momento do sinistro.

| Mnemônico | Significado                | Tipo         | Descrição                               |
| --------- | -------------------------- | ------------ | --------------------------------------- |
| `srk_via` | Via Surrogate Key          | INTEGER      | Chave substituta única                  |
| `via_cmt` | Via Condição Meteorológica | VARCHAR(30)  | Condição do tempo (Chuva, Sol, etc.)    |
| `via_tip` | Via Tipo                   | VARCHAR(20)  | Tipo de pista (Simples, Dupla, etc.)    |
| `via_tra` | Via Traçado                | VARCHAR(150) | Traçado da via (Reta, Curva, etc.)      |
| `via_sen` | Via Sentido                | VARCHAR(20)  | Sentido da via (Crescente, Decrescente) |
| `via_uso` | Via Uso Solo               | VARCHAR(6)   | Tipo de uso do solo (Urbano, Rural)     |

---

### dim_pessoa - Dimensão Pessoa

Armazena informações sobre pessoas envolvidas nos sinistros.

| Mnemônico | Significado                | Tipo        | Descrição                                          |
| --------- | -------------------------- | ----------- | -------------------------------------------------- |
| `srk_pes` | Pessoa Surrogate Key       | INTEGER     | Chave substituta única                             |
| `pes_tip` | Pessoa Tipo                | VARCHAR(20) | Tipo de envolvido (Condutor, Passageiro, Pedestre) |
| `pes_sex` | Pessoa Sexo                | VARCHAR(20) | Sexo da pessoa                                     |
| `pes_idd` | Pessoa Idade               | INTEGER     | Idade da pessoa                                    |
| `pes_fxa` | Pessoa Faixa Etária Ano    | VARCHAR(13) | Faixa etária (0-9, 10-19, etc.)                    |
| `pes_fxc` | Pessoa Faixa Etária Classe | VARCHAR(20) | Classe etária (Criança, Adulto, Idoso)             |
| `pes_esf` | Pessoa Estado Físico       | VARCHAR(20) | Estado físico (Ileso, Ferido, Morto)               |

---

### dim_veiculo - Dimensão Veículo

Armazena informações sobre veículos envolvidos nos sinistros.

| Mnemônico | Significado            | Tipo        | Descrição                                      |
| --------- | ---------------------- | ----------- | ---------------------------------------------- |
| `srk_vei` | Veículo Surrogate Key  | INTEGER     | Chave substituta única                         |
| `vei_id`  | Veículo Identificador  | BIGINT      | Identificador original do veículo              |
| `vei_tip` | Veículo Tipo           | VARCHAR(20) | Tipo de veículo (Automóvel, Motocicleta, etc.) |
| `vei_mrc` | Veículo Marca/Modelo   | VARCHAR(50) | Marca e modelo do veículo                      |
| `vei_ano` | Veículo Ano Fabricação | INTEGER     | Ano de fabricação do veículo                   |

---

## 📊 Tabela Fato

### fat_sinistro - Fato Sinistros

Tabela central que armazena métricas e relacionamentos com as dimensões.

| Mnemônico | Significado               | Tipo    | Descrição                          |
| --------- | ------------------------- | ------- | ---------------------------------- |
| `sns_id`  | Sinistro Identificador    | BIGINT  | Identificador original do sinistro |
| `srk_tmp` | Temporal Surrogate Key    | INTEGER | FK para dim_temporal               |
| `srk_loc` | Localização Surrogate Key | INTEGER | FK para dim_localizacao            |
| `srk_sns` | Sinistro Surrogate Key    | INTEGER | FK para dim_sinistro               |
| `srk_via` | Via Surrogate Key         | INTEGER | FK para dim_via                    |
| `srk_pes` | Pessoa Surrogate Key      | INTEGER | FK para dim_pessoa                 |
| `srk_vei` | Veículo Surrogate Key     | INTEGER | FK para dim_veiculo                |
| `fat_ils` | Fato Ilesos               | INTEGER | Quantidade de pessoas ilesas       |
| `fat_fle` | Fato Feridos Leves        | INTEGER | Quantidade de feridos leves        |
| `fat_fgr` | Fato Feridos Graves       | INTEGER | Quantidade de feridos graves       |
| `fat_fer` | Fato Feridos              | INTEGER | Total de feridos (leves + graves)  |
| `fat_mrt` | Fato Mortos               | INTEGER | Quantidade de mortos               |

---

## 📖 Glossário de Termos

- **SRK (Surrogate Key)**: Chave artificial gerada automaticamente para identificar unicamente cada registro em uma dimensão. Não tem significado de negócio.
- **FK (Foreign Key)**: Chave estrangeira que referencia a chave primária de outra tabela.
- **DIM**: Prefixo para tabelas de dimensão no modelo Star Schema.
- **FAT**: Prefixo para tabelas fato no modelo Star Schema.
- **TMP**: Abreviação de "Temporal" - relacionado a tempo/data.
- **LOC**: Abreviação de "Localização" - relacionado a geografia.
- **SNS**: Abreviação de "Sinistro" - evento de acidente.
- **VIA**: Relacionado a rodovia/estrada.
- **PES**: Abreviação de "Pessoa" - indivíduos envolvidos.
- **VEI**: Abreviação de "Veículo" - meios de transporte.

---

## 🎯 Padrões de Nomenclatura

### Tabelas

- **Dimensões**: `dim_<nome_descritivo>` (ex: `dim_temporal`, `dim_localizacao`, `dim_pessoa`)
- **Fatos**: `fat_<nome_descritivo>` (ex: `fat_sinistro`)

### Colunas

- **Chaves Substitutas**: `srk_<prefixo>` (ex: `srk_tmp`, `srk_loc`, `srk_pes`)
- **Atributos**: `<prefixo>_<abreviação>` (ex: `tmp_dta`, `loc_uf`, `pes_sex`)
- **Métricas**: `fat_<abreviação>` (ex: `fat_ils`, `fat_mrt`)