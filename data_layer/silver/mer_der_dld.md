## DIAGRAMA ENTIDADE-RELACIONAMENTO (DER)

O Diagrama Entidade-Relacionamento (DER) é uma representação gráfica do modelo de dados em nível conceitual. Foca em entidades e seus atributos, sem detalhar aspectos de implementação física (tipos de dados, índices, PK/FK físicas).

### DER conceitual do projeto (Camada Silver)

![DER da camada Silver](../../../assets/der.jpeg)

- **Entidade**: TB_SINISTROS_SILVER
- **Identificadores de negócio (conceituais)**: `sinistroId`, `idEnvolvido`, `veiculoId`
- **Relacionamentos**: Não representados nesta camada; a entidade consolida informações de sinistro, pessoa, veículo, tempo e localização para análise.

Observação: A camada Silver adota o estilo lakehouse com uma única entidade agregadora. Eventuais normalizações (ex.: separar Pessoa/Veículo/Tempo/Local) pertencem a um modelo lógico/físico fora do escopo deste DER conceitual.


## DIAGRAMA LÓGICO DE DADOS (DLD)

O Diagrama Lógico de Dados descreve a estrutura lógica do banco: tabelas, colunas, tipos, chaves e índices. Ele detalha como o modelo conceitual é representado logicamente, sem se prender a particularidades de implementação física específicas do SGBD.

### DLD do projeto (Camada Silver)

![DLD da camada Silver](../../../assets/dld.jpeg)

Observação: o DLD reflete a estrutura lógica consumida na camada Silver (tabela única). Normalizações adicionais podem ser aplicadas em modelos lógico/dimensionais específicos para BI, quando necessário.


## MODELO ENTIDADE-RELACIONAMENTO

O Modelo Entidade-Relacionamento (MER) é uma representação conceitual dos dados de um domínio. Ele descreve entidades (coisas de interesse), seus atributos (características) e os relacionamentos entre elas. É amplamente utilizado para comunicar e projetar estruturas de dados de forma clara, independente da tecnologia de armazenamento.

### MER do projeto (Camada Silver)

- **Entidades**
  - TB_SINISTROS_SILVER

- **Atributos**
  - TB_SINISTROS_SILVER(<u>sinistroId</u>, <u>idEnvolvido</u>, <u>veiculoId</u>, data, horario, dataHora, ano, hora, diaSemana, periodo, periodoSemana, uf, localidade, regiao, municipio, rodovia, rodoviaNumero, quilometro, latitude, longitude, sinistroTipo, sinistroCausa, sinistroCausaPrincipal, sinistroOrdemTipo, condicaoMeteorologica, viaTipo, viaTracado, viaSentido, usoSolo, envolvidoIdade, envolvidoSexo, envolvidoTipo, estadoFisico, faixaEtariaAno, faixaEtariaClasse, veiculoTipo, veiculoMarcaModelo, veiculoAnoFabricacao, ilesos, feridosLeves, feridosGraves, feridos, mortos, gravidade, ups)

- **Relacionamentos**
  - Não possui

Nota: Nesta camada Silver adotamos uma tabela única (estilo lakehouse) para consumo analítico; por isso, o MER possui apenas uma entidade e nenhum relacionamento explícito entre entidades.


