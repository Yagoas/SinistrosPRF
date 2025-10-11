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


