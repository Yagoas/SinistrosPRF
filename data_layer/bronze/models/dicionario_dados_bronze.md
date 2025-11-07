## INTRODUÇÃO

A Polícia Rodoviária Federal – PRF – atende cerca de 70.000 km de rodovias federais e está distribuída em todo o território nacional, combatendo a criminalidade, prestando auxílio ao cidadão, fiscalizando, autuando e atendendo acidentes.

Entre os anos de 2007 e 2016 o registro de acidentes era realizado por meio do sistema BR-Brasil. Neste sistema o policial responsável pela ocorrência insere dados referentes aos envolvidos, ao local, aos veículos e à dinâmica do acidente.

Em janeiro de 2017 o sistema BR-Brasil foi descontinuado e a PRF passou a utilizar um novo sistema para registro das ocorrências de acidentes de trânsito. Esta versão do dicionário de dados busca fornecer uma descrição sucinta das variáveis presentes nos conjuntos de dados registrados a partir de janeiro de 2017.

## DICIONÁRIO DE VARIÁVEIS (DADOS DO BAT – A PARTIR DE 2017)

| ID | Variável                    | Tipo de dado        | Descrição |
|---:|-----------------------------|---------------------|-----------|
| 1  | id                          | inteiro             | Variável com valores numéricos, representando o identificador do acidente. |
| 2  | pesid                       | inteiro             | Variável com valores numéricos, representando o identificador da pessoa envolvida. |
| 3  | data_inversa                | data                | Data da ocorrência no formato dd/mm/aaaa. |
| 4  | dia_semana                  | texto               | Dia da semana da ocorrência. Ex.: Segunda, Terça, etc. |
| 5  | horario                     | hora                | Horário da ocorrência no formato hh:mm:ss. |
| 6  | uf                          | texto (2)           | Unidade da Federação. Ex.: MG, PE, DF, etc. |
| 7  | br                          | inteiro             | Identificador numérico da BR do acidente. |
| 8  | km                          | decimal(10,2)       | Quilômetro do acidente; mínimo 0,1 km; separador decimal ponto. |
| 9  | municipio                   | texto               | Nome do município de ocorrência do acidente. |
| 10 | causa_principal             | booleano            | Indica se a causa do acidente foi identificada como principal pelo policial. |
| 11 | causa_acidente              | texto               | Causa presumível do acidente, baseada nos vestígios, indícios e provas colhidas. |
| 12 | ordem_tipo_acidente         | inteiro             | Sequência dos eventos sucessivos ocorridos no acidente. |
| 13 | tipo_acidente               | texto               | Tipo de acidente. Ex.: Colisão frontal, Saída de pista, etc. |
| 14 | classificacao_acidente      | texto               | Gravidade: Sem Vítimas, Com Vítimas Feridas, Com Vítimas Fatais, Ignorado. |
| 15 | fase_dia                    | texto               | Fase do dia no momento do acidente. Ex.: Amanhecer, Pleno dia, etc. |
| 16 | sentido_via                 | texto               | Sentido da via considerando o ponto de colisão: Crescente, Decrescente. |
| 17 | condicao_meteorologica      | texto               | Condição meteorológica no momento do acidente: Céu claro, chuva, vento, etc. |
| 18 | tipo_pista                  | texto               | Tipo da pista quanto à quantidade de faixas: Dupla, Simples, Múltipla. |
| 19 | tracado_via                 | texto               | Descrição do traçado da via. |
| 20 | uso_solo                    | texto/booleano      | Característica do local: Urbano=Sim; Rural=Não. |
| 21 | id_veiculo                  | inteiro             | Identificador numérico do veículo envolvido. |
| 22 | tipo_veiculo                | texto               | Tipo do veículo (CTB, Art. 96): Automóvel, Caminhão, Motocicleta, etc. |
| 23 | marca                       | texto               | Marca do veículo. |
| 24 | ano_fabricacao_veiculo      | inteiro             | Ano de fabricação do veículo (aaaa). |
| 25 | tipo_envolvido              | texto               | Tipo de envolvido: condutor, passageiro, pedestre, etc. |
| 26 | estado_fisico               | texto               | Condição do envolvido: morto, ferido leve, etc. |
| 27 | idade                       | inteiro             | Idade do envolvido. O código “-1” indica não informado. |
| 28 | sexo                        | texto               | Sexo do envolvido; “inválido” indica não informado. |
| 29 | ilesos                      | inteiro (0/1)       | Identifica se o envolvido foi classificado como ileso. |
| 30 | feridos_leves               | inteiro (0/1)       | Identifica se o envolvido foi classificado como ferido leve. |
| 31 | feridos_graves              | inteiro (0/1)       | Identifica se o envolvido foi classificado como ferido grave. |
| 32 | mortos                      | inteiro (0/1)       | Identifica se o envolvido foi classificado como morto. |
| 33 | latitude                    | decimal(12,10)      | Latitude do local do acidente em formato geodésico decimal. |
| 34 | longitude                   | decimal(12,10)      | Longitude do local do acidente em formato geodésico decimal. |
| 35 | regional                    | texto               | Superintendência regional da PRF da circunscrição do acidente. |
| 36 | delegacia                   | texto               | Delegacia da PRF da circunscrição do acidente. |
| 37 | uop                         | texto               | Unidade Operacional (UOP) da PRF da circunscrição do acidente. |


