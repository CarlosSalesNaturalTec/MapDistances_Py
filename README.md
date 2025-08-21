# Analisador de Distâncias e IDH para Municípios da Bahia

Este script em Python gera um arquivo CSV contendo dados consolidados para os 417 municípios do estado da Bahia, Brasil.

## O que ele faz?

O script coleta, processa e combina as seguintes informações para cada município:
1.  **Nome do Município**: Nome oficial.
2.  **Código IBGE**: Código de 7 dígitos do Instituto Brasileiro de Geografia e Estatística.
3.  **IDHM 2010**: Índice de Desenvolvimento Humano Municipal referente ao ano de 2010.
4.  **Distância Geodésica**: Distância em linha reta (km) da sede do município até a capital, Salvador.
5.  **Distância Rodoviária**: Distância estimada por rodovia (km) até Salvador.
6.  **Duração da Viagem**: Tempo estimado de viagem (em horas) de carro até Salvador.
7.  **Detalhes da Origem/Destino**: Endereço e coordenadas exatas do ponto de partida em Salvador e do ponto de chegada no município.

## Funcionalidades

- **Agregação de Dados**: Combina informações de 4 fontes de dados públicas diferentes em uma única tabela.
- **Sistema de Cache Inteligente**: Na primeira execução, os dados obtidos da web (coordenadas, rotas, IDH) são salvos em arquivos locais no diretório `.cache_ba`. Em execuções futuras, o script lê diretamente do cache, tornando o processo muito mais rápido e evitando sobrecarregar as APIs públicas.
- **Robustez**: Possui uma lógica de fallback para encontrar as coordenadas geográficas dos municípios e para usar o nome do município como destino caso a API de rotas não retorne um endereço específico.
- **Segurança**: Interrompe a execução automaticamente se detectar um número excessivo de respostas inválidas da API de rotas, evitando execuções longas e com falhas.
- **Flexibilidade**: Permite, via argumentos de linha de comando, customizar o nome do arquivo de saída e pular a etapa de cálculo de rotas, que é a mais demorada.

## Pré-requisitos

É necessário ter o Python 3 instalado. Para instalar as dependências, execute o seguinte comando no seu terminal:

```sh
pip install pandas requests beautifulsoup4 tqdm lxml
```

## Como Usar

Abra um terminal na pasta do projeto e execute o script.

**Execução padrão:**

```sh
# Gera o arquivo distancias_bahia.csv no diretório atual
python ba_417_idh_distancias.py
```

### Argumentos de Linha de Comando

- `--out`: Especifica um nome e caminho diferente para o arquivo de saída.
  ```sh
  python ba_417_idh_distancias.py --out saida/dados_bahia.csv
  ```

- `--no-osrm`: Pula o cálculo de distância rodoviária. É útil para uma execução mais rápida, gerando apenas a distância geodésica.
  ```sh
  python ba_417_idh_distancias.py --no-osrm
  ```

## Como Funciona

O fluxo de trabalho do script é o seguinte:

1.  **Coleta de Municípios**: Obtém a lista oficial dos 417 municípios e seus códigos da API de Localidades do IBGE.
2.  **Extração de IDH**: Faz o scraping (raspagem) de uma tabela HTML de uma página da Wikipedia para obter o IDHM de 2010 de cada cidade. O resultado é salvo em `idhm2010.json`.
3.  **Geocodificação**: Utiliza a API do Nominatim (baseada no OpenStreetMap) para encontrar as coordenadas (latitude e longitude) de Salvador (Capital do estado) e de cada um dos outros municípios. Os resultados são salvos em `geocode.json`.
4.  **Cálculo de Distâncias e Rota**:
    - **Geodésica**: Usa a fórmula de Haversine para calcular a distância em linha reta.
    - **Rodoviária**: Envia as coordenadas de origem (Salvador) e destino para a API pública do OSRM, que retorna um objeto completo da rota, incluindo distância, duração e detalhes dos pontos de partida/chegada. Os resultados são salvos em `route.json`.
5.  **Geração do CSV**: Consolida todos os dados em um DataFrame do pandas e o exporta para um arquivo CSV limpo e organizado.

## Fontes dos Dados

- **Municípios e Códigos**: API de Localidades do IBGE
- **IDHM 2010**: Wikipedia
- **Geocodificação**: Nominatim (OpenStreetMap)
- **Roteamento**: Project OSRM

## Exemplo de Saída (`distancias_bahia.csv`)

O arquivo gerado terá a seguinte estrutura:

```csv
municipio,codigo_ibge,idhm_2010,dist_km_geodesica_salvador,dist_km_rodoviaria_salvador,duracao_h_viagem,origem_endereco,origem_coords,destino_municipio_endereco,destino_municipio_coords
Abaíra,2900108,0.597,342.8,534.4,7.65,"Rua da Misericórdia","-12.974331, -38.512191",Abaíra,"-13.251333, -41.664738"
Abaré,2900207,0.556,477.4,552.0,7.82,"Rua da Misericórdia","-12.974331, -38.512191",Abaré,"-8.721844, -39.114441"
...
```