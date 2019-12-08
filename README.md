# HTAP 2019.2

### Objetivo
A partir de registros de “lances”’ de um jogo na NHL registrados num OLTP,  extrair estatísticas em tempo real, como:

* Percentual de gols que jogadores de uma posição X fizeram com relação ao total de gols de uma equipe em uma partida
* Percentual de assistências que jogadores de uma posição X fizeram com relação ao total de gols de uma equipe em uma partida
* Percentual de assistências que jogadores de uma nacionalidade X fizeram com relação ao total de gols de uma equipe em uma partida
* Percentual de gols que jogadores de uma nacionalidade X fizeram com relação ao total de gols de uma equipe em uma partida

Então, armazenar essas estatísticas num BD OLAP e a partir desses dados ser capaz de realizar predições de quem será o vencedor do jogo com um modelo de Aprendizado de Máquina que vá evoluindo com novos exemplos.

### Dados Originais
Os dados utilizados estão disponíveis em https://www.kaggle.com/martinellis/nhl-game-data). São vários arquivos csv, onde os mais importantes são:

_player_info.csv_ : onde existe a informação da posição que o jogador atua

_game_skater_stats.csv_ : onde estão as informações de quantas assistências, gols, chutes, hits e tempo no gelo de um player em uma partida

_game_teams_stats.csv_ : tem a informação se o time venceu ou perdeu uma partida

### Dados transformados

_lances.csv_ : arquivo gerado a partir de game_skater_stats.csv, gerando uma linha para a quantidade de assistências e gols que consta na linha do arquivo original

_partidas.csv_ : também gerado a partir de game_skater_stats.csv criando apenas um registro por partida informando a equipe vencedora e perdedora para ela

_players_with_teams.csv_ : para facilitar a identificação de jogadores de um time

### Metodologia
Criar um motor que a partir dos dados do arquivo _game_skater_stats.csv_ vá gerando lances do jogo, contendo informações como: jogador, tipo de jogada e o timestamp. Esses lances de jogo serão armazenados num OLTP.

A partir dos registros no OLTP, OLAP será atualizado em tempo real, criando estatísticas como: percentual dos gols, assistências, chutes, etc, de uma equipe que foram feitos por jogadores de uma posição X.

Esses registros servirão para que um modelo de de Aprendizado de Máquina realize predições de qual equipe vencerá o jogo, atualizando também as informações do OLAP. Esse modelo também deverá melhorar conforme as partidas terminem.

# Ferramentas e bibliotecas

* Docker e docker-compose
* Postgres
* sklearn e pandas
* sqlalchemy

# Instalação

Clonando o repositório
* git clone https://github.com/danieljunior/htap_final.git

Para criar a imagem
* docker-compose build

Para subir a aplicação (acrescentar -d caso queira colocar em segundo plano)
* docker-compose up

Erros:
* Ao finalizar a aplicação com CTRL+C e tentar subir novamente, pode ocorrer algum erro. Neste caso execute: _docker-compose down -v_

# Funcionamento

A aplicação inicia criando uma instância do Postgres em um container separado. Após isso a criação do schema e a pré-população da base é executada (src/config.py).

O processo responsável pela geração e registro dos lances de jogo executa a cada 1 segundo(src/oltp.py)

O processo responsável por agrupar os dados, gerar as estatísticas e registrar no banco executa a cada 15 segundos (src/olap.py).

A cada 30 segundos é executado o processo responsável por treinar/refinar o modelo com 80% dos dados gerados pelo OLAP e, testar e gerar métricas com 20% dos dados.

Caso a aplicação não seja iniciada em segundo plano, o terminal exibe o log de cada processo. Cada processo tem sua saída com seu respectivo rótulo, a saber: CONFIG, OLTP, OLAP, PREDITOR.

# Utilidades

Para conectar na instância do banco Postgres:
* docker exec -it htap_final_db_1 psql -U testusr testdb

Para conectar ao shell do container que executa os processos
* docker exec -it htap_final_app_1 python