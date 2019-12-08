import os 
import sqlalchemy as db
import pandas as pd
# import logging
# logging.basicConfig(format='%(asctime)s [OLTP] - %(levelname)s: %(message)s',
#                     datefmt='%H:%M:%S',
#                     level=logging.DEBUG)
# logging.getLogger('sqlalchemy.engine').setLevel(logging.DEBUG)


POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")

engine = db.create_engine(f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@db/{POSTGRES_DB}")

def get_db():
    return db

def create_tables():
    connection = engine.connect()
    metadata = db.MetaData()

    # time = db.Table('time', metadata,
    #             db.Column('id', db.Integer(), primary_key=True),
    #             db.Column('name', db.String(255), nullable=False),
    #             db.Column('salary', db.Float(), default=100.0),
    #             db.Column('active', db.Boolean(), default=True)
    #             )

    #OLTP
    time = db.Table('time', metadata,
                db.Column('id', db.Integer(), primary_key=True))

    jogador = db.Table('jogador', metadata,
                db.Column('id', db.Integer(), primary_key=True),
                db.Column('time_id', db.ForeignKey('time.id')),
                db.Column('nacionalidade', db.String(255), nullable=False),
                db.Column('posicao', db.String(255), nullable=False)
                )
    partida = db.Table('partida', metadata,
                    db.Column('id', db.Integer(), primary_key=True),
                    db.Column('vencedor_id', db.ForeignKey('time.id')),
                    db.Column('perdedor_id', db.ForeignKey('time.id')),
                )
    lance = db.Table('lance', metadata,
                    db.Column('id', db.Integer(), primary_key=True, autoincrement=True),
                    db.Column('jogador_id', db.ForeignKey('jogador.id')),
                    db.Column('partida_id', db.ForeignKey('partida.id')),
                    db.Column('tipo', db.String(255), nullable=False),
                )
    #OLAP
    resultado = db.Table('resultado', metadata,
                    db.Column('id', db.Integer(), primary_key=True, autoincrement=True),
                    db.Column('time_id', db.ForeignKey('time.id')),
                    db.Column('venceu', db.Boolean(), default=True)
                )

    gol_por_posicao = db.Table('gol_por_posicao', metadata,
                    db.Column('id', db.Integer(), primary_key=True, autoincrement=True),
                    db.Column('resultado_id', db.ForeignKey('resultado.id')),
                    db.Column('posicao', db.String(255), nullable=False),
                    db.Column('valor', db.Float(), nullable=False)
                )

    assistencia_por_posicao = db.Table('assistencia_por_posicao', metadata,
                    db.Column('id', db.Integer(), primary_key=True, autoincrement=True),
                    db.Column('resultado_id', db.ForeignKey('resultado.id')),
                    db.Column('posicao', db.String(255), nullable=False),
                    db.Column('valor', db.Float(), nullable=False)
                )
    
    gol_por_nacionalidade = db.Table('gol_por_nacionalidade', metadata,
                    db.Column('id', db.Integer(), primary_key=True, autoincrement=True),
                    db.Column('resultado_id', db.ForeignKey('resultado.id')),
                    db.Column('nacionalidade', db.String(255), nullable=False),
                    db.Column('valor', db.Float(), nullable=False),
                )

    assistencia_por_nacionalidade = db.Table('assistencia_por_nacionalidade', metadata,
                    db.Column('id', db.Integer(), primary_key=True, autoincrement=True),
                    db.Column('resultado_id', db.ForeignKey('resultado.id')),
                    db.Column('nacionalidade', db.String(255), nullable=False),
                    db.Column('valor', db.Float(), nullable=False),
                )

    metadata.create_all(engine) #Creates the table

def seed():
    connection = engine.connect()
    metadata = db.MetaData()

    times_csv = pd.read_csv('./data/team_info.csv')
    #criei esse arquivo a partir do join de player_info e game_skater, 
    # removendo duplicadas e missing values
    jogadores_csv = pd.read_csv('./data/players_with_team.csv')
    partidas_csv = pd.read_csv('./data/partidas.csv')

    time = db.Table('time', metadata, autoload=True, autoload_with=engine)
    times = [{'id': t['team_id']} for i,t in times_csv.iterrows()]
    
    query = db.insert(time) 
    result = connection.execute(query,times)

    jogador = db.Table('jogador', metadata, autoload=True, autoload_with=engine)
    jogadores = [{'id': j['player_id'], 'time_id': j['team_id'],
                'nacionalidade': j['nationality'],
                'posicao': j['primaryPosition']} for i,j in jogadores_csv.iterrows()]
    query = db.insert(jogador) 
    result = connection.execute(query,jogadores)

    partida = db.Table('partida', metadata, autoload=True, autoload_with=engine)
    partidas = [{'id': int(p['game_id']), 'vencedor_id': int(p['vencedor_id']),
                'perdedor_id': int(p['perdedor_id'])} for i,p in partidas_csv.iterrows()]
    query = db.insert(partida) 
    result = connection.execute(query, partidas)
    connection.close()

def insert_lance(lance):
    connection = engine.connect()
    metadata = db.MetaData()
    
    lancetbl = db.Table('lance', metadata, autoload=True, autoload_with=engine)
    query = db.insert(lancetbl).values(**lance) 
    result = connection.execute(query)
    connection.close()
    return result

def select_lance_jogador_vencedor():
    connection = engine.connect()
    metadata = db.MetaData()

    lancetbl = db.Table('lance', metadata, autoload=True, autoload_with=engine)
    jogadortbl = db.Table('jogador', metadata, autoload=True, autoload_with=engine)
    partidatbl = db.Table('partida', metadata, autoload=True, autoload_with=engine)
    
    query = db.select([lancetbl, jogadortbl])
    query = query.select_from(
        lancetbl.join(jogadortbl, lancetbl.columns.jogador_id == jogadortbl.columns.id) \
                .join(partidatbl, lancetbl.columns.partida_id == partidatbl.columns.id)) \
                .where(jogadortbl.columns.time_id == partidatbl.columns.vencedor_id)
    results = connection.execute(query).fetchall()
    connection.close()
    df = pd.DataFrame(results)
    df.columns = results[0].keys()
    return df

def select_lance_jogador_perdedor():
    connection = engine.connect()
    metadata = db.MetaData()

    lancetbl = db.Table('lance', metadata, autoload=True, autoload_with=engine)
    jogadortbl = db.Table('jogador', metadata, autoload=True, autoload_with=engine)
    partidatbl = db.Table('partida', metadata, autoload=True, autoload_with=engine)
    
    query = db.select([lancetbl, jogadortbl])
    query = query.select_from(
        lancetbl.join(jogadortbl, lancetbl.columns.jogador_id == jogadortbl.columns.id) \
                .join(partidatbl, lancetbl.columns.partida_id == partidatbl.columns.id)) \
                .where(jogadortbl.columns.time_id == partidatbl.columns.perdedor_id)
    results = connection.execute(query).fetchall()
    connection.close()
    df = pd.DataFrame(results)
    df.columns = results[0].keys()
    return df

def select_olap(tabela, venceu):
    connection = engine.connect()
    metadata = db.MetaData()

    resultadotbl = db.Table('resultado', metadata, autoload=True, autoload_with=engine)
    tbl = db.Table(tabela, metadata, autoload=True, autoload_with=engine)
    
    query = db.select([tbl])
    query = query.select_from(
        resultadotbl.join(tbl, resultadotbl.columns.id == tbl.columns.resultado_id)) \
        .where(resultadotbl.columns.venceu == venceu)
    results = connection.execute(query).fetchall()
    connection.close()
    df = pd.DataFrame(results)
    df.columns = results[0].keys()
    return df

def insert(table, value):
    connection = engine.connect()
    metadata = db.MetaData()
    tbl = db.Table(table, metadata, autoload=True, autoload_with=engine)
    query = db.insert(tbl).values(**value) 
    result = connection.execute(query)
    connection.close()
    return result

# r[r['tipo'] == 'ASSISTENCIA'].groupby(['partida_id','nacionalidade']).size().reset_index().groupby('nacionalidade')[[0]].count()
# resp = []
# for i,lance in t.iterrows():
#     for goal in range(lance['goals']):
#         resp.append({'jogador_id': lance['player_id'], 'partida_id': lance['game_id'], 'tipo': 'GOL'})
#     for assistencia in range(lance['assists']):
#         resp.append({'jogador_id': lance['player_id'], 'partida_id': lance['game_id'], 'tipo': 'ASSISTENCIA'})