import db
import pandas as pd
import time
import logging
logging.basicConfig(format='%(asctime)s, [OLAP] - %(levelname)s: %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)

logger = logging.getLogger(__name__)

def main():
    while True:
        time.sleep(15)
        consume()

def consume():
    results = db.select_lance_jogador_vencedor()
    for time_id in list(set(results['time_id'])):
        valores = {'time_id': time_id, 'venceu': True}
        resultado_id = db.insert('resultado', valores).inserted_primary_key[0]

        nacionalidades_assists = results[(results['tipo'] == 'ASSISTENCIA') & (results['time_id'] == time_id)] \
            .groupby(['partida_id','nacionalidade']).size().reset_index() \
            .groupby('nacionalidade')[[0]].count()
        total_assists = nacionalidades_assists[0].sum()
        nacionalidades_assists[0] = nacionalidades_assists[0] / total_assists
        for nacionalidade_assist in nacionalidades_assists[0].iteritems():
            nacionalidade, assist = nacionalidade_assist
            valores = {'resultado_id': resultado_id, 'nacionalidade': nacionalidade,
                        'valor': assist}
            db.insert('assistencia_por_nacionalidade', valores)

        nacionalidades_gols = results[(results['tipo'] == 'GOL') & (results['time_id'] == time_id)] \
            .groupby(['partida_id','nacionalidade']).size().reset_index() \
            .groupby('nacionalidade')[[0]].count()
        total_gols = nacionalidades_gols[0].sum()
        nacionalidades_gols[0] = nacionalidades_gols[0] / total_gols
        for nacionalidade_gol in nacionalidades_gols[0].iteritems():
            nacionalidade, gol = nacionalidade_gol
            valores = {'resultado_id': resultado_id, 'nacionalidade': nacionalidade,
                        'valor': gol}
            db.insert('gol_por_nacionalidade', valores)

        posicao_gols = results[(results['tipo'] == 'GOL') & (results['time_id'] == time_id)] \
                .groupby(['partida_id','posicao']).size().reset_index() \
                .groupby('posicao')[[0]].count()
        total_gols = posicao_gols[0].sum()
        posicao_gols[0] = posicao_gols[0] / total_gols
        for posicao_gol in posicao_gols[0].iteritems():
            posicao, gol = posicao_gol
            valores = {'resultado_id': resultado_id, 'posicao': posicao,
                        'valor': gol}
            db.insert('gol_por_posicao', valores)

        posicao_assists = results[(results['tipo'] == 'ASSISTENCIA') & (results['time_id'] == time_id)] \
            .groupby(['partida_id','posicao']).size().reset_index() \
            .groupby('posicao')[[0]].count()
        total_assists = posicao_assists[0].sum()
        posicao_assists[0] = posicao_assists[0] / total_assists
        for posicao_assist in posicao_assists[0].iteritems():
            posicao, assist = posicao_assist
            valores = {'resultado_id': resultado_id, 'posicao': posicao,
                        'valor': assist}
            db.insert('assistencia_por_posicao', valores)

    results = db.select_lance_jogador_perdedor()
    for time_id in list(set(results['time_id'])):
        valores = {'time_id': time_id, 'venceu': False}
        resultado_id = db.insert('resultado', valores).inserted_primary_key[0]

        nacionalidades_assists = results[(results['tipo'] == 'ASSISTENCIA') & (results['time_id'] == time_id)] \
            .groupby(['partida_id','nacionalidade']).size().reset_index() \
            .groupby('nacionalidade')[[0]].count()
        total_assists = nacionalidades_assists[0].sum()
        nacionalidades_assists[0] = nacionalidades_assists[0] / total_assists
        for nacionalidade_assist in nacionalidades_assists[0].iteritems():
            nacionalidade, assist = nacionalidade_assist
            valores = {'resultado_id': resultado_id, 'nacionalidade': nacionalidade,
                        'valor': assist}
            db.insert('assistencia_por_nacionalidade', valores)

        nacionalidades_gols = results[(results['tipo'] == 'GOL') & (results['time_id'] == time_id)] \
            .groupby(['partida_id','nacionalidade']).size().reset_index() \
            .groupby('nacionalidade')[[0]].count()
        total_gols = nacionalidades_gols[0].sum()
        nacionalidades_gols[0] = nacionalidades_gols[0] / total_gols
        for nacionalidade_gol in nacionalidades_gols[0].iteritems():
            nacionalidade, gol = nacionalidade_gol
            valores = {'resultado_id': resultado_id, 'nacionalidade': nacionalidade,
                        'valor': gol}
            db.insert('gol_por_nacionalidade', valores)

        posicao_gols = results[(results['tipo'] == 'GOL') & (results['time_id'] == time_id)] \
                .groupby(['partida_id','posicao']).size().reset_index() \
                .groupby('posicao')[[0]].count()
        total_gols = posicao_gols[0].sum()
        posicao_gols[0] = posicao_gols[0] / total_gols
        for posicao_gol in posicao_gols[0].iteritems():
            posicao, gol = posicao_gol
            valores = {'resultado_id': resultado_id, 'posicao': posicao,
                        'valor': gol}
            db.insert('gol_por_posicao', valores)

        posicao_assists = results[(results['tipo'] == 'ASSISTENCIA') & (results['time_id'] == time_id)] \
            .groupby(['partida_id','posicao']).size().reset_index() \
            .groupby('posicao')[[0]].count()
        total_assists = posicao_assists[0].sum()
        posicao_assists[0] = posicao_assists[0] / total_assists
        for posicao_assist in posicao_assists[0].iteritems():
            posicao, assist = posicao_assist
            valores = {'resultado_id': resultado_id, 'posicao': posicao,
                        'valor': assist}
            db.insert('assistencia_por_posicao', valores)
    logger.info(results)

if __name__ == "__main__":
    main()