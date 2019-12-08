import db
import copy
import pandas as pd
import time
from sklearn import linear_model
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report

import logging
logging.basicConfig(format='%(asctime)s, [PREDITOR] - %(levelname)s: %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)

logger = logging.getLogger(__name__)

def main():
    while True:
        time.sleep(30)
        logger.info('Gerando dados de treinamento...')
        data = generate_data()
        logger.info(set(data.VENCEU))

        clf = linear_model.SGDClassifier(max_iter=1000, tol=1e-3)
        logger.info('Treinando...')
        clf = train(clf, data)

def train(clf, data):
    y = data.pop('VENCEU').values
    X_train, X_test, y_train, y_test = train_test_split(
        data, y, test_size=0.20, random_state=42)
    
    clf.partial_fit(X_train, y_train, classes=[True, False])
    
    predict = clf.predict(X_test)

    predict = [int(p) for p in predict]
    y_test = [int(yy) for yy in y_test]
   
    logger.info(classification_report(y_test, predict))
    
    return clf

def generate_data():
    players = pd.read_csv('./data/player_info.csv')
    nationalities = list(set(players.nationality.dropna()))
    positions = list(set(players.primaryPosition.dropna()))
    cols = [n + '_ASSISTS' for n in nationalities] + [n + '_GOLS' for n in nationalities] + \
            [p + '_ASSISTS' for p in positions] + [p + '_GOLS' for p in positions]
    cols_dict = {}
    for col in cols:
        cols_dict[col] = 0
    cols_dict['VENCEU'] = True
    cols_dict2 = copy.deepcopy(cols_dict)
    cols_dict2['VENCEU'] = False
    data = pd.DataFrame([], columns = cols)
    
    assist_nac_venceu = db.select_olap('assistencia_por_nacionalidade', True)
    gols_nac_venceu = db.select_olap('gol_por_nacionalidade', True)
    assist_pos_venceu = db.select_olap('assistencia_por_posicao', True)
    gols_pos_venceu = db.select_olap('gol_por_posicao', True)

    resultados = list(set(list(assist_nac_venceu.resultado_id) + list(gols_nac_venceu.resultado_id) + \
                list(assist_pos_venceu.resultado_id) + list(gols_pos_venceu.resultado_id)))
    for id in resultados:

        for i, a in assist_nac_venceu[assist_nac_venceu['resultado_id'] == id].iterrows():
            cols_dict[a['nacionalidade'] + '_ASSISTS'] = a['valor']
        for i, a in assist_pos_venceu[assist_pos_venceu['resultado_id'] == id].iterrows():
            cols_dict[a['posicao'] + '_ASSISTS'] = a['valor']
        for i, g in gols_pos_venceu[gols_pos_venceu['resultado_id'] == id].iterrows():
            cols_dict[g['posicao'] + '_GOLS'] = g['valor']
        for i, g in gols_nac_venceu[gols_nac_venceu['resultado_id'] == id].iterrows():
            cols_dict[g['nacionalidade'] + '_GOLS'] = g['valor']
        
        row = pd.DataFrame.from_records([cols_dict])
        data = pd.concat([data, row], sort=False)
    
    assist_nac_perdeu = db.select_olap('assistencia_por_nacionalidade', False)
    gols_nac_perdeu = db.select_olap('gol_por_nacionalidade', False)
    assist_pos_perdeu = db.select_olap('assistencia_por_posicao', False)
    gols_pos_perdeu = db.select_olap('gol_por_posicao', False)

    resultados = list(set(list(assist_nac_perdeu.resultado_id) + list(gols_nac_perdeu.resultado_id) + \
                list(assist_pos_perdeu.resultado_id) + list(gols_pos_perdeu.resultado_id)))
        
    for id in resultados:

        for i, a in assist_nac_perdeu[assist_nac_perdeu['resultado_id'] == id].iterrows():
            cols_dict2[a['nacionalidade'] + '_ASSISTS'] = a['valor']
        for i, a in assist_pos_perdeu[assist_pos_perdeu['resultado_id'] == id].iterrows():
            cols_dict2[a['posicao'] + '_ASSISTS'] = a['valor']
        for i, g in gols_pos_perdeu[gols_pos_perdeu['resultado_id'] == id].iterrows():
            cols_dict2[g['posicao'] + '_GOLS'] = g['valor']
        for i, g in gols_nac_perdeu[gols_nac_perdeu['resultado_id'] == id].iterrows():
            cols_dict2[g['nacionalidade'] + '_GOLS'] = g['valor']
        
        row = pd.DataFrame.from_records([cols_dict2])
        data = pd.concat([data, row], sort=False)
    return data
    

if __name__ == "__main__":
    main()