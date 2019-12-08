import time
import db
import pandas as pd
import logging
logging.basicConfig(format='%(asctime)s [OLTP] - %(levelname)s: %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)

logger = logging.getLogger(__name__)

def main():
    while True:
        produce()

def produce():
    lances_csv = pd.read_csv('./data/lances.csv')
    for i, lance in lances_csv.iterrows():
        del lance["Unnamed: 0.1"]
        del lance["Unnamed: 0"]
        logger.info(lance)
        db.insert_lance(lance)
        time.sleep(1)
    logger.info('Fim!')


if __name__ == "__main__":
    main()