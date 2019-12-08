import db
import logging
logging.basicConfig(format='%(asctime)s [CONFIG] - %(levelname)s: %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)

logger = logging.getLogger(__name__)

logger.info('Criando tabelas...')
db.create_tables()
logger.info('Pré-populando...')
db.seed()