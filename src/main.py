import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import logging

from dotenv import load_dotenv

from src.utils.etl import ETLProcess

# Configuração do logging
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s'
)


def main():

    load_dotenv()

    logging.info('Iniciando Pipeline')
    try:
        pipeline = ETLProcess(uri=os.getenv('MONGOURI'), write_mode='replace')

        logging.info('Criando Engine Postgres')
        destination_engine = pipeline.postgres_engine(
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            host=os.getenv('POSTGRES_HOST'),
            port=os.getenv('POSTGRES_PORT'),
            database=os.getenv('POSTGRES_DB'),
        )

        logging.info('Coletando dados do Mongo e tratando-os')
        json_to_list = pipeline.parsing_json(
            database_name='odds', collection_name='sports'
        )

        pipeline.close_client()

        if not json_to_list:
            logging.warning(
                'Nenhum dado foi extraído do MongoDB. Pipeline encerrada.'
            )
            return

        logging.info('Transformando dados em DF')
        df = pipeline.transform_to_df(json_to_list)

        logging.info('Carregando dados no Postgres')
        pipeline.load_to_destination(
            engine=destination_engine, df=df, table='sports'
        )

        pipeline.close_engine(engine=destination_engine)

        logging.info('Fim da Pipeline')

    except Exception as e:
        logging.error(f'Erro inesperado na pipeline: {e}', exc_info=True)


if __name__ == '__main__':
    main()
