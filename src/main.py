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

        dict_collection_key = {
            # 'sports': 'sports',
            # 'sports_competition': 'competitions',
            # 'competition_schedules': 'schedules',
            #'sport_event_player_props' : 'sport_event_players_props',
           # 'sport_event_markets' : 'markets',
           'outcomes' : 'markets'
        }

        for collection_name, key_collection in dict_collection_key.items():
            logging.info(
                f'Iniciando Collection: {collection_name}, Key: {key_collection}'
            )

            logging.info('Coletando dados do Mongo e tratando-os')
            json_to_list = pipeline.parsing_json(
                database_name='odds',
                collection_name=collection_name,
                key_collection=key_collection,
            )

            if not json_to_list:
                logging.warning(
                    'Nenhum dado foi extraído do MongoDB. Pipeline encerrada.'
                )
                continue

            logging.info('Transformando dados em DF')
            df = pipeline.transform_to_df(json_to_list, collection_name)

            logging.info('Carregando dados no Postgres')

            if collection_name == 'sport_event_markets':

                pipeline.load_to_destination(
                    engine=destination_engine, df=df, table=f'sport_event_{key_collection}'
                )
            
            elif collection_name == 'outcomes': 
                pipeline.load_to_destination(
                    engine=destination_engine, df=df, table=f'sport_event_{key_collection}_outcomes'
                )

            else:
                pipeline.load_to_destination(
                    engine=destination_engine, df=df, table=key_collection
                )

            logging.info(
                f'Collection: {collection_name}, Key: {key_collection} finalizada com sucesso'
            )

        pipeline.close_client()

        pipeline.close_engine(engine=destination_engine)

        logging.info('Fim da Pipeline')

    except Exception as e:
        logging.error(f'Erro inesperado na pipeline: {e}', exc_info=True)


if __name__ == '__main__':
    main()
