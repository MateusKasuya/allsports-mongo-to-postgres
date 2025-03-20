import os
import sys

import pandas as pd
from sqlalchemy import Engine
from sqlalchemy.exc import SQLAlchemyError

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
)

from src.utils.destination import DbEngine
from src.utils.source import MongoDBProcess

from typing import List


class ETLProcess(MongoDBProcess, DbEngine):
    """
    Classe responsável por extrair dados de um banco MongoDB e carregá-los em um banco PostgreSQL.
    """

    def __init__(self, uri: str, write_mode: str = 'replace'):
        """
        Inicializa a conexão com o MongoDB e configura a estratégia de escrita no PostgreSQL.

        :param uri: str - URI de conexão do MongoDB.
        :param write_mode: str - Modo de escrita no PostgreSQL ("append" ou "replace").
        """
        super().__init__(uri)
        self.write_mode = write_mode
        self.engine = None  # Engine do PostgreSQL será inicializada depois

    def parsing_json(
        self,
        database_name: str,
        collection_name: str,
        key_collection: str,
        query: dict = None,
    ) -> list:
        """
        Extrai documentos do MongoDB e transforma em uma lista de dicionários.

        :param database_name: str - Nome do banco de dados.
        :param collection_name: str - Nome da coleção.
        :param query: dict - Critério de consulta (padrão: consulta vazia).
        :return: list - Lista de documentos processados.
        """
        if query is None:
            query = {}

        if collection_name == 'outcomes':
            list_document = self.read_nosql(database_name, 'sport_event_markets', query)
        else:
            list_document = self.read_nosql(database_name, collection_name, query)
        list_to_append = []

        for document in list_document:

            if collection_name == 'competition_schedules':
                schedules = document.get(key_collection, [])
                for sport_event in schedules:
                    dict_sport_event = sport_event.get('sport_event', {})
                    list_to_append.append(dict_sport_event)

            elif collection_name == 'sport_event_player_props':
                sport_event_players_props = document.get(key_collection, {})
                sport_event = sport_event_players_props.get('sport_event', {})
                event_id = sport_event.get('id')

                player_props = sport_event_players_props.get('players_props', [])

                for player in player_props:
                    player['sport_event_id'] = event_id

                list_to_append.extend(player_props)

            elif collection_name == 'sport_event_markets':
                sport_event = document.get("sport_event", {})
                event_id = sport_event.get('id')

                markets = document.get(key_collection, [])

                for market in markets:
                    market['sport_event_id'] = event_id

                list_to_append.extend(markets)

            elif collection_name == 'outcomes':
                sport_event = document.get("sport_event", {})
                event_id = sport_event.get('id')

                markets = document.get(key_collection, [])

                for market in markets:
                    market_id = market.get('id')

                    books = market.get('books', [])
                    for book in books:
                        book_id = book.get('id')

                        outcomes = book.get('outcomes', [])

                        for outcome in outcomes:
                            outcome['sport_event_id'] = event_id
                            outcome['market_id'] = market_id
                            outcome['books_id'] = book_id


                        list_to_append.extend(outcomes)


            else:
                # Evita KeyError caso a chave não exista
                data = document.get(key_collection, [])
                list_to_append.extend(data)

        return list_to_append

    def transform_to_df(self, list_to_transform: List[dict], collection: str) -> pd.DataFrame:
        """
        Transforma uma lista de dicionários em um DataFrame, normalizando colunas que contêm dicionários aninhados.

        :param list_to_transform: list - Lista de dicionários extraídos do MongoDB.
        :return: pd.DataFrame - DataFrame tratado e pronto para carga.
        """
        if not list_to_transform:
            return (pd.DataFrame())

        df = pd.DataFrame(list_to_transform)

        # Normaliza dicionários aninhados, exceto "competitors"
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, dict)).any():
                df_normalized = pd.json_normalize(df[col])
                df_normalized.columns = [
                    f'{col}_{subcol}' for subcol in df_normalized.columns
                ]
                df = df.drop(columns=[col]).join(df_normalized)
            

            elif df[col].apply(lambda x: isinstance(x, list)).any():
                df = df.explode(col).reset_index(drop=True)
                df_normalized = pd.json_normalize(df[col])
                df_normalized.columns = [
                    f'{col}_{subcol}' for subcol in df_normalized.columns
                ]

                if collection == 'sport_event_markets':
                    df_normalized = df_normalized.drop(columns = ['books_outcomes']).reset_index(drop = True)
   

                for col_normalized in df_normalized.columns:
                    if df_normalized[col_normalized].apply(lambda x: isinstance(x, list)).any():
                        df_normalized = df_normalized.explode(col_normalized).reset_index(drop = True)


                        df_sub_normalized = pd.json_normalize(df_normalized[col_normalized])
                        df_sub_normalized.columns = [
                            f'{col_normalized}_{subcol}' for subcol in df_sub_normalized.columns
                        ]

                        for sub_col_normalized in df_sub_normalized.columns:
                            if df_sub_normalized[sub_col_normalized].apply(lambda x: isinstance(x, list)).any():
                                df_sub_normalized = df_sub_normalized.explode(sub_col_normalized).reset_index(drop = True)
                                df_max_normalized = pd.json_normalize(df_sub_normalized[sub_col_normalized])
                                df_max_normalized.columns = [
                                    f'{sub_col_normalized}_{subcol}' for subcol in df_max_normalized.columns
                                ]

                                df_sub_normalized = df_sub_normalized.drop(columns = [sub_col_normalized]).reset_index(drop = True)
                                df_sub_normalized = pd.concat([df_sub_normalized, df_max_normalized], axis = 1)

                        df_normalized = df_normalized.drop(columns = [col_normalized]).reset_index(drop = True)
                        df_normalized = pd.concat([df_normalized, df_sub_normalized], axis = 1)
                        


                df = df.drop(columns=[col]).reset_index(drop=True)

                df = pd.concat([df, df_normalized], axis=1)
                
            

        return df

    def load_to_destination(
        self, engine: Engine, df: pd.DataFrame, table: str
    ):
        """
        Carrega um DataFrame para um banco de dados PostgreSQL.

        :param engine: sqlalchemy.engine.Engine - Conexão com o banco de dados.
        :param df: pd.DataFrame - DataFrame a ser carregado.
        :param table: str - Nome da tabela de destino.
        :return: None
        """
        if df.empty:
            print(
                f"[AVISO] DataFrame vazio. Nenhum dado foi carregado para a tabela '{table}'."
            )
            return  # Evita tentativa de inserção com DataFrame vazio

        try:
            df.to_sql(
                name=table,
                con=engine,
                if_exists=self.write_mode,
                index=False,
            )
        except SQLAlchemyError as e:
            raise RuntimeError(
                f"Erro ao inserir dados na tabela '{table}': {e}"
            )
