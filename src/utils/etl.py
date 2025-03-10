import os
import sys

import pandas as pd
from sqlalchemy import Engine
from sqlalchemy.exc import SQLAlchemyError

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
)

from datetime import datetime, timedelta

from src.utils.destination import DbEngine
from src.utils.source import MongoDBProcess


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
        self, database_name: str, collection_name: str, query: dict = None
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

        list_document = self.read_nosql(database_name, collection_name, query)
        list_to_append = []

        for document in list_document:
            # Evita KeyError caso a chave 'sports' não exista
            data = document.get('sports', [])
            list_to_append.extend(
                data
            )  # Garante que apenas listas serão adicionadas

        return list_to_append

    def transform_to_df(self, list_to_transform: list) -> pd.DataFrame:
        """
        Transforma uma lista de dicionários em um DataFrame, normalizando colunas que contêm dicionários aninhados.

        :param list_to_transform: list - Lista de dicionários extraídos do MongoDB.
        :return: pd.DataFrame - DataFrame tratado e pronto para carga.
        """
        if not list_to_transform:
            return (
                pd.DataFrame()
            )  # Retorna DataFrame vazio caso a lista esteja vazia

        df = pd.DataFrame(list_to_transform)

        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, dict)).any():
                # Normaliza os dicionários transformando-os em colunas separadas
                df_exploded = pd.json_normalize(df[col])
                df_exploded.columns = [
                    f'{col}_{subcol}' for subcol in df_exploded.columns
                ]
                df = df.drop(columns=[col]).join(df_exploded)

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
