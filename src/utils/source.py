from typing import List

from pymongo import MongoClient


class MongoDBProcess:
    """
    Classe responsável pela interação com o banco de dados MongoDB.
    Permite operações básicas como inserção, leitura e verificação de documentos.
    """

    def __init__(self, uri: str):
        """
        Inicializa a conexão com o MongoDB.

        :param uri: str - URI de conexão do MongoDB
        """
        self.client = MongoClient(uri)

    def read_nosql(
        self, database_name: str, collection_name: str, query: dict = None
    ) -> List[dict]:
        """
        Lê documentos de uma coleção no MongoDB.

        :param database_name: str - Nome do banco de dados.
        :param collection_name: str - Nome da coleção.
        :param query: dict - Critério de consulta (opcional, padrão é vazio).
        :return: List[dict] - Lista de documentos encontrados.
        """

        if query is None:
            query = {}

        try:
            collection = self.client[database_name][collection_name]
            return list(collection.find(query))
        except Exception as e:
            raise RuntimeError(f'Erro ao ler do MongoDB: {e}')

    def close_client(self) -> None:
        """Fecha a conexão com o MongoDB."""
        self.client.close()
