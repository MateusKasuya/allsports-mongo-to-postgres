from unittest.mock import MagicMock, patch

import pytest

from src.utils.source import MongoDBProcess


@pytest.fixture
def mock_mongo():
    """Cria uma instância mock da classe MongoDBProcess"""
    with patch('src.utils.source.MongoClient') as mock_client:
        mock_instance = mock_client.return_value
        return MongoDBProcess('mongodb://fake-uri')


def test_read_success(mock_mongo):
    """Testa se a leitura retorna os documentos corretamente"""
    mock_mongo.client['test_db']['test_collection'].find.return_value = [
        {'name': 'Alice'},
        {'name': 'Bob'},
    ]
    result = mock_mongo.read_nosql('test_db', 'test_collection')
    assert result == [{'name': 'Alice'}, {'name': 'Bob'}]


def test_read_failure(mock_mongo):
    """Testa erro ao tentar fazer uma consulta"""
    mock_mongo.client['test_db'][
        'test_collection'
    ].find.side_effect = Exception('Query failed')
    with pytest.raises(
        RuntimeError, match='Erro ao ler do MongoDB: Query failed'
    ):
        mock_mongo.read_nosql('test_db', 'test_collection')


def test_close_client(mock_mongo):
    """Testa se o fechamento da conexão chama .close() corretamente"""
    mock_mongo.client.close = MagicMock()
    mock_mongo.close_client()
    mock_mongo.client.close.assert_called_once()
