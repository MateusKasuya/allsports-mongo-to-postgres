from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import SQLAlchemyError

from src.utils.destination import (  # Ajuste conforme sua estrutura de diretórios
    DbEngine,
)


@pytest.fixture
def db_engine():
    return DbEngine()

def test_postgres_engine(db_engine):
    with patch('src.utils.destination.create_engine') as mock_create_engine:
        mock_create_engine.return_value = MagicMock()
        engine = db_engine.postgres_engine(
            'user', 'pass', 'localhost', '5432', 'test_db'
        )
        assert engine is not None
        mock_create_engine.assert_called_once()


def test_create_engine_failure(db_engine):
    with patch(
        'src.utils.destination.create_engine',
        side_effect=SQLAlchemyError('Erro de conexão'),
    ):
        with pytest.raises(
            ConnectionError, match='Erro ao conectar ao banco: Erro de conexão'
        ):
            db_engine._create_engine('invalid_url')


def test_close_engine(db_engine):
    engine_mock = MagicMock()
    db_engine.close_engine(engine_mock)
    engine_mock.dispose.assert_called_once()