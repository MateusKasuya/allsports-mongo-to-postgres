from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from sqlalchemy.exc import SQLAlchemyError

from src.utils.etl import ETLProcess


@pytest.fixture
def etl_instance():
    """Cria uma instância do ETLProcess para os testes."""
    return ETLProcess(uri='mongodb://fake_uri', write_mode='replace')


def test_parsing_json_competition_schedules(etl_instance):
    """Testa parsing_json para a coleção 'competition_schedules'."""
    etl_instance.read_nosql = MagicMock(return_value=[
        {'schedules': [{'sport_event': {'id': '123', 'status': 'ended'}}]}
    ])

    result = etl_instance.parsing_json('db', 'competition_schedules', 'schedules')
    assert result == [{'id': '123', 'status': 'ended'}]


def test_parsing_json_default_collection(etl_instance):
    """Testa parsing_json para uma coleção genérica sem tratamento específico."""
    etl_instance.read_nosql = MagicMock(return_value=[
        {'default_key': [{'id': 'abc', 'value': 42}]}
    ])
    result = etl_instance.parsing_json('db', 'other_collection', 'default_key')
    assert result == [{'id': 'abc', 'value': 42}]


def test_transform_to_df_simple(etl_instance):
    """Testa transformação simples em DataFrame."""
    data = [{'id': 1, 'name': 'Sport A'}, {'id': 2, 'name': 'Sport B'}]
    df = etl_instance.transform_to_df(data, 'default')
    assert list(df.columns) == ['id', 'name']
    assert df.shape == (2, 2)


def test_transform_to_df_with_nested_dict(etl_instance):
    """Testa normalização de dicionário aninhado."""
    data = [{'id': 1, 'info': {'a': 10}}, {'id': 2, 'info': {'a': 20}}]
    df = etl_instance.transform_to_df(data, 'default')
    assert 'info_a' in df.columns
    assert df.shape == (2, 2)


def test_transform_to_df_with_nested_list(etl_instance):
    """Testa normalização de lista aninhada."""
    data = [{'id': 1, 'list_col': [{'x': 'a'}, {'x': 'b'}]}]
    df = etl_instance.transform_to_df(data, 'default')
    assert 'list_col_x' in df.columns
    assert df.shape == (2, 2)


@patch('pandas.DataFrame.to_sql')
def test_load_to_destination_success(mock_to_sql, etl_instance):
    df = pd.DataFrame({'id': [1, 2]})
    engine = MagicMock()
    etl_instance.load_to_destination(engine, df, 'sports')
    mock_to_sql.assert_called_once()


@patch('pandas.DataFrame.to_sql')
def test_load_to_destination_empty_df(mock_to_sql, etl_instance):
    df = pd.DataFrame()
    engine = MagicMock()
    etl_instance.load_to_destination(engine, df, 'sports')
    mock_to_sql.assert_not_called()


@patch('pandas.DataFrame.to_sql', side_effect=SQLAlchemyError('Erro!'))
def test_load_to_destination_error(mock_to_sql, etl_instance):
    df = pd.DataFrame({'id': [1]})
    engine = MagicMock()
    with pytest.raises(RuntimeError, match="Erro ao inserir dados na tabela 'sports'"):
        etl_instance.load_to_destination(engine, df, 'sports')
