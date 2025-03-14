from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from sqlalchemy.exc import SQLAlchemyError

from src.utils.etl import ETLProcess


@pytest.fixture
def etl_instance():
    """Cria uma instância do ETLProcess para os testes."""
    return ETLProcess(uri='mongodb://fake_uri', write_mode='replace')


def test_parsing_json(etl_instance):
    """Testa se parsing_json retorna a lista esperada, incluindo `competition_schedules`."""
    etl_instance.read_nosql = MagicMock(
        return_value=[
            {
                'schedules': [
                    {
                        'sport_event': {
                            'id': 'sr:sport_event:52630061',
                            'start_time': '2025-02-25T00:00:00+00:00',
                            'status': 'ended',
                            'competitors': [
                                {
                                    'id': 'sr:competitor:3431',
                                    'name': 'Washington Wizards',
                                },
                                {
                                    'id': 'sr:competitor:3436',
                                    'name': 'Brooklyn Nets',
                                },
                            ],
                        }
                    }
                ]
            }
        ]
    )

    result = etl_instance.parsing_json(
        'fake_db', 'competition_schedules', 'schedules'
    )

    expected = [
        {
            'id': 'sr:sport_event:52630061',
            'start_time': '2025-02-25T00:00:00+00:00',
            'status': 'ended',
            'competitors': [
                {'id': 'sr:competitor:3431', 'name': 'Washington Wizards'},
                {'id': 'sr:competitor:3436', 'name': 'Brooklyn Nets'},
            ],
        }
    ]
    assert result == expected, 'Erro na extração de dados do MongoDB!'


def test_transform_to_df(etl_instance):
    """Testa se transform_to_df converte corretamente uma lista de dicionários em um DataFrame."""
    data = [{'id': 1, 'name': 'Futebol'}, {'id': 2, 'name': 'Basquete'}]
    df = etl_instance.transform_to_df(data)

    assert isinstance(df, pd.DataFrame), 'O retorno não é um DataFrame!'
    assert df.shape == (2, 2), 'O DataFrame não tem o formato esperado!'
    assert list(df.columns) == [
        'id',
        'name',
    ], 'Os nomes das colunas estão incorretos!'


def test_transform_to_df_with_nested_dict(etl_instance):
    """Testa se transform_to_df lida corretamente com colunas aninhadas."""
    data = [
        {'id': 1, 'details': {'country': 'Brasil', 'league': 'Série A'}},
        {'id': 2, 'details': {'country': 'EUA', 'league': 'NBA'}},
    ]

    df = etl_instance.transform_to_df(data)

    assert 'details_country' in df.columns, 'A normalização do JSON falhou!'
    assert 'details_league' in df.columns, 'A normalização do JSON falhou!'
    assert df.shape == (2, 3), 'O DataFrame não está no formato esperado!'


def test_transform_to_df_with_competitors(etl_instance):
    """Testa se transform_to_df lida corretamente com a coluna `competitors`."""
    data = [
        {
            'id': 'sr:sport_event:52630061',
            'start_time': '2025-02-25T00:00:00+00:00',
            'competitors': [
                {'id': 'sr:competitor:3431', 'name': 'Washington Wizards'},
                {'id': 'sr:competitor:3436', 'name': 'Brooklyn Nets'},
            ],
        }
    ]

    df = etl_instance.transform_to_df(data)

    assert (
        'competitors_id' in df.columns
    ), 'A normalização de `competitors` falhou!'
    assert (
        'competitors_name' in df.columns
    ), 'A normalização de `competitors` falhou!'
    assert df.shape == (
        2,
        4,
    ), 'A normalização da lista não ocorreu corretamente!'


@patch('pandas.DataFrame.to_sql')
def test_load_to_destination_success(mock_to_sql, etl_instance):
    """Testa se load_to_destination executa sem erros ao receber um DataFrame válido."""
    df = pd.DataFrame({'id': [1, 2], 'name': ['Futebol', 'Basquete']})
    mock_engine = MagicMock()

    etl_instance.load_to_destination(mock_engine, df, 'sports')

    # Verifica se to_sql foi chamado corretamente
    mock_to_sql.assert_called_once_with(
        name='sports',
        con=mock_engine,
        if_exists=etl_instance.write_mode,
        index=False,
    )


@patch('pandas.DataFrame.to_sql')
def test_load_to_destination_empty_df(mock_to_sql, etl_instance):
    """Testa se load_to_destination não executa inserção quando o DataFrame está vazio."""
    df = pd.DataFrame()
    mock_engine = MagicMock()

    etl_instance.load_to_destination(mock_engine, df, 'sports')

    # Como o DF está vazio, `to_sql()` **não deve ser chamado**
    mock_to_sql.assert_not_called()


@patch(
    'pandas.DataFrame.to_sql',
    side_effect=SQLAlchemyError('Erro de banco de dados!'),
)
def test_load_to_destination_sqlalchemy_error(mock_to_sql, etl_instance):
    """Testa se load_to_destination captura erros do SQLAlchemy."""
    df = pd.DataFrame({'id': [1, 2], 'name': ['Futebol', 'Basquete']})
    mock_engine = MagicMock()

    with pytest.raises(
        RuntimeError, match="Erro ao inserir dados na tabela 'sports'"
    ):
        etl_instance.load_to_destination(mock_engine, df, 'sports')
