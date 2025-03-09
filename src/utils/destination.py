from sqlalchemy import Engine, create_engine


class DbEngine:
    """
    Classe responsável por criar conexões com bancos de dados Firebird e PostgreSQL utilizando SQLAlchemy.
    """

    def _create_engine(self, url: str) -> Engine:
        """
        Cria e retorna uma engine SQLAlchemy a partir da URL fornecida.

        Parâmetros:
        ----------
        url : str
            URL de conexão do banco de dados.

        Retorno:
        -------
        Engine
            Objeto SQLAlchemy `Engine` para interagir com o banco de dados.
        """
        try:
            return create_engine(url)
        except Exception as e:
            raise ConnectionError(f'Erro ao conectar ao banco: {e}')

    def postgres_engine(
        self, user: str, password: str, host: str, port: str, database: str
    ) -> Engine:
        """
        Cria e retorna uma conexão com um banco de dados PostgreSQL usando SQLAlchemy.

        Parâmetros:
        ----------
        user : str
            Nome de usuário do banco de dados.
        password : str
            Senha do banco de dados.
        host : str
            Endereço do servidor do banco de dados.
        port : str
            Porta do banco de dados.
        database : str
            Nome do banco de dados.

        Retorno:
        -------
        Engine
            Objeto SQLAlchemy `Engine` para interagir com o banco PostgreSQL.
        """
        postgres_url = (
            f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'
        )
        return self._create_engine(postgres_url)

    def close_engine(self, engine: Engine):
        """
        Fecha a conexão da engine SQLAlchemy.

        Parâmetros:
        ----------
        engine : Engine
            Objeto SQLAlchemy `Engine` a ser fechado.
        """
        return engine.dispose()