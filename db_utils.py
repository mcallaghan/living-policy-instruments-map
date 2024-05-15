from nacsos_data.db.engine import DatabaseEngineAsync
from pydantic import BaseModel
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy import create_engine, text, URL

from nacsos_data.db.connection import _get_settings
from nacsos_data.db.engine import DictLikeEncoder, DatabaseEngineAsync


class DatabaseEngineAsyncPreping(DatabaseEngineAsync):

    def __init__(self, host: str, port: int, user: str, password: str,
                 database: str = 'nacsos_core', debug: bool = False):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database

        # TODO expire_on_commit (check if this should be turned off)
        self._connection_str = URL.create(
            drivername='postgresql+psycopg',
            username=self._user,
            password=self._password,
            host=self._host,
            port=self._port,
            database=self._database,
        )
        self.engine = create_async_engine(self._connection_str, echo=debug, future=True,
                                          json_serializer=DictLikeEncoder().encode, pool_pre_ping=True)
        self._session: async_sessionmaker[AsyncSession] = async_sessionmaker(  # type: ignore[type-arg] # FIXME
            bind=self.engine, autoflush=False, autocommit=False)

def get_engine_async_preping(conf_file: str | None = None,
                     debug: bool = False) -> DatabaseEngineAsync:
    settings = _get_settings(conf_file)
    return DatabaseEngineAsyncPreping(host=settings.HOST, port=settings.PORT, user=settings.USER, password=settings.PASSWORD,
                               database=settings.DATABASE, debug=debug)



