import logging
from pyhive import hive

from infra.dbtjoom.types import SparkThriftProfile


class ThriftClient:
    logger = logging.getLogger("ThriftClient")

    def __init__(self, host, port):
        self.host = host
        self.port = int(port)
        self._connection = None

    @classmethod
    def from_profile(cls, profile: SparkThriftProfile):
        return cls(profile.host, profile.port)

    @property
    def connection(self):
        if self._connection is None:
            self._connection = hive.Connection(host=self.host, port=self.port)
        return self._connection

    def close_connection(self):
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def get_table_location(self, table_name: str):
        try:
            for row in self.sql(f"describe formatted {table_name}"):
                if 'Location' in row[0]:
                    return row[1].strip()
        except Exception as e:
            if 'TABLE_OR_VIEW_NOT_FOUND' in str(e):
                self.logger.info(f'Table `{table_name}` not found')
                return None
            raise

        return None

    def get_db_location(self, db_name: str):
        try:
            for row in self.sql(f"describe database {db_name}"):
                if 'Location' in row[0]:
                    return row[1].strip()
        except Exception as e:
            if 'SCHEMA_NOT_FOUND' in str(e):
                self.logger.info(f'Database `{db_name}` not found')
                return None
            raise

        return None

    def drop_if_exists(self, table_name: str, dryrun: bool = False):
        query = f"DROP TABLE IF EXISTS `{table_name}`"
        if dryrun:
            self.logger.info(f"Execute `{query}`")
            return []
        else:
            return self.sql(query)

    def sql(self, query):
        self.logger.info(f"Execute `{query}`")
        cursor = self.connection.cursor()
        cursor.execute(query)
        for row in cursor.fetchall():
            yield row