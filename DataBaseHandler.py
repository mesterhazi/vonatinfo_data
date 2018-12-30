import pymysql
import logging
import collections

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
handler = logging.FileHandler('vonat_data.log')

logger.addHandler(handler)

class DataBaseHandler():
    def __init__(self,  database=('127.0.0.1', 'train_data'),
                 user=('vonat_data_getter', 'user'),
                 character_set='utf8mb4', cursorclass=pymysql.cursors.DictCursor):
        self.str = "Database: {} at {} user: {}".format(database[1], database[0], user[1])
        self._connection = pymysql.connect(
            host=database[0],
            user=user[0],
            password=user[1],
            db=database[1],
            charset=character_set,
            cursorclass=cursorclass)
        self.is_connected = True

    def __del__(self):
        logger.info('DatabaseHandler destructor called')
        self.close()

    def close(self):
        if self.is_connected:
            self._connection.close()
            self.is_connected = False
    def __str__(self):
        return self.str

    def ping(self, reconnect=True):
        self._connection.ping(reconnect)
        self.is_connected = True

    def execute(self, sql, data):
        """ Executes sql command with added data parameters.
        example: execute('INSERT INTO table_name (date) VALUES (%s)', '2018.05.05')  """
        self.ping()
        try:
            with self._connection.cursor() as cursor:
                logger.debug("{} - Executing {} data: {}".format(self, sql, data))
                cursor.execute(sql, data)
        except Exception as e:
            #TODO elaborate DB related exception handling
            logger.error('DATABASE ERROR: {type(e)}:{e}'.format(type(e),e))

    def execute_commit(self, sql, data):
        """ Executes sql command with added data parameters and commits it to the DB """
        self.execute(sql, data)
        self.commit()

    def upload_records(self, sql, records):
        """ Execute the same sql query with different data for the records.
         Use it to upload multiple records held in an iterable object into a table"""
        if not isinstance(records, collections.Iterable):
            self.execute(sql, records)
            return 1

        self.ping()
        try:
            with self._connection.cursor() as cursor:
                for data in records:
                    logger.debug("{} - Executing {} data: {}".format(self, sql, data))
                    cursor.execute(sql, data)
        except Exception as e:
            #TODO elaborate DB related exception handling
            logger.error('DATABASE ERROR: {type(e)}:{e}'.format(type(e),e))
        return len(records)

    def upload_records_commit(self, sql, records):
        num = self.upload_records(sql, records)
        self.commit()
        logger.info('{} records committed to database {}'.format(num, self))

    def commit(self):
        """ Commits the changes made to the DB """
        self._connection.commit()




