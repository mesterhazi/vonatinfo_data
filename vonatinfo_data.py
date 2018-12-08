import requests
import threading
import logging
import pymysql.cursors
import time
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
handler = logging.FileHandler('vonat_data.log')

logger.addHandler(handler)

class VonatDataGetter(threading.Thread):
    def __init__(self, database=('127.0.0.1', 'trains'), period_s=300, url='http://vonatinfo.mav-start.hu/map.aspx/getData'):
        logger.info(f"Initializing VonatDataGetter : db:{database} period:{period_s} url:{url}")
        super().__init__()
        self.url = url
        self.period = period_s
        self.db_connection = pymysql.connect(
            host=database[0],
            user='vonat_data_getter',
            password='',
            db=database[1],
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        self.enabled = True
        logger.info("Initialization done...")

    def _get_data(self, id=False, history=False):
        # TODO error handling
        try:
            r = requests.post(self.url,
                              json={"a":"TRAINS","jo":{"history":history,"id":id}})
            r.raise_for_status()
        except requests.HTTPError:
            logger.error(f"HTTP response error! - {r.status_code} - url:{self.url} history{history} id:{id}")
        except:
            logger.error(f"Unknown error occured when makig a POST request url:{self.url} history{history} id:{id}")
            return None

        return r.text

    def _unpack_data(self, json_str):
        # TODO error handling
        action = json_str['d']['action']
        param = json_str['d']['param']
        result = json_str['d']['result']
        creation_time = result['@CreationTime']
        package_type = result['@PackageType']  # GpsData expected

        train_data = result['Trains']['Train']  # This is a list now
        """ every list element is a dict and has the following pattern:
             '@Delay': int [minutes],
             '@ElviraID': str,
             '@Lat': float, - GPS coordinate
             '@Line': str, - train line ID
             '@Lon': float, - GPS coordinate
             '@Menetvonal': str, - company MAV or GYSEV
             '@Relation': str, - "starting_station - final_station"
             '@TrainNumber': str, - Train ID 55[6 digit train number according to elvira.hu]
        """
        logger.info(f"Unpacking data time: {creation_time}...")
        ret_data = {
            'creation_time' : creation_time,
            'train_data' : train_data,
            'day' : time.strftime('%Y-%m-%d')
        }

        return ret_data

    def _upload_to_database(self, data_dict):
        """ Uploads the given data to a mysql server that is given in the __init__ method of the class"""
        self.db_connection.ping(reconnect=True)  # reconnects if needed
        insert = """INSERT INTO 'trains' ('creation_date', 'day', 'relation', 'train_number', 'line', 
        'delay', 'elvira_id', 'coord_lat', 'coord_lon', 'company') 
        VALUES (%s, %s, %s, %s, %s, %d, %s, %f, %f, %s)"""
        logger.info('Connected to database.')
        try:
            with self.db_connection.cursor() as cursor:
                for data in data_dict['train_data']:
                    # create record
                    rec = (data_dict['creation_date'],
                     data_dict['day'],
                     data['relation'],
                     data['train_number'],
                     data['line'],
                     data['delay'],
                     data['elvira_id'],
                     data['coord_lat'],
                     data['coord_lon'],
                     data['company'])
                    # insert record
                    logger.debug(f'Inserting record: {data}')
                    cursor.execute(insert, rec)
            self.db_connection.commit()
            logger.info('Committing data...')

        except Exception as e:
            logger.error(f'DATABASE ERROR: {e}')
        finally:
            self.db_connection.close()
            logger.info('Database connection closed.')

    def debug_run(self):
        """ Function very similar to 'run', but only executing one get_data-unpack-upload cycle for testing purposes """
        logger.info('debug_run called...')
        raw_data = self._get_data()
        unpacked_data = self._unpack_data(raw_data)
        self._upload_to_database(unpacked_data)
        logger.info('debug_run finished.')
        pass

    def run(self):
        """ Function for Thread.start to run. Periodically get data about the trains, and uploads it in the database"""
        while self.enabled:
            logger.info('Run called...')
            raw_data = self._get_data()
            unpacked_data = self._unpack_data(raw_data)
            self._upload_to_database(unpacked_data)
            logger.info('Run finished.')

    def stop(self):
        self.enabled = False

