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
    def __init__(self, database=('127.0.0.1', 'train_data'), period_s=300, url='http://vonatinfo.mav-start.hu/map.aspx/getData'):
        logger.info(f"Initializing VonatDataGetter : db:{database} period:{period_s} url:{url}")
        super().__init__()
        self.url = url
        self.period = period_s
        self.db_connection = pymysql.connect(
            host=database[0],
            user='vonat_data_getter',
            password='user',
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
        json_obj = json.loads(json_str)
        print(json_obj)
        action = json_obj['d']['action']
        param = json_obj['d']['param']
        result = json_obj['d']['result']
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
        insert = """INSERT INTO trains (creation_date, day, relation, train_number, line, 
                delay, elvira_id, coord_lat, coord_lon, company) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
        logger.info('Connected to database.')
        with self.db_connection.cursor() as cursor:
            for data in data_dict['train_data']:
                try:
                    # create record
                    rec = (data_dict['creation_time'],
                     data_dict['day'],
                     data.get('@Relation'),  # @Relation can be missing
                     data['@TrainNumber'],
                     data['@Line'],
                     data['@Delay'],
                     data['@ElviraID'],
                     data['@Lat'],
                     data['@Lon'],
                     data['@Menetvonal'])
                    # insert record
                    logger.debug(f'Inserting record: {data}')
                    print(insert)
                    print(rec)
                    cursor.execute(insert, rec)
                except KeyError as e:
                    logger.info(f'Key {e} missing')
                except Exception as e:
                    logger.error(f'DATABASE ERROR: {type(e)}:{e}')
        try:
            self.db_connection.commit()
            logger.info('Committing data...')
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

if __name__ == '__main__':
    vonatinfo_data_getter = VonatDataGetter()
    vonatinfo_data_getter.debug_run()
    # vonatinfo_data_getter.start()
    #
    # try:
    #     while True:
    #         time.sleep(1)
    # except KeyboardInterrupt:
    #     vonatinfo_data_getter.stop()
    # finally:
    #     pass