import requests
import threading
import logging
import pymysql.cursors
from DataBaseHandler import DataBaseHandler
import time
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
handler = logging.FileHandler('vonat_data.log')

logger.addHandler(handler)

class VonatDataGetter(threading.Thread):
    def __init__(self, database=('127.0.0.1', 'train_data'), user=('vonat_data_getter','user'), period_s=300, url='http://vonatinfo.mav-start.hu/map.aspx/getData'):
        logger.info("Initializing VonatDataGetter : db:{} period:{} url:{}".format(database, period_s, url))
        super().__init__()
        self.url = url
        self.period = period_s
        self.db_handler = DataBaseHandler(database=database, user=user)
        self.enabled = True
        logger.info("Initialization done...")

    def _get_data(self, id=False, history=False):
        # TODO error handling
        try:
            r = requests.post(self.url,
                              json={"a":"TRAINS","jo":{"history":history,"id":id}})
            r.raise_for_status()
        except requests.HTTPError:
            logger.error("HTTP response error! - {} - url:{} history{} id:{}".format(r.status_code, self.url, history, id))
        except Exception as e:
            logger.error("Error occured when makig a POST request url:{} history{} id:{}".format(self.url, history, id))
            logger.error(str(e))            
            return None

        return r.text

    def _unpack_data(self, json_str):
        # TODO error handling
        if json_str is not None:
            json_obj = json.loads(json_str)
        else:
            logger.error("None received as json string, cannot unpack data!")
            return None
        
        action = json_obj['d']['action']
        param = json_obj['d']['param']
        result = json_obj['d']['result']
        creation_time = result.get('@CreationTime')
        package_type = result.get('@PackageType')  # GpsData expected

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
        logger.info("Unpacking data time: {}...".format(creation_time))
        ret_data = {
            'creation_time' : creation_time.split(' ')[1],  # only get H:M:S
            'train_data' : train_data,
            'day' : time.strftime('%Y-%m-%d')
        }

        return ret_data

    def _upload_to_database(self, data_dict):
        """ Uploads the given data to a mysql server that is given in the __init__ method of the class"""
        if data_dict is None:
            logger.error("None received as data_dict. There is nothing to upload to the dB")
            return None
               
        self.db_handler.ping(reconnect=True)  # reconnects if needed
        insert = """INSERT INTO trains (creation_time, day, relation, train_number, line, 
                delay, elvira_id, coord_lat, coord_lon, company) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
        logger.info('Connected to database.')
        record_list = []
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
                logger.debug('Inserting record to list: {}'.format(data))
                record_list.append(rec)
            except KeyError as e:
                logger.info('Key {} missing'.format(e))
        try:
            self.db_handler.upload_records_commit(insert, record_list)
        finally:
            self.db_handler.close()
            logger.info('Database connection closed.')


    def debug_run(self):
        """ Function very similar to 'run', but only executing one get_data-unpack-upload cycle for testing purposes """
        logger.info('debug_run called...')
        raw_data = self._get_data()
        unpacked_data = self._unpack_data(raw_data)
        self._upload_to_database(unpacked_data)
        logger.info('debug_run finished.')

    def run(self):
        """ Function for Thread.start to run. Periodically get data about the trains, and uploads it in the database"""
        while self.enabled:
            logger.info('Getting and uploading data...')
            s = time.time()
            raw_data = self._get_data()
            unpacked_data = self._unpack_data(raw_data)
            self._upload_to_database(unpacked_data)
            e = time.time()
            logger.info('Finished in {}s'.format(e-s))
            time.sleep(self.period)

    def stop(self):
        self.enabled = False

if __name__ == '__main__':
    vonatinfo_data_getter = VonatDataGetter()
    vonatinfo_data_getter.debug_run()
    # vonatinfo_data_getter.start()
    # try:
    #     while True:
    #         time.sleep(1)
    # except KeyboardInterrupt:
    #     vonatinfo_data_getter.stop()
    # finally:
    #     pass
