import requests
import threading
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
handler = logging.FileHandler('vonat_data.log')

logger.addHandler(handler)

class VonatDataGetter(threading.Thread):
    def __init__(self, database, period_s=300, url='http://vonatinfo.mav-start.hu/map.aspx/getData'):
        logger.info(f"Initializing VonatDataGetter : db:{database} period:{period_s} url:{url}")
        super().__init__()
        self.url = url
        self.period = period_s
        self.database = None  # placeholder for MySQL DB running on rpi
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
        date_time = result['@CreationTime']
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

    def _upload_to_database(self):
        pass

    def run(self):
        """ Function for Thread.start to run. Periodically get data about the trains, and uploads it in the database"""
        pass




r = requests.post('http://vonatinfo.mav-start.hu/map.aspx/getData', json={"a":"TRAINS","jo":{"history":False,"id":False}})
print(r.raw)
