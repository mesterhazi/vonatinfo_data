import requests
import threading
import logging
from workers import upload_all_worker, upload_delays_worker
import time
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
handler = logging.FileHandler('vonat_data.log')

logger.addHandler(handler)

class VonatDataGetter(threading.Thread):
    def __init__(self, period_s=300, url='http://vonatinfo.mav-start.hu/map.aspx/getData',
    workers = [upload_all_worker, upload_delays_worker]):
        logger.info("Initializing VonatDataGetter : period:{} url:{}".format(period_s, url))
        super().__init__()
        self.url = url
        self.period = period_s
        self.workers = workers
        self.active_trains = {}
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
        try:
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
        except Exception as e:
            logger.error("Error occured in _unpack_data tried to get data, tried to get data from:")
            logger.error(json_obj)
            logger.error(str(e))
            return None

        logger.info("Unpacking data time: {}...".format(creation_time))
        ret_data = {
            'creation_time' : creation_time.split(' ')[1],  # only get H:M:S
            'train_data' : train_data,
            'day' : time.strftime('%Y-%m-%d')
        }

        return ret_data

    def debug_run(self):
        """ Function very similar to 'run', but only executing one get_data-unpack-upload cycle for testing purposes """
        logger.info('debug_run called...')
        raw_data = self._get_data()
        unpacked_data = self._unpack_data(raw_data)
        for w in self.workers:
            t = threading.Thread(target=w, args=(unpacked_data,), kwargs={'active_trains' : self.active_trains})
            t.start()
            t.join()
        logger.info('debug_run finished.')
        return unpacked_data

    def run(self):
        """ Function for Thread.start to run. Periodically get data about the trains, and uploads it in the database"""
        logger.info('#VonatDataGetter STARTEED!')
        while self.enabled:
            logger.info('Getting data...')
            s = time.time()
            raw_data = self._get_data()
            unpacked_data = self._unpack_data(raw_data)

            threads = []
            for w in self.workers:
                threads.append(threading.Thread(target=w, args=(unpacked_data,), kwargs={'active_trains' : self.active_trains}))
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            e = time.time()
            logger.info('All workers finished in {}s'.format(e-s))
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
