import requests
import threading
import json


class VonatDataGetter(threading.Thread):
    def __init__(self, database, period_s=300, url='http://vonatinfo.mav-start.hu/map.aspx/getData'):
        super().__init__()
        self.url = url
        self.period = period_s
        self.database = None  # placeholder for MySQL DB running on rpi

    def _get_data(self, id=False, history=False):
        r = requests.post('http://vonatinfo.mav-start.hu/map.aspx/getData',
                          json={"a":"TRAINS","jo":{"history":history,"id":id}})
        return r.text

    def _unpack_data(self, json_str):
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
