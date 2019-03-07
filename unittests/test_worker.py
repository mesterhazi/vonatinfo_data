import unittest
import workers

# Test upload_delays_worker
class Test_upload_delays_worker(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def compare_record(self, record, reference, full_data):
        self.assertEqual(record[0], full_data['creation_time'], 'Creation_time mismatch')
        self.assertEqual(record[1], full_data['day'], 'Creation day mismatch')
        self.assertEqual(record[2], reference.get('@Relation'), '@Relation mismatch')
        self.assertEqual(record[3], reference.get('@TrainNumber'), '@TrainNumber mismatch')
        self.assertEqual(record[4], reference.get('@Delay'), '@Delay mismatch')
        self.assertEqual(record[5], reference.get('@Lat'), '@Lat mismatch')
        self.assertEqual(record[6], reference.get('@Lon'), '@Lon mismatch')

    def test_some_trains_arriving(self):
        active_trains = {}
        data_dict_1 = {
        'creation_time' : '11:11:11',
        'day' : '2019-02-03',
        'train_data' :
        [{'@Relation' : 'Pecs',
         '@TrainNumber' : '1',
         '@Delay' : 10,
         '@Lat' : '0.0',
         '@Lon' : '0.0'},
         {'@Relation' : 'Vp',
          '@TrainNumber' : '2',
          '@Delay' : 10,
          '@Lat' : '0.0',
          '@Lon' : '0.0'},
          {'@Relation' : 'Vac',
           '@TrainNumber' : '3',
           '@Delay' : 10,
           '@Lat' : '0.0',
           '@Lon' : '0.0'}
           ]
        }
        data_dict_2 = {
        'creation_time' : '22:22:22',
        'day' : '2019-02-03',
        'train_data' :
        [{'@Relation' : 'Pecs',
         '@TrainNumber' : '1',
         '@Delay' : 10,
         '@Lat' : '0.0',
         '@Lon' : '0.0'},
         {'@Relation' : 'Vp',
          '@TrainNumber' : '2',
          '@Delay' : 10,
          '@Lat' : '0.0',
          '@Lon' : '0.0'},
           ]}

        data_dict_3 = {
        'creation_time' : '23:33:33',
        'day' : '2019-02-03',
        'train_data' :
        [{'@Relation' : 'Pecs',
         '@TrainNumber' : '1',
         '@Delay' : 10,
         '@Lat' : '0.0',
         '@Lon' : '0.0'}
           ]
        }

        workers.upload_delays_worker(data_dict_1, active_trains=active_trains)
        workers.upload_delays_worker(data_dict_2, active_trains=active_trains)
        workers.upload_delays_worker(data_dict_3, active_trains=active_trains)
        workers.upload_delays_worker(data_dict_3, active_trains=active_trains)
        workers.upload_delays_worker(data_dict_3, active_trains=active_trains)
        print(active_trains)
        # '1' is missing the 5th time so it counts as arrived
        arrived = workers.upload_delays_worker(data_dict_3, active_trains=active_trains)
        print(active_trains)
        print(f"Arrived: {arrived}")
        self.compare_record(active_trains['1'][0], data_dict_3['train_data'][0], data_dict_3)
        self.assertEqual(active_trains['1'][1], 0, 'Missing counter mismatch')
        self.compare_record(active_trains['2'][0], data_dict_2['train_data'][1], data_dict_2)
        self.assertEqual(active_trains['2'][1], 4, 'Missing counter mismatch')
        self.assertEqual(len(active_trains), 2, 'Too many trains in the active_trains, one should have been deleted already')
        self.compare_record(arrived[0], data_dict_1['train_data'][2], data_dict_1)


    def test_empty_data_dict(self):
        active_trains = {}
        data_dict_empty = {'creation_time' : '17:02:20',
        'day' : '2019-02-03',
        'train_data' : []} # data_dict with no trains
        data_dict = {
        'creation_time' : '17:02:20',
        'day' : '2019-02-03',
        'train_data' :
        [{'@Relation' : 'Pecs',
         '@TrainNumber' : '1',
         '@Delay' : 10,
         '@Lat' : '0.0',
         '@Lon' : '0.0'},
         {'@Relation' : 'Vp',
          '@TrainNumber' : '2',
          '@Delay' : 10,
          '@Lat' : '0.0',
          '@Lon' : '0.0'},
          {'@Relation' : 'Vac',
           '@TrainNumber' : '3',
           '@Delay' : 10,
           '@Lat' : '0.0',
           '@Lon' : '0.0'}
           ]
        }

        # Both empty
        workers.upload_delays_worker(None, active_trains=active_trains)
        workers.upload_delays_worker(data_dict, active_trains=active_trains)
        workers.upload_delays_worker(data_dict_empty, active_trains=active_trains)

        print(active_trains)
        self.compare_record(active_trains['1'][0], data_dict['train_data'][0], data_dict)
        self.assertEqual(active_trains['1'][1], 1, 'Missing counter mismatch')
        self.compare_record(active_trains['2'][0], data_dict['train_data'][1], data_dict)
        self.assertEqual(active_trains['2'][1], 1, 'Missing counter mismatch')
        self.compare_record(active_trains['3'][0], data_dict['train_data'][2], data_dict)
        self.assertEqual(active_trains['3'][1], 1, 'Missing counter mismatch')

    def test_some_update_active_trains(self):
        return
        data_dict_1 = {
        'creation_time' : '17:02:20',
        'day' : '2019-02-03',
        'train_data' :
        [{'@Relation' : 'Pecs',
         '@TrainNumber' : '1',
         '@Delay' : 10,
         '@Lat' : '0.0',
         '@Lon' : '0.0'},
         {'@Relation' : 'Vp',
          '@TrainNumber' : '2',
          '@Delay' : 10,
          '@Lat' : '0.0',
          '@Lon' : '0.0'},
          {'@Relation' : 'Vac',
           '@TrainNumber' : '3',
           '@Delay' : 10,
           '@Lat' : '0.0',
           '@Lon' : '0.0'}
           ]
        }
        data_dict_2 = {
        'creation_time' : '17:22:22',
        'day' : '2019-02-03',
        'train_data' :
        [{'@Relation' : 'Pecs',
         '@TrainNumber' : '1',
         '@Delay' : 12,
         '@Lat' : '0.0',
         '@Lon' : '0.0'},
         {'@Relation' : 'Vp',
          '@TrainNumber' : '2',
          '@Delay' : 12,
          '@Lat' : '0.0',
          '@Lon' : '0.0'},
          {'@Relation' : 'Bocs',
           '@TrainNumber' : '4',
           '@Delay' : 12,
           '@Lat' : '0.0',
           '@Lon' : '0.0'}
           ]
        }
        active_trains = {}
        workers.upload_delays_worker(data_dict_1, active_trains=active_trains)
        workers.upload_delays_worker(data_dict_2, active_trains=active_trains)
        self.compare_record(active_trains['1'][0], data_dict_2['train_data'][0], data_dict_2)
        self.assertEqual(active_trains['1'][1], 0, 'Missing counter mismatch')
        self.compare_record(active_trains['2'][0], data_dict_2['train_data'][1], data_dict_2)
        self.assertEqual(active_trains['2'][1], 0, 'Missing counter mismatch')
        # train '3' not in data_dict_2
        self.compare_record(active_trains['3'][0], data_dict_1['train_data'][2], data_dict_1)
        self.assertEqual(active_trains['3'][1], 1, 'Missing counter mismatch')
        self.compare_record(active_trains['4'][0], data_dict_2['train_data'][2], data_dict_2)
        self.assertEqual(active_trains['4'][1], 0, 'Missing counter mismatch')


    def test_empty_active_trains(self):
        return
        active_trains = {}
        data_dict = {
        'creation_time' : '17:02:20',
        'day' : '2019-02-03',
        'train_data' :
        [{'@Relation' : 'Pecs',
         '@TrainNumber' : '1',
         '@Delay' : 10,
         '@Lat' : '0.0',
         '@Lon' : '0.0'},
         {'@Relation' : 'Vp',
          '@TrainNumber' : '2',
          '@Delay' : 10,
          '@Lat' : '0.0',
          '@Lon' : '0.0'},

           ]
        }

        workers.upload_delays_worker(data_dict, active_trains=active_trains)
        print(active_trains)
        self.compare_record(active_trains['1'][0], data_dict['train_data'][0], data_dict)
        self.assertEqual(active_trains['1'][1], 0, 'Missing counter mismatch')
        self.compare_record(active_trains['2'][0], data_dict['train_data'][1], data_dict)
        self.assertEqual(active_trains['2'][1], 0, 'Missing counter mismatch')
