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


    def test_empty_active_trains(self):
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
          {'@Relation' : 'Vac',
           '@TrainNumber' : '3',
           '@Delay' : 10,
           '@Lat' : '0.0',
           '@Lon' : '0.0'}
           ]
        }

        workers.upload_delays_worker(data_dict, active_trains=active_trains)
        print(active_trains)
        self.compare_record(active_trains['1'][0], data_dict['train_data'][0], data_dict)
        self.assertEqual(active_trains['1'][1], 0, 'Missing counter mismatch')
        self.compare_record(active_trains['2'][0], data_dict['train_data'][1], data_dict)
        self.assertEqual(active_trains['2'][1], 0, 'Missing counter mismatch')
        self.compare_record(active_trains['3'][0], data_dict['train_data'][2], data_dict)
        self.assertEqual(active_trains['3'][1], 0, 'Missing counter mismatch')
