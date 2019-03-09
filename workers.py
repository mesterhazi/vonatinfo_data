import logging
import threading
import time
from DataBaseHandler import DataBaseHandler
from config import upload_all_worker_conf, final_delay_worker_conf

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
handler = logging.FileHandler('../vonat_data.log')

logger.addHandler(handler)

def upload_all_worker(data_dict, *args, **kwargs):
    """ Worker thread utilized by the VonatDataGetter classself.
    Uploads all data in the trains """
    database = upload_all_worker_conf['database']
    user = upload_all_worker_conf['user']
    table = upload_all_worker_conf['table']
    if data_dict is None:
        logger.error('Empty dictionary received!')
        return
    try:
        db_handler = DataBaseHandler(database=database, user=user)
    except Exception as e:
        logger.error(
        'Could not initialize a DataBaseHandler instance with the given parameters:db:{}, user:{}'.format(
        database, user))
        logger.error(str(e))
        return

    db_handler.ping(reconnect=True)  # reconnects if needed
    insert = """INSERT INTO {} (creation_time, day, relation, train_number, line, delay, elvira_id, coord_lat, coord_lon, company)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""".format(table)

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
        db_handler.upload_records_commit(insert, record_list)
    finally:
        db_handler.close()
        logger.info('Database connection closed.')



def upload_delays_worker(data_dict, *args, **kwargs):
    """ Upload final delays for every train every day.
    active_trains = {train_number : (record, missing_counter)}"""
    if data_dict is None:
        logger.error('Empty dictionary received!')
        return
    active_trains = kwargs.get('active_trains')
    database = final_delay_worker_conf['database']
    user = final_delay_worker_conf['user']
    table = final_delay_worker_conf['table']

    data_dict_train_numbers = tuple(tr['@TrainNumber'] for tr in data_dict['train_data'])
    for data in data_dict['train_data']:
        active_train_record = [ # Add train to active_trains
            (data_dict['creation_time'],
            data_dict['day'],
            data.get('@Relation'),
            data['@TrainNumber'],
            data['@Delay'],
            data['@Lat'],
            data['@Lon']),
            0] # Reset missing_counter for every train present
        active_trains[data['@TrainNumber']] = active_train_record

    trains_arrived = []
    active_trains_to_del = []
    for key, value in active_trains.items():
        if key not in data_dict_train_numbers:
            active_trains[key][1] = value[1] + 1  # increment missing_counter
        if active_trains[key][1] >= final_delay_worker_conf['missing_threshold']:
            trains_arrived.append(value[0])
            active_trains_to_del.append(key)

    for k in active_trains_to_del:
        active_trains.pop(k, None)

    logger.info('{} trains arrived.'.format(len(trains_arrived)))
    if trains_arrived == []:
        return # nothing to upload

    try:
        db_handler = DataBaseHandler(database=database, user=user)
    except Exception as e:
        logger.error(
        'Could not initialize a DataBaseHandler instance with the given parameters:db:{}, user:{}'.format(
        database, user))
        logger.error(str(e))
        return trains_arrived

    db_handler.ping(reconnect=True)  # reconnects if needed
    insert = """INSERT INTO {} (creation_time, day, relation, train_number, delay, coord_lat, coord_lon)
            VALUES (%s, %s, %s, %s, %s, %s, %s);""".format(table)
    logger.info('Connected to database.')
    try:
        db_handler.upload_records_commit(insert, trains_arrived)
    finally:
        if db_handler is not None:
            db_handler.close()
            logger.info('Database connection closed.')
        return trains_arrived
