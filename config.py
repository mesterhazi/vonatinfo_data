""" Database config for Upload_all_worker """
upload_all_worker_conf = {
    'database' : ('127.0.0.1', 'train_data'),
    'user' : ('vonat_data_getter', 'user'),
    'table' : 'trains'
}

""" Database config for Final_delay_worker """
final_delay_worker_conf = {
    'database' : ('127.0.0.1', 'train_data'),
    'user' : ('vonat_data_getter', 'user'),
    
}
