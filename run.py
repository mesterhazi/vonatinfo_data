from vonatinfo_data import VonatDataGetter
import time

if __name__ == '__main__':
    vonatinfo_data_getter = VonatDataGetter()
    # vonatinfo_data_getter.debug_run()
    vonatinfo_data_getter.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        vonatinfo_data_getter.stop()
    finally:
        pass