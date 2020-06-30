from COVID_Database import COVID_Database
from Display import *
import logging



TIME_SERIES_CONFIRM = "-tsc"
TIME_SERIES_CONFIRM_US = "tscus"
TIME_SERIES_DEATHS = "-tsd"
TIME_SERIES_DEATHS_US = "-tsdus"
TIME_SERIES_RECOVERY = "-tsr"
DAILY_REPORT = "-dr"
DAILY_REPORT_US = "-dru"
FILE_TYPE = [ DAILY_REPORT, DAILY_REPORT_US,TIME_SERIES_RECOVERY,TIME_SERIES_CONFIRM, TIME_SERIES_CONFIRM_US, TIME_SERIES_DEATHS,TIME_SERIES_DEATHS_US]

KEY_PROVINCE = '-p'
KEY_COUNTRY = '-c'
KEY_COMBINE_KEY = '-comb'
KEY = [KEY_PROVINCE, KEY_COUNTRY, KEY_COMBINE_KEY]

class ModifyData():

    def upload(self,file_type, file_path):
        logging.info('module:{}, calling:{}, with:{},{}'.format('ModifyData','upload',file_type, file_path))
        if file_type == TIME_SERIES_CONFIRM:
            COVID_Database.getInstance().Load_Time_Series_Global_Confirmed_Data(file_path)
        elif file_type == TIME_SERIES_CONFIRM_US:
            COVID_Database.getInstance().Load_Time_Series_US_Confirmed_Data(file_path)
        elif file_type == TIME_SERIES_DEATHS:
            COVID_Database.getInstance().Load_Time_Series_Global_Deaths_Data(file_path)
        elif file_type == TIME_SERIES_DEATHS_US:
            COVID_Database.getInstance().Load_Time_Series_US_Deaths_Data(file_path)
        elif file_type == TIME_SERIES_RECOVERY:
            COVID_Database.getInstance().Load_Time_Series_Global_Recovered_Data(file_path)
        elif file_type == DAILY_REPORT:
            COVID_Database.getInstance().Load_Daily_Report_Global_Data(file_path)
        elif file_type == DAILY_REPORT_US:
            COVID_Database.getInstance().Load_Daily_Report_US_Data(file_path)

    def query_driver(self, key, len, key_list, start_date, end_date):
        logging.info('module:{}, calling:{}, with:{},{},{},{},{}'.format('ModifyData','query',key, len, key_list, start_date, end_date))
        query_function = None

        if key == KEY_PROVINCE:
            query_function = COVID_Database.getInstance().query_by_province
        elif key == KEY_COUNTRY:
            query_function = COVID_Database.getInstance().query_by_country
        elif key == KEY_COMBINE_KEY:
                query_function = COVID_Database.getInstance().query_by_combined_key
        else:
            logging.ERROR('ERROR:{},module:{}, calling:{}'.format('invalid key','ModifyData','query'))
            print('invalid key')
            return

        master_list = []
        for i in range(len):
            query_result = query_function(start_date, end_date, key_list[i].replace('_',' '))
            master_list.append(query_result)
        logging.info('DEBUG:master_list{},module:{}, calling:{}'.format(master_list,'ModifyData','query'))

        Display(master_list)
        Display.getInstance().display_on_screen()

