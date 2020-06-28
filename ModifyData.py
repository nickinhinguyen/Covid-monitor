TIME_SERIES_CONFIRM = "-tsc"
TIME_SERIES_CONFIRM_US = "tscus"
TIME_SERIES_DEATHS = "-tsd"
TIME_SERIES_DEATHS_US = "-tsdus"
TIME_SERIES_RECOVERY = "-tsr"
DAILY_REPORT = "-dr"
DAILY_REPORT_US = "-dru"
FILE_TYPE = [ TIME_SERIES_RECOVERY,TIME_SERIES_CONFIRM, TIME_SERIES_CONFIRM_US, TIME_SERIES_DEATHS,TIME_SERIES_DEATHS_US]


class ModifyData():

    def upload(self,file_type, file_path):
        if file_type == TIME_SERIES_CONFIRM:
            Load_Time_Series_Global_Confirmed_Data(file_path)
        elif file_type == TIME_SERIES_CONFIRM_US:
            Load_Time_Series_US_Confirmed_Data(file_path)
        elif file_type == TIME_SERIES_DEATHS:
            Load_Time_Series_Global_Deaths_Data(file_path)
        elif file_type == TIME_SERIES_DEATHS_US:
            Load_Time_Series_US_Deaths_Data(file_path)
        elif file_type == TIME_SERIES_RECOVERY:
            Load_Time_Series_Global_Recovered_Data(file_path)
        elif file_type == DAILY_REPORT:
            Load_Daily_Report_Global_Data(file_path)
        elif file_type == DAILY_REPORT_US:
            Load_Daily_Report_US_Data(file_path)


def query_by_combined_key(self,start_date, end_date, combine_key):
		combine_keys = combine_key.split(',')
		if len(combine_keys) == 2:
			query_by_province_country(start_date, end_date,combine_keys[0], combine_keys[1])
		elif len(combine_keys) == 3:
			query_by_admin2_province_country(start_date, end_date,combine_keys[0], combine_keys[1],combine_key[2])
		else:
			print('invalid combined key')