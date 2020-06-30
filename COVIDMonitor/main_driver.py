import cmd
import os.path
from os import path
from ModifyData import *
from COVID_Database import COVID_Database
from Display import *
import logging
from datetime import datetime


JSON = 'json'
CSV = 'csv'
PLOT = 'plot'
DEATHS = 'D'
RECOVERED = 'R'
ACTIVE = 'A'
CONFIRMED = 'C'
def is_valid_file(file_path):
    if path.isfile(file_path) and file_path.endswith('.csv'):
        return file_path
    elif file_path.endswith('.csv'):
        print("file does not exist \n")
    else:
        print("file must be in .csv type \n")

def is_csv_file(file_type):
    if file_type in FILE_TYPE:
        return True
    else:
        print("invalid file type")
        print(" Please enter one of the folowing file type: \n",FILE_TYPE)
        print("\n")
        return False


class COVIDMonitor(cmd.Cmd):
    """Monitor COVID 19
    upload [file_path]
    update [file_path]
    query [option]"""

    prompt = 'enter input: '
    intro = """
                Monitor COVID 19

                Commands:
                            upload [file_type] [file_path]
                            update [file_type] [file_path]
                            query [key] [key_length] [key1,key2,...] [date]
                            query [key] [key_length] [key1,key2,...] [start_date] [end_date]
            ----------------------"""
    def do_upload(self, line):
        """
        upload a file to database
        upload [file_path]        [file_path] must be in .csv file type
        ----------------------------"""
        try:
            lines = line.split()
            if len(lines) == 2:
                file_type = lines[0]
                file_path = lines[1]
                if is_csv_file(file_type):
                    if is_valid_file(file_path):
                        ModifyData().upload(file_type, file_path)
            else:              
                print("invalid number of args")
        except:
            print('error occured in upload')
            
    def do_update(self, line):
        """update a file to database
        update [file_path]
        ----------------------------"""
        try:         
            lines = line.split()
            file_type = lines[0]
            file_path = lines[1]
            if is_csv_file(file_type):
                if is_valid_file(file_path):
                    #need to check logic if upgrage is the same as upload
                    ModifyData().upload(file_type, file_path)
        except:
            print('error occured in update')
    def do_query(self,line):
        """Query to see the number of Confirmed, Active, Recovery, Deaths of 1 or more of countries/provinces/combine_key

        query [key] [key_length] [values] [date]
        query [key] [key_length] [values] [start_date] [end_date]

        !note: enter key value with 2 or more words seperated by "_"
        ex: 	New_Mexico

        example: 
        * Query by country with 2 countries(Chile, United_Kingdom) on 06-15-2020
        query -c 2 Chile United_Kingdom 06-15-2020 

        * Query by province with 1 province(Ontario) for data from 06-15-2020 to 06-20-2020
        query -p 1 Ontario  06-15-2020 06-20-2020 
        ----------------------------"""
        # try:
        #     lines = line.split()
        #     query_key = lines[0]
        #     key_length = int(lines[1])
        #     key_list = lines[2:2+key_length]
        #     # missing checking valid dates
        #     start_date = (lines[2+key_length])
        #     datetime.strptime( start_date, "%m-%d-%Y" )
        #     end_date = (lines[-1])
        #     datetime.strptime( end_date, "%m-%d-%Y" )
        #     ModifyData().query_driver(query_key,key_length,key_list,start_date,end_date)
        # except ValueError:
        #     print('Invalid date!')
        # except:
        #     print("invalid query request")

        lines = line.split()
        query_key = lines[0]
        key_length = int(lines[1])
        key_list = lines[2:2+key_length]
        # missing checking valid dates
        start_date = (lines[2+key_length])
        datetime.strptime( start_date, "%m-%d-%Y" )
        end_date = (lines[-1])
        datetime.strptime( end_date, "%m-%d-%Y" )
        ModifyData().query_driver(query_key,key_length,key_list,start_date,end_date)

    def do_export(self, line):
        """export queried data
        
        To export data to json file:
        enter input: export json
        
        To export data to csv file:
        enter input: export csv
        
        To see the graph 
        enter input: export plot D     !-- for deaths records
                     export plot A     !-- for actives records
                     export plot C     !-- for confirms records
                     export plot R     !-- for recovered records"""

        # try:
        line = line.split()
        display = Display.getInstance()
        if display != None:
            if line[0] == JSON:
                display.export_JSON()
            elif line[0] == CSV:
                display.export_CSV()
            elif line[0] == PLOT:
                if line[1] == DEATHS:
                    display.plot(DEATHS)
                if line[1] == RECOVERED:
                    display.plot(RECOVERED)
                if line[1] == ACTIVE:
                    display.plot(ACTIVE)
                if line[1] == CONFIRMED:
                    display.plot(CONFIRMED)
            else:
                print("invalid request")
        # except:
        #     print('error occured while export')



    def do_EOF(self, line):
        return True



if __name__ == '__main__':
    logging.basicConfig(filename='COVIDMonitor.log', level = logging.DEBUG,format='%(asctime)s %(message)s')
    logging.info('******************************************************************************************')
    logging.info('Started')
    logging.info('******************************************************************************************')
    COVIDMonitor().cmdloop()
    


