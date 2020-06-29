import cmd
import os.path
from os import path
from ModifyData import *
from COVID_Database import COVID_Database
from Display import *
import logging



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

        example: 
        * Query by country with 2 countries(Chile, Brazil) on 06-15-2020
        query -c 2 Chile Vietnam 06-15-2020 

        * Query by province with 1 province(Ontario) for data from 06-15-2020 to 06-20-2020
        query -p 1 Ontario  06-15-2020 06-20-2020 
        ----------------------------"""
        try:
            lines = line.split()
            query_key = lines[0]
            key_length = int(lines[1])
            key_list = lines[2:2+key_length]
            # missing checking valid dates
            start_date = (lines[2+key_length])
            end_date = (lines[-1])
            ModifyData().query_driver(query_key,key_length,key_list,start_date,end_date)
        except:
            print("invalid query request")

    def do_EOF(self, line):
        return True



if __name__ == '__main__':
    logging.basicConfig(filename='COVIDMonitor.log', level = logging.DEBUG,format='%(asctime)s %(message)s')
    logging.info('******************************************************************************************')
    logging.info('Started')
    logging.info('******************************************************************************************')
    COVIDMonitor().cmdloop()
    


