import cmd
import os.path
from os import path
from ModifyData import *
git

def is_valid_file(file_path):
    if path.isfile(file_path) and file_path.endswith('.csv'):
        print('correct file path')
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
                            query [option]
            ----------------------"""
    def do_upload(self, line):
        """
        upload a file to database
        upload [file_path]        [file_path] must be in .csv file type
        ----------------------------"""
        
        lines = line.split()
        if len(lines) == 2:
            file_type = lines[0]
            file_path = lines[1]
            if is_csv_file(file_type):
                if is_valid_file(file_path):
                    ModifyData().upload(file_type, file_path)
        else:
            print("invalid number of args")

    def do_update(self, line):
        """update a file to database
        update [file_path]
        ----------------------------"""
        lines = line.split()
        file_type = lines[0]
        file_path = lines[1]
        if is_csv_file(file_type):
            if is_valid_file(file_path):
                #need to check logic if upgrage is the same as upload
                ModifyData().upload(file_type, file_path)
    
    def do_EOF(self, line):
        return True



if __name__ == '__main__':
    COVIDMonitor().cmdloop()


