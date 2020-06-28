from datetime import datetime
from sqlalchemy import Column, Integer, Float, Date, String, DateTime, ForeignKey, exc
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

from time import time
import csv



# Global variables for database connections
DB_NAME = "hoyhezpp"
DB_PASSWORD = "SEusEXGHqz4qEzcPbbDnmQw98C9GTuHk"
DB_USER = "hoyhezpp"
DB_HOST = "hansken.db.elephantsql.com"
DB_PORT = "5432"
DB_URL = "postgres://hoyhezpp:SEusEXGHqz4qEzcPbbDnmQw98C9GTuHk@hansken.db.elephantsql.com:5432/hoyhezpp"
LOCALHOST_URL = "postgres://postgres:postgres@localhost:5432/postgres"
# change to DB_URL for production
URL_in_use = LOCALHOST_URL


class COVID_Database:
    
    __Base = declarative_base()

    __instance = None

    class covid_area_data(__Base):
        __tablename__ = 'covid_area_data'
        #tell SQLAlchemy the name of column and its attributes:
        id = Column(Integer, primary_key=True, autoincrement=True)
        admin2 = Column(String,  nullable=True)
        province = Column(String)
        country = Column(String)
    
    # uses float because some csv has number of 10548.0 which is invalid input syntax for type integer
    class covid_daily_data(__Base):
        __tablename__ = 'covid_daily_data'
        area_id = Column(Integer,ForeignKey('covid_area_data.id'), primary_key=True)
        date = Column(DateTime, primary_key=True, nullable=False)
        confirmed = Column(Float)
        death = Column(Float)
        recovered = Column(Float)
        active = Column(Float)

    def __init__(self, postgresurl):
        if self.__instance != None:
            raise Exception("The DB connection already exist!")
        else:
            engine = create_engine(postgresurl, echo=False)
            self.__Base.metadata.create_all(engine)
            session = sessionmaker()
            session.configure(bind=engine)
            self.session = session()
            self.engine = engine
            COVID_Database.__instance = self

    @staticmethod
    def getInstance():
        """ Static access method. """
        if COVID_Database.__instance == None:
            COVID_Database(URL_in_use)
        return COVID_Database.__instance 

    def disconnect_destroy(self):
        self.session.close()
        self.engine.dispose()
        COVID_Database.__instance = None

    def rollback_session(self):
        self.session.rollback()

    def query_all(self):
        session = self.session
        covid_daily_data=self.covid_daily_data
        covid_area_data=self.covid_area_data
        result = session.query(
            covid_daily_data.death,
            covid_daily_data.confirmed,
            covid_daily_data.recovered,
            covid_daily_data.active,
            covid_daily_data.date,
            covid_area_data.province,
            covid_area_data.country,
            covid_area_data.admin2
        ).filter(
            covid_area_data.id == covid_daily_data.area_id
        ).all()
        print(result)
        return (result)


    def query_by_province(self, datetime_start, datetime_end, province):
        session = self.session
        covid_daily_data=self.covid_daily_data
        covid_area_data=self.covid_area_data
        result = session.query(
            func.sum(covid_daily_data.death).label('death'),
            func.sum(covid_daily_data.confirmed).label('confirmed'),
            func.sum(covid_daily_data.recovered).label('recovered'),
            func.sum(covid_daily_data.active).label('active'),
            covid_daily_data.date,
            covid_area_data.province,
        ).filter(
            covid_area_data.id == covid_daily_data.area_id, 
            covid_daily_data.date <= datetime_end,
            covid_daily_data.date >= datetime_start,
            covid_area_data.province == province,
        ).group_by(
            covid_area_data.province,
            covid_daily_data.date
        ).all()
        print(result)
        return (result)


    def query_by_admin2_province_country(self, datetime_start, datetime_end,admin2, province, country):
        session = self.session
        covid_daily_data=self.covid_daily_data
        covid_area_data=self.covid_area_data
        result = session.query(
            covid_daily_data.death,
            covid_daily_data.confirmed,
            covid_daily_data.recovered,
            covid_daily_data.active,
            covid_daily_data.date,
            covid_area_data.province,
            covid_area_data.country,
            covid_area_data.admin2
        ).filter(
            covid_area_data.id == covid_daily_data.area_id, 
            covid_daily_data.date <= datetime_end,
            covid_daily_data.date >= datetime_start,
            covid_area_data.province == province,
            covid_area_data.country == country,
            covid_area_data.admin2 == admin2
        ).all()
        return result
    

    # this function will assumes that the entries are not duplicates, 
    # e.g if canada has data recorded of death/province, there is no entry of death/entire country 
    # it's known that US has above duplicate, will only query the entry for the entire country 
    def query_by_country(self, datetime_start, datetime_end, country):
        session = self.session
        covid_daily_data=self.covid_daily_data
        covid_area_data=self.covid_area_data
        if (country == "US"):
            result = session.query(
                covid_daily_data.death,
                covid_daily_data.confirmed,
                covid_daily_data.recovered,
                covid_daily_data.active,
                covid_daily_data.date,
                covid_area_data.country
            ).filter(
                covid_area_data.id == covid_daily_data.area_id, 
                covid_daily_data.date <= datetime_end,
                covid_daily_data.date >= datetime_start,
                covid_area_data.province == None,
                covid_area_data.admin2 == None,
                covid_area_data.country == "US"
            )
            return result
        # for other countries 
        result = session.query(
        func.sum(covid_daily_data.death).label('death'),
        func.sum(covid_daily_data.confirmed).label('confirmed'),
        func.sum(covid_daily_data.recovered).label('recovered'),
        func.sum(covid_daily_data.active).label('active'),
        covid_daily_data.date,
        covid_area_data.country,
        ).filter(
        covid_area_data.id == covid_daily_data.area_id, 
        covid_daily_data.date <= datetime_end,
        covid_daily_data.date >= datetime_start,
        covid_area_data.province == province,
        covid_area_data.country == country,
        ).group_by(
        covid_area_data.country,
        covid_daily_data.date
        ).all()
        return (result)

    def query_by_province_country(self, datetime_start, datetime_end, province, country):
        session = self.session
        covid_daily_data=self.covid_daily_data
        covid_area_data=self.covid_area_data
        result = session.query(
            func.sum(covid_daily_data.death).label('death'),
            func.sum(covid_daily_data.confirmed).label('confirmed'),
            func.sum(covid_daily_data.recovered).label('recovered'),
            func.sum(covid_daily_data.active).label('active'),
            covid_daily_data.date,
            covid_area_data.province,
            covid_area_data.country,
        ).filter(
            covid_area_data.id == covid_daily_data.area_id, 
            covid_daily_data.date <= datetime_end,
            covid_daily_data.date >= datetime_start,
            covid_area_data.province == province,
            covid_area_data.country == country,
        ).group_by(
            covid_area_data.province,
            covid_daily_data.date,
            covid_area_data.country
        ).all()
        return (result)

    def query_by_combined_key(self,start_date, end_date, combine_key):
        combine_keys = combine_key.split(',')
        if len(combine_keys) == 2:
            self.query_by_province_country(start_date, end_date,combine_keys[0], combine_keys[1])
        elif len(combine_keys) == 3:
            self.query_by_admin2_province_country(start_date, end_date,combine_keys[0], combine_keys[1],combine_key[2])
        else:
            print('invalid combined key')
    #  this function loads data from time_series_covid19_confirmed_US.csv file 
    def Load_Time_Series_US_Confirmed_Data(self, file_name):
        session = self.session
        covid_daily_data=self.covid_daily_data
        covid_area_data=self.covid_area_data 
        with open(file_name) as infile:
            reader = csv.reader(infile)
            headers = next(reader)  # Read first line for headers
            num_columns = len(headers)
            # Add our desired other columns
            for row in reader:
                # replaces empty string with None so they appear as NULL in database
                row = [None if x=="" else x for x in row]
                print("a row is processed")
                #  check if the area entry already exists in the database
                id = session.query(covid_area_data.id).filter(covid_area_data.admin2==row[5], covid_area_data.province == row[6], covid_area_data.country == row[7]).all()
                # if not create the entry
                if (len(id)==0):
                    new_area_entry = covid_area_data(
                        admin2=row[5], 
                        province = row[6],
                        country = row[7],
                        )
                    session.add(new_area_entry)
                    session.flush()
                    id = new_area_entry.id
                else:
                    id = id[0]
                for i in range(11, num_columns): 
                    #  user merge instead of add which does the same thing as upsert, update or insert
                    session.merge(covid_daily_data(
                        area_id = id, 
                        date=datetime.strptime(headers[i], '%m/%d/%y'),
                        confirmed = row[i]
                        ))
        session.commit()

    #  this function loads data from time_series_covid19_confirmed_global.csv file 
    def Load_Time_Series_Global_Confirmed_Data(self, file_name): 
        session = self.session
        covid_daily_data=self.covid_daily_data
        covid_area_data=self.covid_area_data 
        with open(file_name) as infile:
            reader = csv.reader(infile)
            headers = next(reader)  # Read first line for headers
            num_columns = len(headers)
            # Add our desired other columns
            for row in reader:
                # replaces empty string with None so they appear as NULL in database
                row = [None if x=="" else x for x in row]
                print("a row is processed")
                #  check if the area entry already exists in the database
                id = session.query(covid_area_data.id).filter(covid_area_data.province == row[0], covid_area_data.country == row[1]).all()
                # if not create the entry
                if (len(id)==0):
                    new_area_entry = covid_area_data(
                        province = row[0],
                        country = row[1],
                        )
                    session.add(new_area_entry)
                    session.flush()
                    id = new_area_entry.id
                else:
                    id = id[0]
                for i in range(4, num_columns): 
                    #  user merge instead of add which does the same thing as upsert, update or insert
                    session.merge(covid_daily_data(
                        area_id = id, 
                        date=datetime.strptime(headers[i], '%m/%d/%y'),
                        confirmed = row[i]
                        ))
        session.commit()

    #  this function loads data from time_series_covid19_deaths_US.csv file 
    def Load_Time_Series_US_Deaths_Data(self, file_name): 
        session = self.session
        covid_daily_data=self.covid_daily_data
        covid_area_data=self.covid_area_data 
        with open(file_name) as infile:
            reader = csv.reader(infile)
            headers = next(reader)  # Read first line for headers
            num_columns = len(headers)
            # Add our desired other columns
            for row in reader:
                # replaces empty string with None so they appear as NULL in database
                row = [None if x=="" else x for x in row]
                print("a row is processed")
                #  check if the area entry already exists in the database
                id = session.query(covid_area_data.id).filter(covid_area_data.admin2==row[5], covid_area_data.province == row[6], covid_area_data.country == row[7]).all()
                # if not create the entry
                if (len(id)==0):
                    new_area_entry = covid_area_data(
                        admin2=row[5], 
                        province = row[6],
                        country = row[7],
                        )
                    session.add(new_area_entry)
                    session.flush()
                    id = new_area_entry.id
                else:
                    id = id[0]
                for i in range(12, num_columns): 
                    #  user merge instead of add which does the same thing as upsert, update or insert
                    session.merge(covid_daily_data(
                        area_id = id, 
                        date=datetime.strptime(headers[i], '%m/%d/%y'),
                        confirmed = row[i]
                        ))
        session.commit()
    
    #  this function loads data from time_series_covid19_deaths_global.csv file 
    def Load_Time_Series_Global_Deaths_Data(self, file_name): 
        session = self.session
        covid_daily_data=self.covid_daily_data
        covid_area_data=self.covid_area_data 
        with open(file_name) as infile:
            reader = csv.reader(infile)
            headers = next(reader)  # Read first line for headers
            num_columns = len(headers)
            # Add our desired other columns
            for row in reader:
                # replaces empty string with None so they appear as NULL in database
                row = [None if x=="" else x for x in row]
                print("a row is processed")
                #  check if the area entry already exists in the database
                id = session.query(covid_area_data.id).filter(covid_area_data.province == row[0], covid_area_data.country == row[1]).all()
                # if not create the entry
                if (len(id)==0):
                    new_area_entry = covid_area_data(
                        province = row[0],
                        country = row[1],
                        )
                    session.add(new_area_entry)
                    session.flush()
                    id = new_area_entry.id
                else:
                    id = id[0]
                for i in range(4, num_columns): 
                    #  user merge instead of add which does the same thing as upsert, update or insert
                    session.merge(covid_daily_data(
                        area_id = id, 
                        date=datetime.strptime(headers[i], '%m/%d/%y'),
                        death = row[i]
                        ))
        session.commit()

    #  this function loads data from time_series_covid19_recovered_global.csv file 
    def Load_Time_Series_Global_Recovered_Data(self, file_name): 
        session = self.session
        covid_daily_data=self.covid_daily_data
        covid_area_data=self.covid_area_data 
        with open(file_name) as infile:
            reader = csv.reader(infile)
            headers = next(reader)  # Read first line for headers
            num_columns = len(headers)
            # Add our desired other columns
            
            for row in reader:
                #  check if the area entry already exists in the database
                # replaces empty string with None so they appear as NULL in database
                row = [None if x=="" else x for x in row]
                print("a row is processed")
                id = session.query(covid_area_data.id).filter(covid_area_data.province == row[0], covid_area_data.country == row[1]).all()
                # if not create the entry
                if (len(id)==0):
                    new_area_entry = covid_area_data(
                        province = row[0],
                        country = row[1],
                        )
                    session.add(new_area_entry)
                    session.flush()
                    id = new_area_entry.id
                else:
                    id = id[0]
                t = time()
                for i in range(4, num_columns): 
                    #  user merge instead of add which does the same thing as upsert, update or insert
                    session.merge(covid_daily_data(
                        area_id = id, 
                        date=datetime.strptime(headers[i], '%m/%d/%y'),
                        recovered = row[i]
                        ))
        session.commit()

    #  this function loads data from csv files in csse_covid_19_daily_reports folder
    def Load_Daily_Report_Global_Data(self, file_name): 
        session = self.session
        covid_daily_data=self.covid_daily_data
        covid_area_data=self.covid_area_data 
        # get the correct date
        date = datetime.strptime(file_name[:-4], '%m-%d-%Y')
        print(date)
        with open(file_name) as infile:
            reader = csv.reader(infile)
            headers = next(reader)  # Read first line for headers
                    # Add our desired other columns
            for row in reader:
                # replaces empty string with None so they appear as NULL in database
                row = [None if x=="" else x for x in row]
                print("a row is processed")
                #  check if the area entry already exists in the database if not, create one. 
                id = session.query(covid_area_data.id).filter(covid_area_data.admin2==row[1], covid_area_data.province == row[2], covid_area_data.country == row[3]).all()
                # if not create the entry
                if (len(id)==0):
                    new_area_entry = covid_area_data(
                        admin2=row[1], 
                        province = row[2],
                        country = row[3],
                        )
                    session.add(new_area_entry)
                    session.flush()
                    id = new_area_entry.id
                else:
                    id = id[0]
                #  user merge instead of add which does the same thing as upsert, update or insert
                session.merge(covid_daily_data(
                    area_id = id, 
                    date= date,
                    confirmed = row[7],
                    death = row[8],
                    recovered = row[9],
                    active = row[10]
                    ))
        session.commit()

    #  this function loads data from csv files in csse_covid_19_daily_reports_us folder 
    #  this function is No longer in use, as the user input of US daily report is now prohibited
    def Load_Daily_Report_US_Data(self, file_name): 
        session = self.session
        covid_daily_data=self.covid_daily_data
        covid_area_data=self.covid_area_data 
        # get the correct date
        date = datetime.strptime(file_name[:-4], '%m-%d-%Y')
        print(date)
        with open(file_name) as infile:
            reader = csv.reader(infile)
            headers = next(reader)  # Read first line for headers
                    # Add our desired other columns
            for row in reader:
                # replaces empty string with None so they appear as NULL in database
                row = [None if x=="" else x for x in row]
                print("a row is processed")
                #  check if the area entry already exists in the database if not, create one. 
                id = session.query(covid_area_data.id).filter( covid_area_data.province == row[0], covid_area_data.country == row[1]).all()
                # if not create the entry
                if (len(id)==0):
                    new_area_entry = covid_area_data(
                        province = row[0],
                        country = row[1],
                        )
                    session.add(new_area_entry)
                    session.flush()
                    id = new_area_entry.id
                else:
                    id = id[0]
                #  check if the valuesare valid 
                #  user merge instead of add which does the same thing as upsert, update or insert
                session.merge(covid_daily_data(
                    area_id = id, 
                    date= date,
                    confirmed = row[5],
                    death = row[6],
                    recovered = row[7],
                    active = row[8]
                    ))
        session.commit()


    











if __name__ == "__main__":
    try:
        sqldatabase = COVID_Database.getInstance()
        # # 6-16 6-18 are global daily report, 6-17 is us daily report
        # sqldatabase.Load_Daily_Report_Global_Data('06-16-2020.csv')
        # sqldatabase.Load_Daily_Report_Global_Data('06-18-2020.csv')
        # time1 = datetime.strptime("06-17-2020", '%m-%d-%Y')
        # time2 = datetime.strptime("12-31-2030", '%m-%d-%Y')
        sqldatabase.query_all()
    except exc.SQLAlchemyError as e:
        print("error occurs")
        print(e)
        sqldatabase.rollback_session() #Rollback the changes on error
    finally:
        sqldatabase.disconnect_destroy()
    #     print ("Time elapsed: " + str(time() - t) + " s.")





