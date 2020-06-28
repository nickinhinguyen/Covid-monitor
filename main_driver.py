from datetime import datetime
from sqlalchemy import Column, Integer, Float, Date, String, DateTime, ForeignKey, exc
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
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


# define the database schemas
Base = declarative_base()


# specify the table.
class covid_area_data(Base):
    __tablename__ = 'covid_area_data'
    #tell SQLAlchemy the name of column and its attributes:
    id = Column(Integer, primary_key=True, autoincrement=True)
    admin2 = Column(String,  nullable=True)
    province = Column(String)
    country = Column(String)
    
# uses float because some csv has number of 10548.0 which is invalid input syntax for type integer
class covid_daily_data(Base):
    __tablename__ = 'covid_daily_data'
    area_id = Column(Integer,ForeignKey('covid_area_data.id'), primary_key=True)
    date = Column(DateTime, primary_key=True, nullable=False)
    confirmed = Column(Float)
    death = Column(Float)
    recovered = Column(Float)
    active = Column(Float)

#  this function loads data from time_series_covid19_confirmed_US.csv file 
def Load_Time_Series_US_Confirmed_Data(file_name, session): 
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
def Load_Time_Series_Global_Confirmed_Data(file_name, session): 
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
def Load_Time_Series_US_Deaths_Data(file_name, session): 
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
def Load_Time_Series_Global_Deaths_Data(file_name, session): 
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
def Load_Time_Series_Global_Recovered_Data(file_name, session): 
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
def Load_Daily_Report_Global_Data(file_name, session): 
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
def Load_Daily_Report_US_Data(file_name, session): 
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
    # create database connection
    
    engine  = create_engine(LOCALHOST_URL, echo=False)
    Base.metadata.create_all(engine)
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()
    t = time()
    try:
        data = Load_Daily_Report_US_Data('06-17-2020.csv', s)
    except exc.SQLAlchemyError as e:
        print("error occurs")
        print(e)
        s.rollback() #Rollback the changes on error
    finally:
        s.close() 
    print ("Time elapsed: " + str(time() - t) + " s.")




