from datetime import datetime
from sqlalchemy import Column, Integer, Float, Date, String, DateTime, ForeignKey, exc
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import csv





# Global variables for database connections
DB_NAME = "postgres"
DB_PASSWORD = "postgres"
DB_USER = "postgres"
DB_HOST = "postgres"
DB_PORT = "5432"
DB_URL = "postgres://password:postgres@localhost:5432/postgres"



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
    

class covid_daily_data(Base):
    __tablename__ = 'covid_daily_data'
    area_id = Column(Integer,ForeignKey('covid_area_data.id'), primary_key=True)
    date = Column(DateTime, primary_key=True, nullable=False)
    confirmed = Column(Integer)
    death = Column(Integer)
    recovered = Column(Integer)
    active = Column(Integer)






#  this function loads data from time_series_covid19_confirmed_US.csv file 
def Load_Time_Series_US_Confirmed_Data(file_name, session): 
    with open(file_name) as infile:
        reader = csv.reader(infile)
        headers = next(reader)  # Read first line for headers
        num_columns = len(headers)
        Returned_row_list = []
        # Add our desired other columns
        for row in reader:
            #  check if the area entry already exists in the database
            id = session.query(covid_area_data.id).filter(covid_area_data.admin2==row[5], covid_area_data.province == row[6], covid_area_data.country == row[7]).all()
            print("id queried is ", id);
            # if not create the entry
            if (len(id)==0):
                new_area_entry = covid_area_data(
                    admin2=row[5], 
                    province = row[6],
                    country = row[7],
                    )
                session.add(new_area_entry)
                session.flush()
                print("created id is", new_area_entry.id)
                id = new_area_entry.id
            else:
                id = id[0]
            for i in range(11, num_columns): 
                #  user merge instead of add which does the same thing as upsert, update or insert
                session.merge(covid_daily_data(
                    area_id = id, 
                    # 18/09/19 01:55:19 %d/%m/%y %H:%M:%S
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
        Returned_row_list = []
        # Add our desired other columns
        for row in reader:
            #  check if the area entry already exists in the database
            id = session.query(covid_area_data.id).filter(covid_area_data.province == row[0], covid_area_data.country == row[1]).all()
            print("id queried is ", id);
            # if not create the entry
            if (len(id)==0):
                new_area_entry = covid_area_data(
                    province = row[0],
                    country = row[1],
                    )
                session.add(new_area_entry)
                session.flush()
                print("created id is", new_area_entry.id)
                id = new_area_entry.id
            else:
                id = id[0]
            for i in range(4, num_columns): 
                #  user merge instead of add which does the same thing as upsert, update or insert
                session.merge(covid_daily_data(
                    area_id = id, 
                    # 18/09/19 01:55:19 %d/%m/%y %H:%M:%S
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
        Returned_row_list = []
        # Add our desired other columns
        for row in reader:
            #  check if the area entry already exists in the database
            id = session.query(covid_area_data.id).filter(covid_area_data.admin2==row[5], covid_area_data.province == row[6], covid_area_data.country == row[7]).all()
            print("id queried is ", id);
            # if not create the entry
            if (len(id)==0):
                new_area_entry = covid_area_data(
                    admin2=row[5], 
                    province = row[6],
                    country = row[7],
                    )
                session.add(new_area_entry)
                session.flush()
                print("created id is", new_area_entry.id)
                id = new_area_entry.id
            else:
                id = id[0]
            for i in range(12, num_columns): 
                #  user merge instead of add which does the same thing as upsert, update or insert
                session.merge(covid_daily_data(
                    area_id = id, 
                    # 18/09/19 01:55:19 %d/%m/%y %H:%M:%S
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
        Returned_row_list = []
        # Add our desired other columns
        for row in reader:
            #  check if the area entry already exists in the database
            id = session.query(covid_area_data.id).filter(covid_area_data.province == row[0], covid_area_data.country == row[1]).all()
            print("id queried is ", id);
            # if not create the entry
            if (len(id)==0):
                new_area_entry = covid_area_data(
                    province = row[0],
                    country = row[1],
                    )
                session.add(new_area_entry)
                session.flush()
                print("created id is", new_area_entry.id)
                id = new_area_entry.id
            else:
                id = id[0]
            for i in range(4, num_columns): 
                #  user merge instead of add which does the same thing as upsert, update or insert
                session.merge(covid_daily_data(
                    area_id = id, 
                    # 18/09/19 01:55:19 %d/%m/%y %H:%M:%S
                    date=datetime.strptime(headers[i], '%m/%d/%y'),
                    death = row[i]
                    ))
    session.commit()

# a function that parses csv files passed in in to a list of time_series_us objects. 
#  this function loads data from time_series_covid19_recovered_global.csv file 
def Load_Time_Series_Global_Recovered_Data(file_name, session): 
    with open(file_name) as infile:
        reader = csv.reader(infile)
        headers = next(reader)  # Read first line for headers
        num_columns = len(headers)
        Returned_row_list = []
        # Add our desired other columns
        for row in reader:
            #  check if the area entry already exists in the database
            id = session.query(covid_area_data.id).filter(covid_area_data.province == row[0], covid_area_data.country == row[1]).all()
            print("id queried is ", id);
            # if not create the entry
            if (len(id)==0):
                new_area_entry = covid_area_data(
                    province = row[0],
                    country = row[1],
                    )
                session.add(new_area_entry)
                session.flush()
                print("created id is", new_area_entry.id)
                id = new_area_entry.id
            else:
                id = id[0]
            for i in range(4, num_columns): 
                #  user merge instead of add which does the same thing as upsert, update or insert
                session.merge(covid_daily_data(
                    area_id = id, 
                    # 18/09/19 01:55:19 %d/%m/%y %H:%M:%S
                    date=datetime.strptime(headers[i], '%m/%d/%y'),
                    recovered = row[i]
                    ))
    session.commit()




if __name__ == "__main__":
    # create database connection
    engine  = create_engine(DB_URL, echo=False)
    Base.metadata.create_all(engine)
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()
    
    try:
        data = Load_Time_Series_Global_Recovered_Data('time_series_covid19_recovered_global.csv', s)
        # s.add_all(data)
        s.commit() #Attempt to commit all the records
    except exc.SQLAlchemyError as e:
        print("error occurs")
        print(e)
        s.rollback() #Rollback the changes on error
    finally:
        s.close() 



