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
#  this function is No longer in use, as the user input of US daily report is now prohibited
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





# datetime_start, datetime_end are both python datetime object, session is sqlalchemy session, 
# only US actually uses the admin2 column, this function is only called for US data. 
# so basically this function will read data with admin2 != None
def query_by_province(datetime_start, datetime_end, province, session):
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

def query_by_admin2_province_country(datetime_start, datetime_end,admin2, province, country, session):
    result = session.query(
        covid_daily_data.death,
        covid_daily_data.confirmed,
        covid_daily_data.recovered,
        covid_daily_data.active
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

def query_by_province_country(datetime_start, datetime_end, province, country, session):
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

# this function will assumes that the entries are not duplicates, 
# e.g if canada has data recorded of death/province, there is no entry of death/entire country 
# it's known that US has above duplicate, will only query the entry for the entire country 
def query_by_country(datetime_start, datetime_end, country, session):
    if (country == "US"):
        result = session.query(
            covid_daily_data.death,
            covid_daily_data.confirmed,
            covid_daily_data.recovered,
            covid_daily_data.active
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


if __name__ == "__main__":
    # create database connection
    
    engine  = create_engine(LOCALHOST_URL, echo=False)
    Base.metadata.create_all(engine)
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()
    t = time()
    try:
        # # 6-16 6-18 are global daily report, 6-17 is us daily report
        # Load_Daily_Report_Global_Data('06-16-2020.csv', s)
        # Load_Daily_Report_Global_Data('06-18-2020.csv', s)
        time1 = datetime.strptime("06-15-2020", '%m-%d-%Y')
        time2 = datetime.strptime("12-31-2030", '%m-%d-%Y')
        query_by_province(time1, time2, "South Carolina", s)
    except exc.SQLAlchemyError as e:
        print("error occurs")
        print(e)
        s.rollback() #Rollback the changes on error
    finally:
        s.close() 
    print ("Time elapsed: " + str(time() - t) + " s.")





