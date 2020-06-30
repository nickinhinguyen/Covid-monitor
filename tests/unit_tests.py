from COVIDMonitor.COVID_Database import COVID_Database
import pytest
import datetime
import os

dirname = os.path.dirname(os.path.abspath(__file__))


# this is the url to the test database. This database will be reset everytime
test_db_url = "postgres://lmigwaef:mrsdCiLEmZ6qNJ9kQk0x_v-CMaEktGoL@ruby.db.elephantsql.com:5432/lmigwaef"

test_db_url = "postgres://postgres:postgres@localhost:5432/postgres"

# helper function in test file that reset the test database
def reset_database(url):
    # connect to the database and reset the database. removing all the data
    sqldatabase = COVID_Database.getInstance(url)
    sql = 'DROP TABLE IF EXISTS covid_daily_data;'
    sqldatabase.engine.execute(sql)
    sql = 'DROP TABLE IF EXISTS covid_area_data;'
    sqldatabase.engine.execute(sql)
    # disconnect
    sqldatabase.disconnect_destroy()


def test_connection():
    sqldatabase = COVID_Database.getInstance(test_db_url)
    assert type(sqldatabase) == COVID_Database
    assert sqldatabase.session.is_active == True
    # disconnect the database, avoid affecting other tests
    sqldatabase.disconnect_destroy()

def test_singleton():
    # call get instance to creat the first instance
    sqldatabase = COVID_Database.getInstance(test_db_url)
    # second instance created should leading to an error
    with pytest.raises(Exception, match=r"The DB connection already exist!"):
        sqldatabase = COVID_Database(test_db_url)
    instance = COVID_Database.getInstance(test_db_url)
    assert sqldatabase is instance
    # disconnect the database, avoid affecting other tests
    sqldatabase.disconnect_destroy()

def test_Upload_Time_series():
    # reset database
    reset_database(test_db_url)
    # now the database is fully cleared. start the test
    sqldatabase = COVID_Database.getInstance(test_db_url)
    # test with the small data included in tests folder, 

    # test for upload Gloanl confirmed Data
    sqldatabase.Load_Time_Series_Global_Confirmed_Data(os.path.join(dirname, 'time_series_covid19_confirmed_global.csv'))
    # test if the data is inserted correctly, the query should have 6 rows returned and data stored correctly
    time1 = datetime.datetime(2020, 6, 18, 0, 0)
    time2 = datetime.datetime(2020, 6, 18, 0, 0)
    data = sqldatabase.query_by_combined_key(time1, time2, 'Alberta,Canada')
    print(data)
    assert data[0] == (None, 7579.0, None, None, datetime.datetime(2020, 6, 18, 0, 0), 'Alberta', 'Canada', None)
    time1 = datetime.datetime(2020, 6, 17, 0, 0)
    time2 = datetime.datetime(2020, 6, 17, 0, 0)
    data = sqldatabase.query_by_combined_key(time1, time2, 'Afghanistan')
    assert data[0] == (None, 26874.0, None, None, datetime.datetime(2020, 6, 17, 0, 0), None, 'Afghanistan', None)
    data = sqldatabase.query_all()
    assert len(data) == 6
    
    # test if the deaths column which is index0 in returned tuple is updated correct
    # no new data should be added
    sqldatabase.Load_Time_Series_Global_Deaths_Data(os.path.join(dirname, 'time_series_covid19_deaths_global.csv'))
    time1 = datetime.datetime(2020, 6, 18, 0, 0)
    time2 = datetime.datetime(2020, 6, 18, 0, 0)
    data = sqldatabase.query_by_combined_key(time1, time2, 'Alberta,Canada')
    assert data[0] == (151.0, 7579.0, None, None, datetime.datetime(2020, 6, 18, 0, 0), 'Alberta', 'Canada', None)
    time1 = datetime.datetime(2020, 6, 17, 0, 0)
    time2 = datetime.datetime(2020, 6, 17, 0, 0)
    data = sqldatabase.query_by_combined_key(time1, time2, 'Afghanistan')
    assert data[0] == (491.0, 26874.0, None, None, datetime.datetime(2020, 6, 17, 0, 0), None, 'Afghanistan', None)
    data = sqldatabase.query_all()
    assert len(data) == 6

    # test if the recovered column is updated correctly after the upload, recovered is index 2 of the returned tuple
    # a new row should be added in the queried result, as ",Canada" indicates a new key
    sqldatabase.Load_Time_Series_Global_Recovered_Data(os.path.join(dirname, 'time_series_covid19_recovered_global.csv'))
    time1 = datetime.datetime(2020, 6, 18, 0, 0)
    time2 = datetime.datetime(2020, 6, 18, 0, 0)
    data = sqldatabase.query_by_combined_key(time1, time2, 'Canada')
    assert data[0] == (151.0, 7579.0, 63782.0, None, datetime.datetime(2020, 6, 18, 0, 0), 'Canada')
    time1 = datetime.datetime(2020, 6, 17, 0, 0)
    time2 = datetime.datetime(2020, 6, 17, 0, 0)
    data = sqldatabase.query_by_combined_key(time1, time2, 'Afghanistan')
    assert data[0] == (491.0, 26874.0, 6158.0, None, datetime.datetime(2020, 6, 17, 0, 0), 'Afghanistan')
    data = sqldatabase.query_all()
    assert len(data) == 7


    # disconnect the database, avoid affecting other tests
    sqldatabase.disconnect_destroy()
    
    
