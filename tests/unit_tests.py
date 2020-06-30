from COVIDMonitor.COVID_Database import COVID_Database
import pytest


LOCALHOST_URL = "postgres://postgres:postgres@localhost:5432/postgres"

def test_connection():
	sqldatabase = COVID_Database.getInstance()
	assert type(sqldatabase) == COVID_Database
	assert sqldatabase.session.is_active == True
	sqldatabase.disconnect_destroy()

def test_singleton():
    sqldatabase = COVID_Database(LOCALHOST_URL)
	# second instance created should leading to an error
    with pytest.raises(Exception, match=r"The DB connection already exist!"):
        sqldatabase = COVID_Database(LOCALHOST_URL)
    instance = COVID_Database.getInstance()
    assert sqldatabase is instance
