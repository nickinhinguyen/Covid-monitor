import unittest
import sys
import datetime 
sys.path.insert(1, '../COVIDMonitor')

from Display import Display

SAMPLE_QUERY_OUTPUT = [[(621.0, 21533.0, 0.0, 212.0, datetime.datetime(2020, 6, 18, 0, 0), 'South Carolina')],[(6211.0, 21533.0, 0.0, 2912.0, datetime.datetime(2020, 6, 18, 0, 0), 'China')],[(61.0, 21533.0, 0.0, 20912.0, datetime.datetime(2020, 6, 18, 0, 0), 'Vietnam')]]

class TestDisplay(unittest.TestCase):
    instance = Display(SAMPLE_QUERY_OUTPUT)
    
    def test_get_data_key(self):
        result = Display.getInstance().get_data_key()
        self.assertEqual(result,['South Carolina','China','Vietnam'])


#     def test_export_JSON(self):
#         result = Display.getInstance().export_JSON(self)

unittest.main() 