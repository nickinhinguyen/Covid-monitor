import unittest
import sys
sys.path.insert(1, '../COVIDMonitor')

from main_driver import CheckValidFile

class Test_is_valied_file(unittest.TestCase):
    def test_file_not_end_with_csv(self):
        result = CheckValidFile.is_valid_file("file.txt")
        self.assertEqual(result,None)
        
    def test_file_not_exist(self):
        result = CheckValidFile.is_valid_file("non_existed_file.csv")
        self.assertEqual(result,None)
    def test_valid_file(self):
        result = CheckValidFile.is_valid_file("../tests/staticTestFile/06-16-2020.csv")
        self.assertEqual(result,"../tests/staticTestFile/06-16-2020.csv")

class Test_is_is_csv_file(unittest.TestCase):

    def test_valid_file_type1(self):
        result = CheckValidFile.is_csv_file('-dr')
        self.assertEqual(result, True)
        
    def test_valid_file_type2(self):
        result = CheckValidFile.is_csv_file('-tsr')
        self.assertEqual(result, True)

    def test_empty_file_type(self):
        result = CheckValidFile.is_csv_file('')
        self.assertEqual(result, False) 
    def test_invalid_file_type(self):

        result = CheckValidFile.is_csv_file('dr')
        self.assertEqual(result, False)  


unittest.main()