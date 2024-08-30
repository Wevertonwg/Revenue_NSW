import unittest
import pandas as pd
from io import StringIO
from main import read_data, transform_data, load_data, Person

class TestMain(unittest.TestCase):
    def setUp(self):
        #Sample data for testing
        self.csv_data = """
        Weverton|Goulart|Weverton WG LTD|20041985|49999|123 Heaven St|Rockdale|NSW|2216|2404040404|0404040404|weverton.wg@wevertonwg.com
        Maria Isabel|de Faria Goulart|Isabel Faria LTD|0101/1985|50000|123 Heaven St|Rockdale|NSW|2216|2404040405|0404040405|isabelfaria@isabelfaria.com
        Leticia|Faria Goulart|Leles World|01012017|100001|123 Heaven St|Rockdale|NSW|2216|2404040406|0404040406|lele@leleworld.com
        """
        ##Maria Isabel|de Faria Goulart|Isabel Faria LTD|0101/1985|50000|123 Heaven St|Rockdale|NSW|2216|2404040405|0404040405|isabelfaria@isabelfaria.com
        #Leticia|Faria Goulart|Leles World|01012017|100001|123 Heaven St|Rockdale|NSW|2216|2404040406|0404040406|lele@leleworld.com
        self.file_path = 'test_data.csv'
        self.output_file_path = 'test_transformed_data.json'

        with open(self.file_path, 'w') as f:
            f.write(self.csv_data)

    def test_read_data(self):
        df = read_data(self.file_path)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.shape[0], 3)  #Rows expectation in the sample data

    def test_transform_data(self):
        df = read_data(self.file_path)
        df = transform_data(df)
        self.assertEqual(df['SalaryBucket'].iloc[0], 'A')  #Weverton should be in bucket 'A'
        self.assertEqual(df['SalaryBucket'].iloc[1], 'B')  #Maria should be in bucket 'B'
        self.assertEqual(df['SalaryBucket'].iloc[2], 'C')  #Leticia should be in bucket 'C'
        self.assertEqual(df['FullName'].iloc[0], 'Weverton Goulart')
        self.assertEqual(df['FullName'].iloc[1], 'Maria Isabel de Faria Goulart')
        self.assertEqual(df['FullName'].iloc[2], 'Leticia Faria Goulart')

    def test_load_data(self):
        df = read_data(self.file_path)
        df = transform_data(df)
        load_data(df, self.output_file_path)
        #checking if file was created
        with open(self.output_file_path, 'r') as f:
            content = f.read()
            self.assertTrue(content.startswith('{'))  #Ensure the file contains JSON content

    def tearDown(self):
        import os
        os.remove(self.file_path)
        if os.path.exists(self.output_file_path):
            os.remove(self.output_file_path)

if __name__ == '__main__':
    unittest.main()