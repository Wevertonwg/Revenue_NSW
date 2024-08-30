import pandas as pd
import re
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import os

#Developer: Weverton Goulart - 30/08/2024

#location of files
file_path = 'C:\\Users\\user\\Downloads\\member-data.csv'
output_file_path = 'C:\\Users\\user\\Downloads\\transformed_data.json'

# Define the DAG
dag = DAG(
    'req32647_dag',
    default_args={'start_date': days_ago(1)},
    schedule_interval='0 22 * * *',
    catchup=False
)

#Defining the Person and Address classes
class Person:
    def __init__(self, first_name, last_name, company, birth_date, salary, address, phone, mobile, email):
        self.first_name = first_name
        self.last_name = last_name
        self.company = company
        self.birth_date = birth_date
        self.salary = salary
        self.address = address
        self.phone = phone
        self.mobile = mobile
        self.email = email

    def __str__(self):
        return f"{self.first_name} {self.last_name}, {self.company}, {self.birth_date}, {self.salary}, {self.address}, {self.phone}, {self.mobile}, {self.email}"

    #Address as nested class
    class Address:
        def __init__(self, address, suburb, state, post):
            self.address = address
            self.suburb = suburb
            self.state = state
            self.post = post

        def __str__(self):
            return f"{self.address}, {self.suburb}, {self.state}, {self.post}"

#function checking and cleaning currency values
def clean_currency(value):
    try:
        if pd.isna(value) or value == '':
            return 0
        value = re.sub(r'[^\d.]', '', value)
        return float(value) if value else 0
    except Exception as e:
        print(f"Error cleaning currency value: {value}. Exception: {e}")
        return 0

#Method to read data from CSV file
def read_data(file_path):
    try:
        headers = ['FirstName', 'LastName', 'Company', 'BirthDate', 'Salary', 'Address', 'Suburb', 'State', 'Post', 'Phone', 'Mobile', 'Email']
        dtypes = {  'FirstName': 'string',
                    'LastName' : 'string',
                    'Company'  : 'string',
                    'BirthDate': 'string',
                    'Salary'   : 'float64',
                    'Address'  : 'string',
                    'Suburb'   : 'string',
                    'State'    : 'string',
                    'Post'     : 'int64',
                    'Phone'    : 'int64',
                    'Mobile'   : 'int64',
                    'Email'    : 'string'}
        parse_dates = ['BirthDate']

        data = pd.read_csv(file_path, delimiter='|', header=None, names=headers, dtype=dtypes)


        return data
    
    except FileNotFoundError:
        print(f"Error: The file at {file_path} was not found.")
    except pd.errors.EmptyDataError:
        print("Error: The file is empty.")
    except pd.errors.ParserError:
        print("Error: There was a problem parsing the file.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

#Method to transform data
def transform_data(df):
    try:

        def format_date(dob):
            if pd.isna(dob) or dob == '':
                return ''
            dob = dob.zfill(8)  #Adding zero if the length is 7
            day = dob[:2]
            month = dob[2:4]
            year = dob[4:]
            return f"{day}-{month}-{year}"

        df['BirthDate'] = df['BirthDate'].apply(format_date)

        #Converting Salary to string, then cleaning and converting to numeric
        df['Salary'] = df['Salary'].astype(str).apply(clean_currency)

        #Ensure 'Salary' is numeric and check its type
        df['Salary'] = pd.to_numeric(df['Salary'], errors='coerce')
        #print(f"Data type of Salary after conversion: {df['Salary'].dtype}")

        #SalaryBucket column based on range of values
        def categorize_salary(salary):
            if pd.isna(salary):
                return ''
            if salary < 50000:
                return 'A'
            elif 50000 <= salary <= 100000:
                return 'B'
            else:
                return 'C'

        df['SalaryBucket'] = df['Salary'].apply(categorize_salary)

        #Formating Salary as currency
        df['Salary'] = df['Salary'].apply(lambda x: '${:,.2f}'.format(x) if pd.notnull(x) else None)

        #removing standard spaces, tabs, and new lines
        df['FirstName'] = df['FirstName'].str.strip().str.replace(r'\t|\n|\r', '', regex=True)
        df['LastName'] = df['LastName'].str.strip().str.replace(r'\t|\n|\r', '', regex=True)

        #New FullName column
        df['FullName'] = df['FirstName'] + ' ' + df['LastName']

        #creating Address objects
        df['AddressObject'] = df.apply(lambda row: Person.Address(
            address=row['Address'],
            suburb=row['Suburb'],
            state=row['State'],
            post=row['Post']
        ), axis=1)

        #Creating Person objects
        people = df.apply(lambda row: Person(
            first_name=row['FirstName'],
            last_name=row['LastName'],
            company=row['Company'],
            birth_date=row['BirthDate'],
            salary=row['Salary'],
            address=row['AddressObject'],
            phone=row['Phone'],
            mobile=row['Mobile'],
            email=row['Email']
        ), axis=1)

        #Dropping columns
        df.drop(['FirstName', 'LastName'], axis=1, inplace=True)

        #Changing AddressObject column with address formatted as string
        df['Address'] = df['AddressObject'].apply(str)
        df.drop('AddressObject', axis=1, inplace=True)

        #Reordering columns
        cols = df.columns.tolist()
        cols.insert(0, cols.pop(cols.index('FullName')))
        cols.insert(4, cols.pop(cols.index('SalaryBucket')))
        df = df[cols]

        return df

    except Exception as e:
        print(f"An error occurred during transformation: {e}")
        raise

#Method to load data to JSON file
def load_data(df, output_file_path):
    try:
        df.to_json(output_file_path, orient='records', lines=True)

        print(f"Data successfully saved to {output_file_path}")
    except Exception as e:
        print(f"An error occurred while saving data: {e}")


#calling methods
if __name__ == "__main__":
    file_path = 'C:\\Users\\user\\Downloads\\member-data.csv'
    output_file_path = 'C:\\Users\\user\\Downloads\\transformed_data.json'
    
    df = read_data(file_path)
    if df is not None:
        try:
            df = transform_data(df)
            print(df)
            load_data(df, output_file_path)
        except Exception as e:
            print(f"An error occurred: {e}")