# DataSpark EDA analysis developed by Jagadeesh V
#  Created by Jagadeesh V during first week of Dec. 2024

# Step 1: Understanding the Data
# Step 2: Handling Duplicates
# Step 3: Handling Missing Data
# Step 4: Transforming Data
# Step 5: Cleaning Text Data (Type Conversion) and again to handle missing values
# Step 6: Handling Outliers
# Step 7: Merging Data
 
import mysql.connector
from sqlalchemy import create_engine
import pandas as pd
pd.options.mode.copy_on_write = True

username = 'root'
password = 'Sq!R00t'
host = 'localhost'
port = '3306'  # Default MySQL port
database = 'Dataspark_DB'

# Create the engine
engine = create_engine(f'mysql+pymysql://{username}:{password}@{host}:{port}/{database}')

### -------------------------------------------------- ###

# I. Data Cleaning Functions
## I1. Checking & clearing duplicates in the datasets
def chk_clear_dup(data_frame, file):
    try:
        print(f"Checking and Clearing Duplicates in {file}")
        if  data_frame.duplicated().sum() > 0:
            print(f'Duplicates in {file}  {df_cust.duplicated().sum()}')
            data_frame.drop_duplicates(inplace=True)
            print(f'Duplicates in {file} Cleared \n')
        else:
            print(f'No Duplicates in {file} \n')
    except Exception as e:
        print(f'Error in duplicate check function  for {file} ')

## I2. Missing values and fill them using ffill.  Also bfill if first row has missing value
def chk_clear_null_val(data_frame, file):
    try:
        print(f"Checking and Clearing Null Values in {file}")
        null_col = data_frame.isnull().sum()[data_frame.isnull().sum() > 0]
        if not null_col.empty:
            print(f"{file}  Dataset: Forward filling the missing values of columns: {null_col}")
            data_frame.ffill(inplace=True)
            # Checking for 1st Row missing values to use bfill
            null_cols = data_frame.iloc[0].isnull()
            null_cols_list = null_cols[null_cols].index.tolist()
            if null_cols_list:
                print("Clearing also with backward fill as Null value are in first rows of :\n", null_cols_list)
                data_frame.bfill(inplace=True)
        else:
            print("No Null values found\n")
        ###  Confirming No missing values
        null_col = data_frame.isnull().sum()[data_frame.isnull().sum() > 0]
        print(f"Final Null values result :: Missing values in {file} Dataset:\n", null_col, "\n")
    except Exception as e:
        print(f'Error in Check & clear null values function  for {file} ')

## I3.Converting Data types Dates & Numbers
### I3a. Convert to date time format  [eg. Cust.Birth Day, 
def change_dtype_datetime(data_frame, file):
    try:
        print(f"Converting String types to Date if any in {file}")
        df_temp = pd.DataFrame()
        obj_cols = data_frame.select_dtypes(include='object').columns.tolist()
        print(f"String Cols {obj_cols} in {file}")
        
        for col_name in obj_cols:
            df_temp[col_name] = pd.to_datetime(data_frame[col_name], errors='coerce', format='%m/%d/%Y')
            date_cols = df_temp[col_name].dropna()
            ttl_count = len(df_temp[col_name])
            dt_count = len(date_cols)

            if dt_count >= ttl_count* 0.70:
                print (f"Column {col_name} contains date values and hence type is converted from String to Date")
                data_frame[col_name] = pd.to_datetime(data_frame[col_name], errors='coerce', format='%m/%d/%Y')
                #print (data_frame[col_name])
#        data_frame.reset_index(drop=True, inplace=True)
        print(f"Task completed for {file} \n")
    except Exception as e:
        print(f'Error in Date type Conversion function  for {file}-{col_name} ')

def is_numeric(value):
    return str(value).isdigit()


#### Main Routine calling the functions as required ####

# Calling function & cleaning datasets one by one in a loop
# Using the same dataframe variable to reduce memory use
# Read the Raw input data from csv file

for i in range(4):
    if i == 0:
        df_dataset = pd.read_csv('D:\Guvi\DataSpark Proj\Org Datasets\Customers.csv',encoding='latin')
        print("Cleansing Customer Dataset\n")
        file = "customers"
    elif i == 1:
        df_dataset = pd.read_csv('D:\Guvi\DataSpark Proj\Org Datasets\Exchange_Rates.csv',encoding='latin')
        print("Cleansing Exchange Rate Dataset\n")
        file = "exchange_rate"
    elif i == 2:
        df_dataset = pd.read_csv('D:\Guvi\DataSpark Proj\Org Datasets\Products.csv',encoding='latin')
        print("Cleansing Products Dataset\n")
        file = "products"
    elif i == 3:
        df_dataset = pd.read_csv('D:\Guvi\DataSpark Proj\Org Datasets\Sales.csv',encoding='latin')
        print("Cleansing Sales Dataset\n")
        file = "sales"
    else:
        df_dataset = pd.read_csv('D:\Guvi\DataSpark Proj\Org Datasets\Stores.csv',encoding='latin')
        print(f"Cleansing Stores Dataset\n")
        file = "stores"
        
    print(f"The intial uncleaned data structure")
    print (df_dataset.info())
    chk_clear_null_val(df_dataset, file)
    chk_clear_dup(df_dataset, file)
    change_dtype_datetime(df_dataset, file)
#    df_dataset.reset_index(drop=True, inplace=True)
#  As the df was not getting updated if called thru' function doing it here
    obj_cols = df_dataset.select_dtypes(include='object').columns.tolist()
    print("Converting String types to integer if any in {file}")
    print(f"String Cols {obj_cols} in {file}")
    for col_name in obj_cols:
#        change_dtype_int(df_dataset, col_name, file)
        try:
            df_temp = pd.DataFrame()
            df_temp[col_name] = df_dataset[col_name].apply(is_numeric)
            ttl_count = len(df_temp[col_name])
            int_cols = df_temp[df_temp[col_name]]
            int_count = len(int_cols)
    #        print(f"Column {col_name} Total count: {ttl_count}")
     #       print(f"Int. count: {int_count}  \n")

            if int_count >= ttl_count* 0.70:
                print (f"Column {col_name} contains Integer values and so converting type from String to Int.")
                df_dataset = df_dataset[df_dataset[col_name].str.isnumeric()]
                df_dataset[col_name] = pd.to_numeric(df_dataset[col_name], errors='coerce', downcast='integer')
    #            df_dataset[col_name] = df_dataset[col_name].astype('int32')
                print(f'The datatype changed successfully \n')
        except Exception as e:
            print(f"Error when changing datatype to int for {col_name} is {e}")
    df_dataset.columns = df_dataset.columns.str.replace(' ', '_')
    print(f"Task completed for {file} \n")
    print(f"The cleansed data structure")
    print (df_dataset.info())
    print(f"\n {df_dataset}")
    df_dataset.to_sql(name= file, con=engine, if_exists='replace', index=False)
    print('*** ----------------------------------------------------------------------------------------------- ***\n')


