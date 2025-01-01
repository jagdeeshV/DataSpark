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
import re
import mysql.connector


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
        print(f"Done")
    except Exception as e:
        print(f'Error in Date type Conversion function  for {file}-{col_name} ')

## I4. Checking Zip codes for error and replacing invalids with 9s
def chk_clear_zip(data_frame, file):
    for i in range(len(data_frame)):
        pattern1 = 'NO'
        pattern2 = 'NO'
        zip_code = data_frame.iloc[i]['Zip Code']
        if data_frame.iloc[i]['Country'] == 'Australia':
            data_frame.iloc[i, data_frame.columns.get_loc('Zip Code')] = data_frame.iloc[i, data_frame.columns.get_loc('Zip Code')].zfill(4)
    #        data_frame.iloc[i, data_frame.columns.get_loc('Zip Code')] = '9999'
            pattern = r"^\d{4}$"
        elif data_frame.iloc[i]['Country'] == 'Canada':
            pattern = r"^(?=[^Ddata_framefIiOoQqUu\d\s])[A-Za-z]\d(?=[^Ddata_framefIiOoQqUu\d\s])[A-Za-z]\s{0,1}\d(?=[^Ddata_framefIiOoQqUu\d\s])[A-Za-z]\d$"####
        elif data_frame.iloc[i]['Country'] == 'Germany' :
            data_frame.iloc[i, data_frame.columns.get_loc('Zip Code')] = '99999'
            pattern = r"^\d{5}$"
        elif data_frame.iloc[i]['Country'] == 'France':
            pattern = r"^971\d{2}$"
            data_frame.iloc[i, data_frame.columns.get_loc('Zip Code')] = '99999'
            pattern1 = r"^\d{5}$"
        elif data_frame.iloc[i]['Country'] == 'Italy':
            data_frame.iloc[i, data_frame.columns.get_loc('Zip Code')] = '99999'
            pattern = r"^\d{5}$"
        elif data_frame.iloc[i]['Country'] == 'Netherlands':
            pattern = r"^\d{4}\s{0,1}[A-Za-z]{2}$"
    #    elif data_frame.iloc[i]['Country'] == 'United Kingdom':
    #        pattern = r"^[A-Z]{1,2}[0-9R][0-9A-Z]?\s*[0-9][A-Z-[CIKMOV]]{2}"
        else:
            pattern = "NO"
        zip_code = data_frame.iloc[i]['Zip Code']

        if pattern  != "NO":
            match = re.match(pattern, zip_code)
            if match:
                a = 1 ##  print("Valid zip code")
            elif pattern1 != 'NO':
                match = re.match(pattern1, zip_code)
                if match:
                    a = 1 ##  print("Valid zip code")
                else:
                    match = re.match(pattern2, zip_code)
                    if match:
                        a = 1 ##  print("Valid zip code")
                    else:
                        data_frame.iloc[i, data_frame.columns.get_loc('Zip Code')] = '99999'
#                        print(f"Invalid {data_frame.iloc[i]['Zip Code']}  -  {data_frame.iloc[i]['Country']} Length len({data_frame.iloc[i]['Zip Code']})")
            else:
                data_frame.iloc[i, data_frame.columns.get_loc('Zip Code')] = '99999'
#                print(f"Invalid {data_frame.iloc[i]['Zip Code']}  -  {data_frame.iloc[i]['Country']} Length {len(data_frame.iloc[i]['Zip Code'])}")

def contains_currency_symbol(value):
    currency_symbols = ['$', '€', '¥', '£', '₹']  # Add more symbols as needed
    return any(symbol in value for symbol in currency_symbols)

def is_numeric(value):
    return str(value).isdigit()


#### Main Routine calling the functions as required ####

# Calling function & cleaning datasets one by one in a loop
# Using the same dataframe variable to reduce memory use
# Read the Raw input data from csv file

for i in range(5):
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
#  1
    chk_clear_null_val(df_dataset, file)
#  2
    chk_clear_dup(df_dataset, file)
#  3
    try:
        a = df_dataset.columns.get_loc('Zip Code')
        a = a+ df_dataset.columns.get_loc('Zip_Code')
        b = df_dataset.columns.get_loc('Country')
    except Exception as e:
        a = 0
        b = 0
    if a != 0 and b  != 0:
        print (f"Data set contains Zip Code and clearnsing")
        chk_clear_zip(df_dataset, file)
    else:
        print(f"No Zip code column to clean \n")
        
#  4
    change_dtype_datetime(df_dataset, file)

#   5
    obj_cols = df_dataset.select_dtypes(include='object').columns.tolist()
    for col_name in obj_cols:
        # Count rows with currency symbols
        count = df_dataset[col_name].apply(contains_currency_symbol).sum()
        if count > 0:
            print(f"{col_name} has {count} currency symbols")
            print(df_dataset[col_name])
            df_dataset[col_name] = df_dataset[col_name].replace(r'[\$\€\£\¥\,]', '', regex=True).astype(float)
            print("DataFrame after removing rows with currency symbols:")
            print(df_dataset[col_name])
            
#  7
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

            if int_count >= ttl_count* 0.90:
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
    print(f"\n {df_dataset} \n")
    print (df_dataset.info())

    if i == 0:
        df_customers = df_dataset
    elif i == 1:
        df_exchange = df_dataset
        empty_df = df_dataset.iloc[[0]]
        empty_df = empty_df.loc[[]]
        print (empty_df.info())
        empty_df.to_sql(name= file, con=engine, if_exists='replace', index=False)
        mydb = mysql.connector.connect(host = "localhost", user = "root", password = "Sq!R00t")
        mycursor = mydb.cursor(buffered = True)
        mycursor.execute('use Dataspark_DB')
        for index, row in df_dataset.iterrows():
            mycursor.execute( 
                "INSERT INTO exchange_rate (Date, Currency, Exchange) VALUES (%s, %s, %s) ", \
                 (row['Date'], row['Currency'], row['Exchange']))
            mydb.commit()
    elif i == 2:
        df_products = df_dataset
    elif i == 3:
        df_sales = df_dataset
    else:
        df_stores = df_dataset

    if i != 1:
        df_dataset.to_sql(name= file, con=engine, if_exists='replace', index=False)
    print('*** ----------------------------------------------------------------------------------------------------------------- ***\n')

# MErging Datasets
## 1.Reading all  CSV at a time

##  2. Common columns renaming
df_customers_copy = df_customers
df_stores_copy = df_stores
df_customers_copy.rename(columns={
    'City': 'City_customer',
    'State_Code': 'StateCode_customer',
    'State': 'State_customer',
    'Zip_Code': 'ZipCode_customer',
    'Country': 'Country_customer',
    'Continent': 'Continent_customer'
}, inplace=True)

df_stores_copy.rename(columns={'Country':'Country_store','State':'State_store'},inplace=True)

##  3. Merging Datasets

df_SalesandStoreMerged = pd.merge(df_sales, df_stores_copy, on='StoreKey', how='left')
df_SalesStoresProductsMerged = pd.merge(df_SalesandStoreMerged, df_products, on='ProductKey', how='left')
df_SalesStoresProductsCustomersMerged = pd.merge(df_SalesStoresProductsMerged, df_customers_copy, on='CustomerKey', how='left')
print(df_SalesStoresProductsCustomersMerged.info())
df_finalMerged = pd.merge(df_SalesStoresProductsCustomersMerged, df_exchange, \
                     left_on=['Order_Date','Currency_Code'], \
                     right_on=['Date','Currency'], \
                     how='left')
df_finalMerged.drop(columns=['Date', 'Currency'],inplace=True)
print(df_finalMerged.info())

#Calculating Reveunue Per Customer USD and Profit Per CustomerUSD
df_finalMerged['Revenue_USD'] = df_finalMerged['Quantity'] * df_finalMerged['Unit_Price_USD']
df_finalMerged['Profit_USD'] = df_finalMerged['Revenue_USD']-(df_finalMerged['Quantity'] * df_finalMerged['Unit_Cost_USD'])

#Creating a Frequency Column to the data

# Count the number of orders per customer
order_counts = df_finalMerged.groupby('CustomerKey')['Order_Number'].count()

# function to Categorize frequency
def categorize_frequency(count):
    if count >= 1 and count <= 4:
        return 'Occasional'
    elif count >= 5 and count <= 10:
        return 'Moderate'
    elif count > 10:
        return 'Frequent'
    else:
        return 'Unknown'

# Apply categorization to get a Series of categories
order_counts_category = order_counts.apply(categorize_frequency)

# Merge this frequency information back into the original DataFrame
# Reset index to merge
order_counts_category = order_counts_category.reset_index(name='Frequency')

# Merge with the original DataFrame
df_finalMerged = df_finalMerged.merge(order_counts_category, on='CustomerKey', how='left')

pd.set_option('display.max_columns', None)
df_finalMerged.head(1)

df_finalMerged.to_sql(name= "finalmerged", con=engine, if_exists='replace', index=False)
