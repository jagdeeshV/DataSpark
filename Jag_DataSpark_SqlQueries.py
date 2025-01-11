# DatSpark illuminating...... developed by Jagadeesh V
#  Created by Jagadeesh V during last week of Dec 2024
from googleapiclient.discovery import build
import mysql.connector
import streamlit as st
import re
from datetime import datetime
import pandas as pd
import keyboard
import time
import os
import psutil

#mydb = mysql.connector.connect(host = "localhost", user = "root", password = "Sq!R00t")
#mycursor.execute('use Dataspark_DB')


# Main Streamlit routine and 10 queries
st.header(":Data Spark Project")

db_host = "localhost"
db_user = "root"
db_password = "Sq!R00t"      

# Part 3 : Creating a streamlit application with the query
query_options = [
    " 1. Country and Gender wise Cusotmer distribution",
    " 2. Country, Agegroup and Gender wise Cusotmer distribution",
    " 3. Average Order Value ( A O V )",
    " 4. Category and Product wise Profit Analysis",
    " 5. Stores by Revenue (Top to bottom)",
    " 6. Country wise Revenue ",
    " 7. Profitability of Products"
    ]
selected_query = st.selectbox("Select a query for result", query_options)

if st.button("Execute"):
    mydb = mysql.connector.connect(host = "localhost", user = "root", password = "Sq!R00t", database = "Dataspark_DB")
    mycursor = mydb.cursor(buffered = True)
    if selected_query == query_options[0]:
        query_result = pd.read_sql_query( "select  distinct a.Country_customer, q1.Males, q2.Females from finalmerged a    left join \
        (select Country_customer, count(distinct name) as Males from finalmerged where gender = 'Male' group by Country_customer) q1 \
                        on a.Country_customer = q1.Country_customer                                           left join \
        (select a.Country_customer, count(distinct name) as Females from finalmerged a where gender = 'Female' group by a.Country_customer) q2 \
                        on a.Country_customer = q2.Country_customer order by a.country_customer", mydb)
    elif selected_query == query_options[1]:
        query_result = pd.read_sql_query("select b.Country_customer, b.age_group, sum(b.Males) male, sum(b.Females) female \
from (select distinct a.Country_customer, \
  case \
      when round(datediff(current_date, a.birthday)/ 365, 0) between  0 and 15 then '00 - 15' \
      when round(datediff(current_date, a.birthday)/ 365, 0) between 16 and 30 then '16 - 30' \
      when round(datediff(current_date, a.birthday)/ 365, 0) between 31 and 45 then '31 - 45' \
      when round(datediff(current_date, a.birthday)/ 365, 0) between 46 and 60 then '46 - 60' \
      when round(datediff(current_date, a.birthday)/ 365, 0) between 61 and 75 then '61 - 75' \
      when round(datediff(current_date, a.birthday)/ 365, 0) between 76 and 90 then '76 - 90' \
      end Age_Group, a.birthday, males, females \
  from finalmerged a \
left join (select distinct a.Country_customer, a.birthday, count(distinct concat(city_customer, name)) as Males from finalmerged a where gender = 'Male' \
        group by a.Country_customer, a.birthday) q1 on a.Country_customer = q1.Country_customer and a.birthday = q1.birthday \
left join (select distinct a.Country_customer, a.birthday, count(distinct concat(city_customer, name)) as Females from finalmerged a where gender = 'Female' \
        group by a.Country_customer, a.birthday) q2 on a.Country_customer = q2.Country_customer and a.birthday = q2.birthday) b \
group by b.Country_customer, b.age_group", mydb) 
    elif selected_query == query_options[2]: 
        query_result =pd.read_sql_query("select Continent_customer, Country_customer, name, sum(Quantity) qty, count(*) No_of_orders, round(sum(Revenue_USD), 2) sales, \
            round(sum(Revenue_USD)/ count(*), 2) AOV from finalmerged a \
group by Continent_customer, Country_customer, name \
order by Continent_customer, Country_customer, name", mydb)
    elif selected_query == query_options[3]:
        query_result = pd.read_sql_query("select Category, product_name, round(sum(Revenue_USD), 2) revenue, round(sum(Revenue_USD)- sum(Profit_USD), 2) Cost,  \
                                    round(sum(Profit_USD), 2) Profit, round(sum(Profit_USD) / (sum(Revenue_USD)- sum(Profit_USD))* 100, 2) profit_percent from finalmerged a \
                                group by Category, product_name", mydb)
    elif selected_query == query_options[4]:
        query_result = pd.read_sql_query("select Country_store, StoreKey, round(sum(Revenue_USD), 2) revenue, round(sum(Revenue_USD)- sum(Profit_USD), 2) Cost, \
                round(sum(Quantity* eXCHANGE), 2) Exchange_gain, round(sum(Profit_USD), 2) Net_Profit, \
                                    round(sum(Profit_USD) / (sum(Revenue_USD)- sum(Profit_USD))* 100, 2) profit_percent from finalmerged a \
                                    group by Country_store, StoreKey order by round(sum(Profit_USD) / (sum(Revenue_USD)- sum(Profit_USD))* 100, 2) desc", mydb)
    elif selected_query == query_options[5]:
        query_result = pd.read_sql_query("select Country_store, StoreKey, round(sum(Revenue_USD), 2) revenue, round(sum(Revenue_USD)- sum(Profit_USD), 2) Cost,  \
                round(sum(Quantity* eXCHANGE), 2) Exchange_gain, round(sum(Profit_USD), 2) Net_Profit, \
        round(sum(Profit_USD) / (sum(Revenue_USD)- sum(Profit_USD))* 100, 2) profit_percent from finalmerged a group by Country_store, StoreKey \
        order by round(sum(Profit_USD) / (sum(Revenue_USD)- sum(Profit_USD))* 100, 2) desc", mydb)
    elif selected_query == query_options[6]:
        query_result = pd.read_sql_query("select Category, product_name, round(sum(Revenue_USD), 2) revenue, round(sum(Revenue_USD)- sum(Profit_USD), 2) Cost, \
            round(sum(Quantity* eXCHANGE), 2) Exchange_gain, round(sum(Profit_USD), 2) Net_Profit, \
                      round(sum(Profit_USD) / (sum(Revenue_USD)- sum(Profit_USD))* 100, 2) profit_percent from finalmerged a group by Category, product_name", mydb)
    mydb.close()
    
    st.dataframe(query_result)

#exit_app = st.sidebar.button("Shut Down")
if st.button ('Exit App'):
    st.markdown ("### Thank You for using the App")
    time.sleep(3)
    # Close streamlit browser tab
    keyboard.press_and_release('ctrl+w')
    # Terminate streamlit python process
    pid = os.getpid()
    p = psutil.Process(pid)
    p.terminate()


