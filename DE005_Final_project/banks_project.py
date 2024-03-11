
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np 
import sqlite3 
from datetime import datetime 


url="https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks"
log_file="code_log.txt"
table_attribs=["Name", "MC_USD_Billion"]
csv_path="D:/DataEngineer/Python_Project_For_DE/DE005_Final_project/exchange_rate.csv"
output_path="Largest_banks_data.csv"
table_name="Largest_banks"
# Code for ETL operations on Country-GDP data

# Importing the required libraries

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' 
    now=datetime.now()
    timestamp=now.strftime(timestamp_format)
    with open(log_file,"a") as f:
        f.write(timestamp+" : "+message)

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''

    page= requests.get(url).text
    data= BeautifulSoup(page, "html.parser")
    df=pd.DataFrame(columns=table_attribs)
    tables= data.find_all('tbody')
    rows=tables[0].find_all('tr')
    col=rows[1].find_all("td")
    for i in rows[1:]:
        col=i.find_all('td')
        
        data_Dict={"Name": col[1].find_all("a")[1].contents,
                   "MC_USD_Billion": float(col[2].contents[0].replace('\n',''))}
        df1= pd.DataFrame(data_Dict, index=[0])
        df=pd.concat([df, df1],ignore_index=True)
    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    exchange_rate=pd.read_csv(csv_path)
    dict=exchange_rate.to_dict()
    MC_GBP_Billion=[np.round(x*exchange_rate["Rate"].iloc[1],2) for x in df['MC_USD_Billion']]
    MC_EUR_Billion=[np.round(x*exchange_rate["Rate"].iloc[0],2) for x in df['MC_USD_Billion']]
    MC_INR_Billion=[np.round(x*exchange_rate["Rate"].iloc[2],2) for x in df['MC_USD_Billion']]
    df.insert(2, "MC_GBP_Billion",MC_GBP_Billion)
    df.insert(3, "MC_EUR_Billion",MC_EUR_Billion)
    df.insert(4, "MC_INR_Billion",MC_INR_Billion)
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print('__'*10)
    print(query_statement)
    query_out= pd.read_sql(query_statement,sql_connection)
    print(query_out)
    print("__"*10)

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

log_progress('Preliminaries complete. Initiating ETL process \n')

df=extract(url,table_attribs)

log_progress('Data extraction complete. Initiating Transformation process \n')

df=transform(df,csv_path)
print(df)

log_progress('Data transformation complete. Initiating loading process \n')

load_to_csv(df,output_path)
log_progress('Data saved to CSV file \n')

sql_connection = sqlite3.connect('Banks.db')
log_progress('SQL Connection initiated. \n')

load_to_db(df,sql_connection, table_name)
log_progress('Data loaded to Database as table. Running the query \n')

query_statement = f"SELECT * FROM Largest_banks"
log_progress('Process Complete. \n')

run_query(query_statement,sql_connection)
sql_connection.close()