# Code for ETL operations on Country-GDP data

# Importing the required libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("./code_log.txt", "a") as f:
        f.write(timestamp + ' : ' + message + '\n')

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
         col = row.find_all('td')
         if len(col)!=0:
            bank_name = col[1].find_all('a')[1]['title']
            market_cap = col[2].contents[0][:-1]
            data_dict = {"Name": bank_name,
                         "MC_USD_Billion": float(market_cap)}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)
    return df

def transform(df, csv_path):
    exchange_rates = pd.read_csv(csv_path)
    exchange_rate_dict = exchange_rates.set_index('Currency')['Rate'].to_dict()

    df['MC_GBP_Billion'] = [np.round(x*exchange_rate_dict['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate_dict['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate_dict['INR'],2) for x in df['MC_USD_Billion']]

    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, conn, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

# Declaring known values
url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
db_name = 'Banks.db'
table_name = 'Largest_banks'
output_path = './Largest_banks_data.csv'
csv_path = 'exchange_rate.csv'
log_file = 'code_log.txt'
table_attribs = ["Name", "MC_USD_Billion"]
log_progress("Preliminaries complete. Initiating ETL process")

# Call extract() function
df = extract(url, table_attribs)
log_progress("Data extraction complete. Initiating Transformation process")

# Call transform() function
df = transform(df, csv_path)
print(df)
log_progress("Data transformation complete. Initiating Loading process")

# Call load_to_csv()
load_to_csv(df, output_path)
log_progress("Data saved to CSV file")

# Initiate SQLite3 connection
conn = sqlite3.connect(db_name)
log_progress("SQL Connection initiated")

# Call load_to_db()
load_to_db(df, conn, table_name)
log_progress("Data loaded to Database as a table, Executing queries")

# Call run_query()
query_statement1 = f"SELECT * FROM Largest_banks"
run_query(query_statement1, conn)

query_statement2 = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query_statement2, conn)

query_statement3 = f"SELECT Name from Largest_banks LIMIT 5"
run_query(query_statement3, conn)

log_progress("Process Complete")

# Close SQLite3 connection
conn.close()
log_progress("Server Connection closed")