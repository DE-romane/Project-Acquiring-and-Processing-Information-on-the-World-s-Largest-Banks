# Project: Acquiring and Processing Information on the World's Largest Banks

## Overview:
The project is a Python-based ETL (Extract, Transform, Load) pipeline that automates data extraction(web scarping ), transformation, and storage of data related to the largest banks. The project scrapes data from a webpage, transforms the extracted data by adding currency conversion information, and then loads the processed data into both a CSV file and a SQLite database. Let’s go deeper into each part of the code and its functionality.

## Code Information:
- **Code Name:** `banks_project.py`
- **Data URL:** [List of largest banks](https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks)
- **Output CSV Path:** `./Largest_banks_data.csv`
- **Database Name:** `Banks.db`
- **Table Name:** `Largest_banks`
- **Log File:** `code_log.txt`

## Project Tasks:

### **1. Importing Libraries**
```python
import pandas as pd 
from bs4 import BeautifulSoup
import requests 
from datetime import datetime 
import numpy as np
import sqlite3
```
These libraries are critical for various parts of the ETL process:
- **`pandas`**: The backbone for data manipulation. It is used to create DataFrames, read CSV files, and write to CSVs and databases.
- **`BeautifulSoup`**: A web scraping library used to parse the HTML structure of the webpage. It allows us to extract specific content, such as tables, from web pages.
- **`requests`**: Fetches web pages via HTTP requests. This is the first step for scraping the required data.
- **`datetime`**: Provides utilities to handle timestamps, particularly useful for logging progress during execution.
- **`numpy`**: Useful for handling numerical computations, specifically for transforming and rounding data.
- **`sqlite3`**: Enables interaction with a lightweight SQLite database where we store the processed data.

---

### **2. Variable Initialization**
```python
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
exchange_rate_path = 'exchange_rate.csv'
table_attribs = ['Name', 'MC_USD_Billion']
db_name = 'Banks.db'
table_name = 'Largest_banks'
conn = sqlite3.connect(db_name)
query_statements = [
        'SELECT * FROM Largest_banks',
        'SELECT AVG(MC_GBP_Billion) FROM Largest_banks',
        'SELECT Name from Largest_banks LIMIT 5'
    ]
logfile = 'code_log.txt'
output_csv_path = 'Largest_banks_data.csv'
```
This section initializes essential variables used in the code:
- **`url`**: The URL of the Wikipedia page from which the data will be scraped. This URL is an archived version of the page containing the largest banks by market capitalization.
- **`exchange_rate_path`**: The file path to the CSV file containing exchange rates, which will be used to convert market capitalization from USD to other currencies (GBP, INR, EUR).
- **`table_attribs`**: Specifies the columns or attributes we want to extract and store. These are "Name" and "MC_USD_Billion" (market capitalization in USD).
- **`db_name`**: The name of the SQLite database where the data will be stored.
- **`table_name`**: The name of the table that will store the bank data within the SQLite database.
- **`conn`**: Establishes a connection to the SQLite database.
- **`query_statements`**: A list of SQL queries that will be executed to retrieve information from the database.
- **`logfile`**: The path to the log file where progress messages will be recorded.
- **`output_csv_path`**: The path where the final CSV file will be saved.

---

### **3. Logging Function**
```python
def log_progress(message):
    timeformat = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timeformat)
    
    with open(logfile, 'a') as f:
        f.write(timestamp + ' : ' + message + '\n')
```
- **`log_progress`**: This function logs messages to the `logfile` to track the progress of different stages of the ETL process. It:
  - Gets the current timestamp using the `datetime` library.
  - Converts the timestamp to a human-readable format (Year-Month-Day-Hour:Minute:Second).
  - Writes a formatted message with the timestamp into the log file.
  - **Append mode ('a')**: In this mode, new content is added to the end of the file. If the file doesn't already exist, it will be created.
  - sample will be like --> 2024-Sep-22-15:30:45 : Data extraction complete
- Example usage: After each major step (extraction, transformation, loading), the function logs the message to help with debugging or status tracking.

---

### **4. Data Extraction**
```python
def extract(url, table_attribs):
    df = pd.DataFrame(columns=table_attribs)
#Extracts the HTML content from the response and stores it as a string
    page = requests.get(url).text 
#Parses the raw HTML content a tree-like structure for easy extraction of elements
    data = BeautifulSoup(page, 'html.parser')
    tables = data.find_all('tbody')[0]

#Finds all the <tr> (table row) elements within the selected <tbody> element.
#<tr>: Represents a row in an HTML table. 
#Each row contains multiple cells (columns) represented by `<td>` tags.
    rows = tables.find_all('tr')

    for row in rows:
        col = row.find_all('td')
#This checks if the row has any table cells. If `len(col)` is zero, it indicates the row is empty or does not contain data, and it is skipped.
        if len(col) != 0:
#Extracts the second <a> (anchor) tag inside the second <td> (i.e., col[1]).
            ancher_data = col[1].find_all('a')[1]
            if ancher_data is not None:
                data_dict = {
                    'Name': ancher_data.contents[0],
                    'MC_USD_Billion': col[2].contents[0]
                }
#Converts the data_dict (containing the bank's name and market capitalization) into a temporary one-row DataFrame df1
                df1 = pd.DataFrame(data_dict, index=[0])
ُ#Concatenates the temporary DataFrame `df1` with the main DataFrame `df`
                df = pd.concat([df, df1], ignore_index=True)

#Converts the 'MC_USD_Billion' column from the DataFrame into a list (`USD_list`), containing market capitalization values as strings
    USD_list = list(df['MC_USD_Billion'])
    USD_list = [float(''.join(x.split('\n'))) for x in USD_list]
#Updates the `'MC_USD_Billion'` column in the DataFrame with the cleaned and converted market capitalization values (now as floats)
    df['MC_USD_Billion'] = USD_list

#Returns the fully populated and cleaned DataFrame `df`, containing two columns:
#Name: The name of each bank.
#MC_USD_Billion: The market capitalization of each bank, now cleaned and stored as floating-point numbers.
    return df
```
- **`extract`**: This function scrapes the bank data (name and market cap in USD) from the given webpage and returns it as a DataFrame.
    1. **Create Empty DataFrame**: Initializes an empty DataFrame `df` with the column names `table_attribs` (Name, MC_USD_Billion).
    2. **Fetch Webpage**: Uses the `requests.get(url)` method to fetch the HTML content of the specified webpage.
    3. **Parse HTML**: Parses the HTML using BeautifulSoup and selects the first `<tbody>` (table body) tag, which holds the required data.
    4. **Iterate Over Rows**: For each row in the table (`<tr>`):
        - Extracts all `<td>` (table data) elements (columns) and checks if the row has content.
        - From the second column (`col[1]`), it extracts the second `<a>` (anchor) tag and gets the name of the bank.
        - Gets the market capitalization (in USD) from the third column (`col[2]`).
        - Appends this data as a new row to the DataFrame `df`.
    5. **Convert Market Cap to Float**: Market cap values are extracted as strings, so they are cleaned (e.g., removing newline characters) and converted to floating-point numbers.
    6. **Return DataFrame**: The function returns the cleaned DataFrame `df` containing the bank names and their market caps.

---

### **5. Data Transformation**
```python
def transform(df, exchange_rate_path):
    csvfile = pd.read_csv(exchange_rate_path)
    dict = csvfile.set_index('Currency').to_dict()['Rate']

    df['MC_GBP_Billion'] = [np.round(x * dict['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * dict['INR'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * dict['EUR'],2) for x in df['MC_USD_Billion']]

    return df
```
- **`transform`**: This function adds new columns to the DataFrame, converting the market capitalization (USD) into three different currencies: GBP (British Pound), INR (Indian Rupee), and EUR (Euro).
    1. **Read Exchange Rate CSV**: Loads the exchange rate data from the CSV file into a DataFrame `csvfile`.
    2. **Create Exchange Rate Dictionary**: Converts the exchange rate DataFrame into a dictionary, where currency symbols (GBP, INR, EUR) are the keys and their respective rates against USD are the values.
    3. **Add New Columns**:
        - For each bank, multiplies its market cap in USD (`MC_USD_Billion`) by the exchange rate for each currency to get the market cap in GBP, INR, and EUR.
        - Rounds the result to 2 decimal places using `np.round`.
    4. **Return Transformed DataFrame**: Returns the modified DataFrame with the new columns for market caps in different currencies.

---

### **6. Loading Data to CSV**
```python
def load_to_csv(df, output_path):
    df.to_csv(output_path)
```
- **`load_to_csv`**: Saves the DataFrame to a CSV file at the specified path `output_path`. This is useful for creating a flat file for further analysis or sharing.

---

### **7. Loading Data to Database**
```python
def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
```
- **`load_to_db`**: Loads the DataFrame into the SQLite database. The table is named `table_name`, and it replaces any existing table with the same name. The `index=False` argument ensures that DataFrame indices are not written to the database.

---

### **8. Running SQL Queries**
```python
def run_query(query_statement, sql_connection):
    for query in query_statements:
        print(query)
        print(pd.read_sql(query, sql_connection), '\n')
```
- **`run_query`**: This function executes SQL queries on the SQLite database and prints

 the results.
    1. Iterates over the list of predefined SQL queries in `query_statements`.
    2. Uses `pd.read_sql` to execute each query on the database connection and returns the result as a DataFrame.
    3. Prints both the query and the resulting DataFrame for inspection.

---

### **9. Running the ETL Process**
```python
log_progress('Preliminaries complete. Initiating ETL process.')

df = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process.')

df = transform(df, exchange_rate_path)
log_progress('Data transformation complete. Initiating loading process.')

load_to_csv(df, output_csv_path)
log_progress('Data saved to CSV file.')

log_progress('SQL Connection initiated.')

load_to_db(df, conn, table_name)
log_progress('Data loaded to Database as table. Running the query.')

run_query(query_statements, conn)
conn.close()
log_progress('Process Complete.')
```
This block drives the entire ETL process step-by-step:
1. **Log Start of ETL**: Logs a message indicating that the ETL process is starting.
2. **Extract Data**: Calls `extract(url, table_attribs)` to scrape the bank data from the webpage.
3. **Log Completion of Extraction**: Logs the completion of the data extraction process.
4. **Transform Data**: Calls `transform(df, exchange_rate_path)` to convert the market cap data from USD to other currencies.
5. **Log Completion of Transformation**: Logs the completion of the data transformation.
6. **Load to CSV**: Calls `load_to_csv(df, output_csv_path)` to save the transformed data as a CSV file.
7. **Log CSV Loading**: Logs the data save operation.
8. **Load to Database**: Calls `load_to_db(df, conn, table_name)` to load the DataFrame into an SQLite database.
9. **Log Database Loading**: Logs that data has been loaded into the database.
10. **Run SQL Queries**: Calls `run_query(query_statements, conn)` to execute SQL queries and prints the results.
11. **Close Database Connection**: Closes the connection to the SQLite database.
12. **Log Process Completion**: Logs the completion of the ETL process.

---

### **Conclusion**
This project automates the process of extracting bank data from a webpage, transforming it to add currency conversions, and loading it into both a CSV and a SQLite database. It also allows running SQL queries on the stored data, providing a complete end-to-end solution for data collection, transformation, storage, and retrieval. The logging mechanism ensures traceability and helps in debugging or monitoring the process.
