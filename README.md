# ETL-Project-World-s-Largest-Banks

In this project, we will perform Extract, Transform, and Load operations on the Top 10 World's Largest Bank data measured by market capitalization in billion USD. We will need to extract tabular information from https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks. This is done by making HTTP requests to fetch the HTML content of the web page using 'requests' library, then use 'BeautifulSoup' library to scrape information from the web page. The extracted data will be stored to a pandas data frame for further processing. Transform the data by adding columns for Market Capitalization in GBP, EUR, and INR. The exchange rates is provided in a different csv file. Load the transformed data frame to an output CSV file locally and SQL database server. We will also run some query on the database table using sqlite and write log functions on the code to log the progress of every operations being performed.

## Workflow:

1. Data Extraction: The extract function uses web scraping techniques to extract relevant information from a Wikipedia page listing the largest banks.

2. Data Transformation: The transform function converts the market capitalization values from USD to GBP, EUR, and INR using exchange rates provided in a CSV file.

3. Data Loading: The transformed data is saved locally as a CSV file using the load_to_csv function. The data is also loaded into a SQLite database table using the load_to_db function.

4. Queries Execution: The run_query function is used to execute SQL queries on the database table.

## Instructions for Use

1. Ensure that the required libraries (BeautifulSoup, pandas, requests, etc.) are installed.
2. Run the code, providing the necessary input parameters such as the Wikipedia URL, CSV file path for exchange rates, database name, and file paths for CSV output and logs.
3. Check the generated CSV file locally and the database table for the processed data.
4. Review the log file ('code_log.txt') for a detailed history of the ETL process.
