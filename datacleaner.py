import pandas as pd
import datetime as dt
from sqlalchemy import create_engine
import pyodbc

def fileLoader(csvpath):
    raw_data = pd.read_csv(csvpath)
    return raw_data

def fileSaver(df, filename):
    df.to_csv(f'Data/{filename}')

def dupeCleaner(df):
    return df.drop_duplicates().reset_index(drop=True)

def naCleaner(df):
    return df.dropna().reset_index(drop=True)

def dateCleaner(df, col):

    # Clean any quotation marks
    df[col] = df[col].str.replace('"','', regex=True)
    
    try:
        df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')
    except Exception as e:
        print(f"Error while converting {col}: {e}")

    # Identify & remove rows with invalid dates
    invalid_dates = pd.to_datetime(df[col], dayfirst=True, errors='coerce').isna()
    df = df[~invalid_dates].copy() # Remove error rows
    df.reset_index(drop=True, inplace=True) # Reset index for cleaned dataframe

    return df

def loanCleaner(df, loan_from, loan_to):
    df['duration'] = (df[loan_to] - df[loan_from]).dt.days
    df.loc[(df['duration'] < 0) | (df[loan_from] > pd.Timestamp.today()), 'loan_valid'] = False
    df.loc[df['duration'] >= 0, 'loan_valid'] = True

    return df

def writeToSQL(df, server, database, table):
    connection_string = f'mssql+pyodbc://@{server}/{database}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server'
    engine = create_engine(connection_string)

    try:
        df.to_sql(table, con=engine, if_exists='replace', index=False)
        print(f"Table{table} written to SQL")
    except Exception as e:
        print(f"Error writing to the SQL Server: {e}")

if __name__ == '__main__':
    customer_path = 'Data/customer.csv'
    loan_path = 'Data/book.csv'
    date_columns = ['Book checkout', 'Book Returned']
    server = 'localhost'
    database = 'library'

    # Customer data
    customers = fileLoader(customer_path)
    customers = dupeCleaner(customers)
    customers = naCleaner(customers)
    fileSaver(customers,'customers_clean.csv')
    writeToSQL(customers, server, database, table='customer_bronze')

    # Loan data
    loans = fileLoader(loan_path)
    loans = dupeCleaner(loans)
    for col in date_columns:
        loans = dateCleaner(loans, col)
    loans = naCleaner(loans)
    loans = loanCleaner(loans, 'Book checkout', 'Book Returned')
    fileSaver(loans, 'loans_clean.csv')
    writeToSQL(loans, server, database, table='loan_bronze')