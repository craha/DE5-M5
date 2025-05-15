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

    return df, len(df.query('loan_valid == False'))

def writeToSQL(df, server, database, table):
    connection_string = f'mssql+pyodbc://@{server}/{database}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server'
    engine = create_engine(connection_string)

    try:
        if table == 'engineering':
            df.to_sql(table, con=engine, if_exists='append', index=False)
        else: df.to_sql(table, con=engine, if_exists='replace', index=False)
        print(f"Table{table} written to SQL")
    except Exception as e:
        print(f"Error writing to the SQL Server: {e}")

if __name__ == '__main__':
    customer_path = './Data/customer.csv'
    loan_path = './Data/book.csv'
    date_columns = ['Book checkout', 'Book Returned']
    server = 'localhost'
    database = 'library'
    eng_msgs = []
    load_label = 'Total records loaded'
    dupe_label = 'Duplicate records dropped:'
    na_label = 'Invalid (NA) records dropped'
    date_label = 'Invalid (date) records dropped'
    duration_label = 'Invalid (duration) records dropped'

    # Customer data
    entity = 'customer'
    customers = fileLoader(customer_path)
    num_customers_original = len(customers)
    eng_msgs.append({'Entity': entity, 
                     'Label': load_label, 
                     'Num Records': num_customers_original})

    customers = dupeCleaner(customers)
    num_customers_deduped = len(customers)
    cust_dupe_dropcount = num_customers_original - num_customers_deduped
    eng_msgs.append({'Entity': entity, 
                     'Label': dupe_label, 
                     'Num Records': cust_dupe_dropcount})

    customers = naCleaner(customers)
    num_customers_naCleaned = len(customers)
    cust_na_dropcount = num_customers_deduped - num_customers_naCleaned
    eng_msgs.append({'Entity': entity, 
                     'Label': na_label, 
                     'Num Records': cust_na_dropcount})

    fileSaver(customers,'customers_clean.csv')
    writeToSQL(customers, server, database, table='customer_bronze')

    eng_df = pd.DataFrame(eng_msgs)
    writeToSQL(eng_df, server, database, table='engineering')

    # Loan data
    entity = 'loan'
    eng_msgs = []

    loans = fileLoader(loan_path)
    num_loans_original = len(loans)
    eng_msgs.append({'Entity': entity, 
                     'Label': load_label, 
                     'Num Records': num_loans_original})

    loans = dupeCleaner(loans)
    num_loans_deduped = len(loans)
    loans_dupe_dropcount = num_loans_original - num_loans_deduped 
    eng_msgs.append({'Entity': entity, 
                    'Label': dupe_label, 
                    'Num Records': loans_dupe_dropcount})

    for col in date_columns:
        loans = dateCleaner(loans, col)
    num_loans_dateCleaned = len(loans)
    loans_date_dropcount = num_loans_deduped - num_loans_dateCleaned
    eng_msgs.append({'Entity': entity, 
                    'Label': date_label, 
                    'Num Records': loans_date_dropcount})

    loans = naCleaner(loans)
    num_loans_naCleaned = len(loans)
    loans_na_dropcount = num_loans_dateCleaned - num_loans_naCleaned 
    eng_msgs.append({'Entity': entity, 
                'Label': na_label, 
                'Num Records': loans_na_dropcount})

    loans, loans_valid_dropcount = loanCleaner(loans, 'Book checkout', 'Book Returned')
    eng_msgs.append({'Entity': entity, 
                'Label':duration_label, 
                'Num Records': loans_valid_dropcount})   

    fileSaver(loans, 'loans_clean.csv')
    writeToSQL(loans, server, database, table='loan_bronze')

    eng_df = pd.DataFrame(eng_msgs)
    writeToSQL(eng_df, server, database, table='engineering')