import pandas as pd
from sqlalchemy import create_engine


# Load data
raw_data = pd.read_csv('Data/book.csv', parse_dates=['Book Returned'], date_format='%d/%m/%Y')
book_data = raw_data.copy()
book_data.dropna(inplace=True)

# Clean dates
book_data['Book checkout'] = book_data['Book checkout'].str.replace('"','')
book_data['Book checkout'] = pd.to_datetime(book_data['Book checkout'], errors='coerce', format='%d/%m/%Y')
book_data.dropna(inplace=True)

# Assumptions: checkout should be on or before today and returned should be on or after checkout
book_data = book_data[(book_data['Book checkout'] <= book_data['Book Returned']) & (book_data['Book checkout'] <= pd.Timestamp.today())]

# Clean numbers
book_data['Days allowed to borrow'] = book_data['Days allowed to borrow'].apply(lambda x: 14 if x == '2 weeks' else -1)
book_data = book_data.astype({'Days allowed to borrow': 'int', 
                  'Id': 'int',
                  'Customer ID': 'int'})

# Dedupe 
# Assumption: no one checks out different copies of the same book at the same time
book_data.drop_duplicates(subset=['Books', 'Book checkout', 'Book Returned','Customer ID'], inplace=True)

# Calculate useful values
book_data['Due date'] = book_data['Book checkout'] + pd.to_timedelta(book_data['Days allowed to borrow'], unit='d')
book_data['Days overdue'] = (book_data['Book Returned'] - book_data['Due date']).dt.days
book_data['Days overdue'] = book_data['Days overdue'].apply(lambda x: x if x > 0 else 0)

# Push to SQL Server
engine = create_engine("mssql+pyodbc://lib_user:libuser123@localhost/library?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server", echo=True)
with engine.connect() as connection:
    book_data.to_sql('book',connection, if_exists='replace')