# Assumptions: checkout should be on or before today and returned should be on or after checkout
loan_data = loan_data[(loan_data['Book checkout'] <= loan_data['Book Returned']) & (loan_data['Book checkout'] <= pd.Timestamp.today())]

# Clean numbers: perhaps shouldn't change the Id or Customer ID!
loan_data['Days allowed to borrow'] = loan_data['Days allowed to borrow'].apply(lambda x: 14 if x == '2 weeks' else -1)
loan_data = loan_data.astype({'Days allowed to borrow': 'int', 
                  'Id': 'int',
                  'Customer ID': 'int'})

# Dedupe: raise with stakeholders instead!
# Assumption: no one checks out different copies of the same book at the same time
loan_data.drop_duplicates(subset=['Books', 'Book checkout', 'Book Returned','Customer ID'], inplace=True)

# Calculate useful values: perhaps not necessary to calculate Due Date & Days Overdue
loan_data['Loan length'] = (loan_data['Book Returned'] - loan_data['Book checkout']).dt.days
loan_data['Due date'] = loan_data['Book checkout'] + pd.to_timedelta(loan_data['Days allowed to borrow'], unit='d')
loan_data['Days overdue'] = (loan_data['Book Returned'] - loan_data['Due date']).dt.days
loan_data['Days overdue'] = loan_data['Days overdue'].apply(lambda x: x if x > 0 else 0)

# Save to csv file
loan_data.to_csv('Data/loans_clean.csv')
customer_data.to_csv('Data/customers_clean.csv')

# Push to SQL Server
engine = create_engine("mssql+pyodbc://lib_user:libuser123@localhost/library?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server", echo=True)
with engine.connect() as connection:
    loan_data.to_sql('loan',connection, if_exists='replace')
    customer_data.to_sql('customer',connection,if_exists='replace')