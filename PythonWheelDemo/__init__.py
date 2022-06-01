from .ingestion import etl

def etl(df, database, table):
   database ="meta" 
   table ="fan"
   connection_string=""
   df.write()
   print(f"Command Executed:")