import psycopg2
import schedule
import time

# Connect to the database
conn = psycopg2.connect(host = '127.0.0.1' , database ='postgres' ,
        port = '5432', user = 'postgres' , password = 'database')
        
# Create a cursor
cur = conn.cursor()

def partition():

    try:
        #dropping table if exists in db
        cur.execute("DROP TABLE IF EXISTS sales_units")
        partition_table = 'create table sales_units PARTITION of sales_details FOR VALUES from (1) to (25) '
        cur.execute(partition_table)
        print('partition table created')
    except Exception as error:
        print(error)
    #commiting all transactions made
    conn.commit() 
    # Close the cursor
    cur.close()

#scheduling every day 
schedule.every().day.at("18:00").do(partition)

while True:           
    # Run all jobs that are scheduled to run                              
     schedule.run_pending()                
     # delay for 1 sec 
     time.sleep(1)                      
