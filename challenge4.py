# Melakukan import postgres connector dan pandas
import psycopg2
import pandas as pd

# Melakukan percobaan koneksi
conn = psycopg2.connect(database="kalbe",
                        host="localhost",
                        user="postgres",
                        password="postgres",
                        port="5432")

# Membuat object cursor sebagai penanda
cursor = conn.cursor()

# Membaca data file excel dengan pandas
df = pd.read_excel('./hdfs/data/data1/daily_market_price.xlsx', 'No 5')


# Deklarasi SQL Query untuk memasukkan record ke db karyawan
insert_sql = """ INSERT INTO customer_orders (order_no, purchase_amount, order_date, customer_id, salesman_id)
                VALUES (%s, %s, %s, %s, %s)"""
values = list(df.itertuples(index=False, name=None))

try:
    # Eksekusi SQL commands
    cursor.executemany(insert_sql, values)
    # Commit ke database
    conn.commit()

except (Exception, psycopg2.DatabaseError) as error:
    print(error)
    conn.rollback()

# Menutup koneksi
conn.close()