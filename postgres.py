# Melakukan import postgres connector
import psycopg2

# Melakukan percobaan koneksi
conn = psycopg2.connect(database="karyawan",
                        host="localhost",
                        user="postgres",
                        password="postgres",
                        port="5432")

# Membuat object cursor sebagai penanda
cursor = conn.cursor()

# Deklarasi SQL Query untuk memasukkan record ke db karyawan
insert_sql = """ INSERT INTO data_karyawan (FIRST_NAME, LAST_NAME, AGE, SEX, INCOME)
                VALUES (%s,%s,%s,%s,%s)"""
values = ("Fulky", "Zulkarnaen", 21, "M", 7000000)

try:
    # Eksekusi SQL commands
    cursor.execute(insert_sql, values)
    # Commit ke database
    conn.commit()

except (Exception, psycopg2.DatabaseError) as error:
    conn.rollback()

# Menutup koneksi
conn.close()