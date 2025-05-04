import rdap
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
import streamlit as st

# Deskripsikan data dari database yang akan digunakan
db_config = {
  "host":"sql12.freesqldatabase.com",
  "user":"sql12776603",
  "password":"T7YWGzrxaH",
  "database":"sql12776603"
}

# Menghubungkan python ke database
db = mysql.connector.connect(
  host=db_config['host'],
  user=db_config['user'],
  password=db_config['password'],
  database=db_config['database']
)
cursor = db.cursor()

# Membuat engine mysql
engine = create_engine(f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}")

# Cek table
cursor.execute("SHOW TABLES")
cek_table = cursor.fetchall()
cek_table_list = [item[0] for item in cek_table]

query = "SELECT * FROM datanonduplikat"
cursor.execute(query) 

# Membuat table datadesa menjadi format dataframe
data = cursor.fetchall()
column_names = [desc[0] for desc in cursor.description]
df_dataframe = pd.DataFrame(data, columns=column_names)
df = df_dataframe.copy()
if 'id' in df.columns:
  df.set_index('id', inplace=True)

test = rdap.RdapClient()

st.write("Mulai iterasi")
for idx, domain in df['nama_domain'].items():
    print(f"{idx}/{len(df)}. {domain}")
    try:
        hasil = test.get(domain).data
        df.at[idx, 'expiration_date'] = next((pd.to_datetime(event["eventDate"]).strftime("%Y-%m-%d") for event in hasil["events"] if event["eventAction"] == "expiration"), None)
        df.at[idx, 'nameservers'] = ', '.join([ns["ldhName"] for ns in hasil.get("nameservers", [])])
        df.at[idx, 'status'] = ', '.join([ns for ns in hasil.get("status", [])])
    except Exception as e:
        df.at[idx, 'expiration_date'] = None
        df.at[idx, 'nameservers'] = None
        df.at[idx, 'status'] = None

# Check if table exists and create if needed
if 'infodomain' not in cek_table_list:
  # Create table infodomain
  sql_create_table_infodomain = """CREATE TABLE infodomain (
      id INT AUTO_INCREMENT PRIMARY KEY,
      nama_domain VARCHAR(60),
      expiration_date DATE,
      nameservers VARCHAR(225),
      status VARCHAR(225)
  )"""
  cursor.execute(sql_create_table_infodomain)
  
  # Then insert data using pandas
  df.to_sql(name="infodomain", con=engine, if_exists='append', index=False)
  
# tutup
cursor.close()
db.close()
st.write("Selesai")