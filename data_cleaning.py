# Do all imports and installs here
import psycopg2
import configparser
from datetime import datetime
import os
import logging
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek
from pyspark.sql.functions import to_timestamp, monotonically_increasing_id
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-redshift_2.10:2.0.1 pyspark-shell'
import pandas as pd

conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
cur = conn.cursor()

port_df = pd.read_csv("./airport-codes_csv.csv")

fname = '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
i94_df = pd.read_sas(fname, 'sas7bdat', encoding="ISO-8859-1")

i94_df = i94_df[["cicid","i94yr","i94mon","i94cit","i94res","i94port","arrdate"]]

# Get port locations from SAS text file
with open("./I94_SAS_Labels_Descriptions.SAS") as f:
    content = f.readlines()
content = [x.strip() for x in content]
ports = content[302:962]
splitted_ports = [port.split("=") for port in ports]
port_codes = [x[0].replace("'","").strip() for x in splitted_ports]
port_locations = [x[1].replace("'","").strip() for x in splitted_ports]
port_cities = [x.split(",")[0] for x in port_locations]
port_states = [x.split(",")[-1] for x in port_locations]
df_port_locations = pd.DataFrame({"port_code" : port_codes, "port_city": port_cities, "port_state": port_states})

irregular_ports_df = df_port_locations[df_port_locations["port_city"] == df_port_locations["port_state"]]
irregular_ports = list(set(irregular_ports_df["port_code"].values))



    
def airports_code_null():
    port_df.dropna(subset=['iata_code'], inplace=True)
    print("Deleted null code airport records")
    return    
airports_code_null()

def airports_duplicities():
    clean_port_df = port_df.drop_duplicates(subset='iata_code', keep='first')
    clean_port_df.to_csv("clean_port_df.csv")
    print("Airport records with duplicate code deleted")
    return    
airports_duplicities()

def immigrations_irregular_ports():
    df_i94_filtered = i94_df[~i94_df["i94port"].isin(irregular_ports)]
    df_i94_filtered.to_csv("df_i94_filtered.csv")
    print("Delete immigrations irregular ports records")
    return    
immigrations_irregular_ports()
