import pandas as pd
import numpy as np
import sqlite3

# Read sqlite query results into a pandas DataFrame
con = sqlite3.connect("~/Development/Datasets/discogs_value.sqlite")
df = pd.read_sql_query("SELECT * from collection_value", con)

# Verify that result of SQL query is stored in the dataframe
print(df.head())

con.close()

