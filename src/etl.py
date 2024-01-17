import psycopg2
from constant import initialize_Spark, INSURANCE_COLS
from extract import loadDFWithSchema
from transform import rename_cols, clean_data
from load import create_table, write_postgresql, get_insterted_data

conn = psycopg2.connect(
    host="localhost", database="insurance", user="postgres", password="postgres"
)

print("Connection to PostgreSQL created", "\n")


cur = conn.cursor()

spark = initialize_Spark()


# Extract
df = loadDFWithSchema(spark)

# Transformation
df_renamed = rename_cols(df, INSURANCE_COLS)
df_final = clean_data(df_renamed)

# Load data
create_table(cur)

insert_query, insurance_seq = write_postgresql(df_final)
cur.execute(insert_query, insurance_seq)

print("Data inserted into PostgreSQL", "\n")

get_insterted_data(cur)

cur.close()

print("Commiting changes to database", "\n")
# make sure that your changes are shown in the db
conn.commit()

print("Closing connection", "\n")

# close the connection
conn.close()

print("Done!", "\n")
