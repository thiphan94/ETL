def create_table(cursor):
    """create table in database"""
    try:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS insurance_table (
                gender INTEGER NOT NULL,
                age INTEGER NOT NULL,
                driving_license BOOLEAN NOT NULL,
                region_code INTEGER NOT NULL,
                previously_insured BOOLEAN NOT NULL,
                vehicle_age INTEGER NOT NULL,
                vehicle_damage BOOLEAN NOT NULL,
                annual_premium INTEGER NOT NULL,
                policy_sales_channel INTEGER NOT NULL,
                vintage INTEGER NOT NULL,
                response BOOLEAN NOT NULL
            );
        """
        )
        print("Created table in PostgreSQL", "\n")
    except ValueError as e:
        print(f"Something went wrong when creating the table: {str(e)}", "\n")


def write_postgresql(df):
    """insert values to Postgresql"""
    insurance_seq = [tuple(x) for x in df.collect()]

    records_list_template = ",".join(["%s"] * len(insurance_seq))

    insert_query = f"INSERT INTO insurance_table (gender, age, driving_license, region_code, previously_insured,\
        vehicle_age, vehicle_damage, annual_premium, policy_sales_channel, vintage, response\
        ) VALUES {records_list_template}"

    print("Inserting data into PostgreSQL...", "\n")

    return insert_query, insurance_seq


def get_insterted_data(cursor):
    """query the table"""
    query_select = "select gender, driving_license, previously_insured, annual_premium from insurance_table"

    cursor.execute(query_select)

    cars_records = cursor.fetchmany(2)

    print("Printing 2 rows")
    for row in cars_records:
        print(
            "gender = ",
            row[0],
        )
        print("driving_license = ", row[1])
        print("previously_insured = ", row[2])
        print("annual_premium  = ", row[3], "\n")
