import argparse, psycopg2, sys, csv, os
from psycopg2.extras import execute_values

def get_data_type(column):
    if column.isdigit():
        return "INT"
    if len(column) == 10:
        
        if(column[4] == column[7] == '-' and
            column[:4].isdigit() and
            column[5:7].isdigit() and
            column[8:].isdigit()):
            return "DATE"
    return "TEXT"
        
    

def find_primary_key(columns,table_name):
    cols=[]
    for column in columns:
        if(column.rstrip('_id')==table_name):
            return [column]
         
        if column.endswith("_id"):
            cols.append(column)
    return cols

def generate_primary_keys(primary_keys):
    primary_key_statement = "  PRIMARY KEY (" + ",".join(primary_keys) + ")"
    return primary_key_statement

def find_foreign_keys(columns,table_names,table_name):
    foreign_keys = []
    foreign_table=[]
    for column in columns:
        if column.endswith("_id"):
            if column.rstrip('_id')!=table_name:
                if column.rstrip("_id") in table_names:
                    foreign_keys.append(column)
                    foreign_table.append(column.rstrip('_id'))
        if column.endswith("_key"):
            l_index = column.rfind('_')
            l_index2 = column.rfind('_', 0 ,l_index)
            s=column[l_index2 + 1:l_index]
            
            if s in table_names:
                foreign_keys.append(column)
                foreign_table.append(s)
        
    return foreign_keys,foreign_table

def generate_foreign_keys(foreign_keys,foreign_table):
    listw=[]
    for column, referenced_table in zip(foreign_keys, foreign_table):
        listw.append(f"  FOREIGN KEY ({column}) REFERENCES {referenced_table} on DELETE set null")

    return ',\n'.join(listw)

def generate_columns(columns,columns1):
    ddl_columns = []
    for i in range(len(columns)):
        ddl_columns.append(f"  {columns[i]} {get_data_type(columns1[i])},")
    # for column in columns:
    #     ddl_columns.append(f"  {column} {get_data_type(column)},")
    return '\n'.join(ddl_columns)
def generate_ddl(csv_dir, output_path=None):
    my_dictionary={}
    table_dependencies = {}
    ddl_statements = []
    names_tables=[]
    for csv_file in os.listdir(csv_dir):
        if csv_file.endswith(".csv"):
            table_name = os.path.splitext(csv_file)[0]
            # ddl_statements.append(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
            names_tables.append(table_name)
    for csv_file in os.listdir(csv_dir):
        if csv_file.endswith(".csv"):
            table_name = os.path.splitext(csv_file)[0]
            with open(os.path.join(csv_dir, csv_file), 'r') as file:
                csv_reader = csv.reader(file)
                columns = next(csv_reader)
                columns1 = next(csv_reader)
                primary_key = find_primary_key(columns,table_name)
                foreign_keys,foreign_table = find_foreign_keys(columns,names_tables,table_name)
                table_dependencies[table_name]=foreign_table
                ddl_statement = f"CREATE TABLE {table_name} (\n"
                ddl_statement += generate_columns(columns,columns1)
                ddl_statement += "\n"
                ddl_statement += generate_primary_keys(primary_key)
                if(len(foreign_keys)==0): 
                    ddl_statement += "\n);\n"
                    my_dictionary[table_name]=ddl_statement
                    continue
                else:
                    ddl_statement += ",\n"
                ddl_statement += generate_foreign_keys(foreign_keys,foreign_table)
                ddl_statement += "\n);\n"
                # ddl_statements.append(ddl_statement)
                my_dictionary[table_name]=ddl_statement


    
    # print(table_dependencies)
    table_dictionary = {table: list(set(dependencies)) for table, dependencies in table_dependencies.items()}
    
    
    order=[]
    processed_tables = set()
    for current_table in names_tables:
        if all(dependency in processed_tables for dependency in table_dictionary[current_table]):
            # print(f"{current_table} ")
            order.append(current_table)
            processed_tables.add(current_table)
        else:
            names_tables.append(current_table)
    
    for remaining_table in names_tables:
        if remaining_table not in processed_tables:
            # print(f"{remaining_table} ")
            order.append(remaining_table)
            processed_tables.add(remaining_table)
    # for x in ddl_statements:
    #     print(x)
    # print(order)
    for x in order:
        ddl_statements.append(f"DROP TABLE IF EXISTS {x} CASCADE;")
    for x in order:
        ddl_statements.append(my_dictionary[x])
    output_content = '\n'.join(ddl_statements)
    if output_path:
        with open(output_path, 'w') as output_file:
            output_file.write(output_content)
    else:
        print(output_content)



def export_table_data(connection, table_name, format, output_path):
    try:
        cursor = connection.cursor()

        # Fetch data from the table
        cursor.execute(f"SELECT * FROM {table_name};")
        rows = cursor.fetchall()

        if format == "csv":
            # Export to CSV format
            if output_path:
                with open(output_path, 'w', newline='') as csv_file:
                    csv_writer = csv.writer(csv_file)

                    # Write header
                    csv_writer.writerow([desc[0] for desc in cursor.description])

                    # Write data
                    csv_writer.writerows(rows)
            else:
                print(','.join([desc[0] for desc in cursor.description]))
                for row in rows:
                    # Format values as SQL literals
                    values = ','.join([f"{str(value)}" for value in row])
                    print(values)
            
        elif format == "sql":
            # Export to SQL format
            sql_file_content = ""
            columnlist=[desc[0] for desc in cursor.description]
            for row in rows:
                # Format values as SQL literals
                values = ', '.join([f"'{str(value)}'" if value is not None else 'NULL' for value in row])
                sql_file_content += f"INSERT INTO {table_name} ({', '.join(columnlist)}) values ({values});\n"

            if output_path:
                with open(output_path, 'w') as sql_file:
                    sql_file.write(sql_file_content)
            else:
                print(sql_file_content)

            
        else:
            print(f"Unsupported format: {format}")
    except Exception as e:
        print(f"Error during export_table_data: {e}")
    finally:
        # Close the cursor
        if 'cursor' in locals() and cursor is not None:
            cursor.close()



        
  
 

def main(args):
    connection = psycopg2.connect(host = args.host, port = args.port, database = args.name, user = args.user, password = args.pswd)
    cursor = connection.cursor()

    if(args.export_ddl):
        csv_dir = args.csv_dir
        output_path = args.output_path
        generate_ddl(csv_dir, output_path)
    
    if(args.import_table_data):
        csv_filepath = args.path
        
        with open(csv_filepath, 'r') as csv_file:

            csv_reader = csv.reader(csv_file)
            header = next(csv_reader) 
            csvfilename = os.path.basename(csv_filepath)
            table_name= os.path.splitext(csvfilename)[0]
            insert_query = f"INSERT INTO {table_name} ({','.join(header)}) VALUES %s"
            execute_values(
                cursor,
                insert_query,
                csv_reader
            )
            connection.commit()
        
    if(args.export_table_data):
        format = args.format
        table = args.table
        output_path = args.output_path
        export_table_data(connection, table, format, output_path)

    if(args.testing):
        cursor.execute("DROP TABLE IF EXISTS test;")
        cursor.execute("CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);")
        cursor.execute("INSERT INTO test (num, data) VALUES (%s, %s)", (100, "abc'def"))
        cursor.execute("INSERT INTO test (num, data) VALUES (%s, %s)", (200, "abc'def"))
        cursor.execute("INSERT INTO test (num, data) VALUES (%s, %s)", (100, "abc'def"))
        
        cursor.execute("SELECT * FROM test;")
        row = cursor.fetchone()
        while row != None:
            print(row)
            row = cursor.fetchone()
        
        cursor.execute("SELECT * FROM test where num = 100;")
        print(cursor.fetchall())

        cursor.execute("SELECT * FROM test;")
        print(cursor.fetchmany(3))

    if connection:
        cursor.close()
        connection.close()        

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--name")
    parser.add_argument("--user")
    parser.add_argument("--pswd")
    parser.add_argument("--host")
    parser.add_argument("--port")
    parser.add_argument("--export-ddl", action='store_true')
    parser.add_argument("--import-table-data", action='store_true')
    parser.add_argument("--export-table-data", action='store_true')
    parser.add_argument("--csv_dir")
    parser.add_argument("--output_path")
    parser.add_argument("--table")
    parser.add_argument("--path")
    parser.add_argument("--format")
    parser.add_argument("--testing", action = 'store_true')

    args = parser.parse_args()
    main(args)
