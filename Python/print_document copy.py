import xml.etree.ElementTree as ET
import pandas as pd
import pyodbc

# Function to flatten the XML
def partial_flatten(element, prefix=""):
    data = {}
    for child in element:
        tag = child.tag.split('}')[-1]  # Remove namespace
        key = prefix + tag if prefix else tag
        if child.text and child.text.strip():
            data[key] = child.text.strip()
        else:
            data.update(partial_flatten(child, key + '/'))
        for attr_key, attr_value in child.attrib.items():
            data[f"{key}_attr_{attr_key}"] = attr_value  # Use a more readable format for attributes
    return data

# Path to the XML file
xml_file_path = 'G:\\samanth473_drive\\CDP\\CDA-phcaserpt-1.3.0-CDA-phcaserpt-1.3.1\\examples\\samples\\CDAR2_IG_PHCASERPT_R2_STU3.1_SAMPLE.xml'

# Parse the XML file
try:
    tree = ET.parse(xml_file_path)
    root = tree.getroot()
except FileNotFoundError:
    print(f"Error: File '{xml_file_path}' not found.")
    exit()
except ET.ParseError as e:
    print(f"Error: Failed to parse XML file. {str(e)}")
    exit()

# Extract and flatten data for each section
sections = {
    "document": partial_flatten(root),
    "patient": partial_flatten(root.find('.//cda:patientRole', {'cda': 'urn:hl7-org:v3'})),
    "guardian": partial_flatten(root.find('.//cda:guardian', {'cda': 'urn:hl7-org:v3'})),
    "author": partial_flatten(root.find('.//cda:author', {'cda': 'urn:hl7-org:v3'})),
    "custodian": partial_flatten(root.find('.//cda:custodian', {'cda': 'urn:hl7-org:v3'})),
    "encounter": partial_flatten(root.find('.//cda:componentOf', {'cda': 'urn:hl7-org:v3'})),
    "component": partial_flatten(root.find('.//cda:component', {'cda': 'urn:hl7-org:v3'})),
}

# Function to generate concise and readable column names from paths
def generate_column_name(path):
    parts = path.split('/')
    # Only keep the last part of the path and attributes
    last_parts = [part.split(':')[-1] for part in parts[-2:]]
    column_name = '_'.join(last_parts)
    # Remove 'attr_' prefix for attributes and normalize
    column_name = column_name.replace('attr_', '').replace('{', '').replace('}', '').replace('-', '').replace('_', '').title()
    return column_name

# Generate DataFrames with simplified and mapped column names
dataframes = {}
for section, data in sections.items():
    simplified_data = {generate_column_name(key): value for key, value in data.items()}
    dataframes[section] = pd.DataFrame([simplified_data])

# Display DataFrames
for section, df in dataframes.items():
    print(f"\n{section.capitalize()} Information DataFrame:")
    print(df)

# SQL Server connection using Windows Authentication
conn_str = (
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=Samanth;'
    'DATABASE=Dummy;'
    'Trusted_Connection=yes;'
)

# Function to generate SQL CREATE TABLE statement
def generate_create_table_sql(table_name, df):
    sql = f"CREATE TABLE {table_name} (\n"
    for col in df.columns:
        sql += f"    [{col}] NVARCHAR(MAX),\n"
    sql = sql.rstrip(',\n') + "\n);"
    return sql

# Function to escape SQL values
def escape_sql_value(value):
    if value is None:
        return "NULL"
    return "'{}'".format(value.replace("'", "''"))  # Escape single quotes

# Create tables and insert data
with pyodbc.connect(conn_str) as conn:
    cursor = conn.cursor()
    for section, df in dataframes.items():
        table_name = section.capitalize() + "Table"
        create_table_sql = generate_create_table_sql(table_name, df)
        print(f"\nSQL for {table_name}:\n{create_table_sql}")
        cursor.execute(create_table_sql)
        
        # Insert data into table
        for _, row in df.iterrows():
            columns = ', '.join(f'[{col}]' for col in row.index)
            values = ', '.join(escape_sql_value(x) for x in row.values)
            insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({values});"
            print(f"Executing SQL: {insert_sql}")  # Print insert statement for debugging
            cursor.execute(insert_sql)
        
    conn.commit()
