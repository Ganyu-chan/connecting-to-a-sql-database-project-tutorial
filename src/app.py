import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables

# 1) Connect to the database with SQLAlchemy
def connect():
    global engine
    try:
        connection_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
        print("Starting the connection...")
        engine = create_engine(connection_string, isolation_level="AUTOCOMMIT")
        engine.connect()
        print("Connected successfully!")
        return engine
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None
engine = connect()

if engine is None:
    exit() 

# 2) Create the tables
    # inserta la informacion de "create.sql" directamente desde el archivo
with engine.connect() as connection:
    with open ("/workspaces/connecting-to-a-sql-database-project-tutorial/src/sql/create.sql") as file:
        query = text(file.read())
        connection.execute(query)


# 3) Insert data
    # Inserta la informacion de "insert.sql" directamente
with engine.connect() as connection:
    with open ("/workspaces/connecting-to-a-sql-database-project-tutorial/src/sql/insert.sql") as file1:
        query1 = text(file1.read())
        connection.execute(query1)

# 4) Use Pandas to read and display a table
df = pd.read_sql("SELECT * FROM publishers;", engine)
df1 = pd.read_sql("Select * from authors;", engine)
df2 = pd.read_sql("Select * from books;", engine)
df3 = pd.read_sql("Select * from book_authors;", engine)
print(df)
print(df1)
print(df2)
print(df3)


# para droppear todas las tablas al ejecutarla en jupyter / Comentar si no se quiere ejecutar
with engine.connect() as connection:
    connection.execute(text("""
DROP TABLE IF EXISTS publishers CASCADE;
DROP TABLE IF EXISTS authors CASCADE;
DROP TABLE IF EXISTS books CASCADE;
DROP TABLE IF EXISTS book_authors CASCADE;
"""))