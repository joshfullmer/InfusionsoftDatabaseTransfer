from mysql.connector import connect
import pandas as pd


connection = connect(
    user='qj154',
    password='5425qj154',
    database='qj154',
    host='127.0.0.1',
    port=27011
)
cursor = connection.cursor()

query = ('SELECT * FROM Job;')

cursor.execute(query)

data = [row for row in cursor]

query = ("""SELECT `COLUMN_NAME`
            FROM `INFORMATION_SCHEMA`.`COLUMNS`
            WHERE `TABLE_SCHEMA`='qj154'
            AND `TABLE_NAME`='Job';""")

cursor.execute(query)

columns = [row[0] for row in cursor]

df = pd.DataFrame(data, columns=columns)
print(df)

cursor.close()
connection.close()
