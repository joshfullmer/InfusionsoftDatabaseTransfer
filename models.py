from mysql.connector import connect
import pandas as pd
import re


class Database:
    def __init__(self, appname, port):
        self.connection = connect(
            user=appname,
            password=f'5425{appname}',
            database=appname,
            host='127.0.0.1',
            port=port
        )
        self.appname = appname
        self.cursor = self.connection.cursor()

    def get_table(self, tablename, columns=[]):
        if not columns:
            columns = self.get_column_names(tablename)

        quoted_columns = [f'`{column}`' for column in columns]
        column_string = ','.join(quoted_columns)
        query = (f'SELECT {column_string} FROM {tablename};')
        self.cursor.execute(query)
        data = [row for row in self.cursor]

        return pd.DataFrame(data, columns=columns)

    def get_count(self, tablename, where=''):
        query = (f'SELECT COUNT(*) FROM {tablename} {where};')
        self.cursor.execute(query)
        data = self.cursor.fetchone()[0]

        return data

    def get_column_names(self, tablename):
        query = (f"""
            SELECT `COLUMN_NAME`
            FROM `INFORMATION_SCHEMA`.`COLUMNS`
            WHERE `TABLE_SCHEMA`='{self.appname}'
            AND `TABLE_NAME`='{tablename}';
        """)
        self.cursor.execute(query)
        return [row[0] for row in self.cursor]

    def get_app_setting(self, setting):
        query = (f"""
            SELECT Value FROM AppSetting WHERE Name='{setting}';
        """)
        self.cursor.execute(query)
        results = [row for row in self.cursor]
        if not results:
            return None
        return results[0][0]

    def get_auto_increment(self, tablename):
        query = (f"""
            SELECT AUTO_INCREMENT
            FROM information_schema.TABLES
            WHERE information_schema.TABLES.TABLE_NAME='{tablename}'
            AND information_schema.TABLES.TABLE_SCHEMA='{self.appname}';
        """)
        self.cursor.execute(query)
        results = [row for row in self.cursor]
        return results[0][0]

    def get_table_create(self, tablename):
        query = (f"""
            SHOW CREATE TABLE {tablename};
        """)
        self.cursor.execute(query)
        results = [row for row in self.cursor]
        return results[0][1]

    def alter_custom_field_table(self, create_statements, rel, missing):
        create_strings = []
        for fieldname in missing:
            create_strings.append(
                f'ADD `{rel[fieldname]}` {create_statements[fieldname]}'
            )

        create_block = ',\n'.join(create_strings)
        query = (f"""
            ALTER TABLE Custom_Contact
            {create_block};
        """)
        self.cursor.execute(query)
        self.connection.commit()

    def insert_dataframe(self, tablename, dataframe, replace=False):
        quoted_columns = [f'`{column}`' for column in list(dataframe)]
        column_names = ','.join(quoted_columns)

        n = 10000
        df_list = [dataframe[i:i+n] for i in range(0, dataframe.shape[0], n)]

        for df in df_list:
            rows = [tuple(x) for x in df.values]
            row_strings = []
            for row in rows:
                row_values = []
                for cell in row:
                    if isinstance(cell, int):
                        row_values.append(str(cell))
                    elif cell is None or pd.isnull(cell):
                        row_values.append('NULL')
                    else:
                        string = re.escape(str(cell))
                        string = re.sub(r'"', r'\"', string)
                        row_values.append('"' + string + '"')
                row_string = '(' + ','.join(row_values) + ')'
                row_strings.append(row_string)
            values = ',\n'.join(row_strings)

            if replace:
                insert = 'REPLACE'
            else:
                insert = 'INSERT'

            query = (f"""
                {insert} INTO {tablename}
                    ({column_names})
                VALUES
                    {values};
            """)
            self.cursor.execute(query)
            self.connection.commit()

    def update_app_setting(self, setting, value):
        query = (f"""
            UPDATE AppSetting
            SET Value='{value}'
            WHERE Name='{setting}';
        """)
        self.cursor.execute(query)
        self.connection.commit()

    def move_credit_cards(self, old_appname, contact_rel):
        s_id = f'Id_{old_appname}'
        d_id = f'Id_{self.appname}'

        # Tablenames
        contact_rel_table = f'ContactRelationship_{old_appname}_{self.appname}'
        cc_xfer_table = f'CreditCard_from_{old_appname}'
        cc_rel_table = f'CCRelationship_{old_appname}_{self.appname}'

        # Contact Relationship Table
        create_contact_rel_table = (f"""
            CREATE TABLE IF NOT EXISTS `{contact_rel_table}` (
                `{s_id}` int(10) NOT NULL,
                `{d_id}` int(10) NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
        """)

        self.cursor.execute(create_contact_rel_table)
        contact_rel_df = pd.DataFrame(
            list(contact_rel.items()),
            columns=[s_id, d_id]
        )
        self.insert_dataframe(contact_rel_table, contact_rel_df)

        # Create credit card ID relationship
        old_ids = self.get_table(
            cc_xfer_table,
            ['Id'])['Id'].tolist()
        increment_start = self.get_auto_increment('CreditCard') + 50
        increment_end = (2 * len(old_ids)) + increment_start
        new_ids = [i for i in range(increment_start, increment_end, 2)]
        cc_rel = dict(zip(old_ids, new_ids))

        # Credit Card Relationship Table
        create_cc_rel_table = (f"""
            CREATE TABLE IF NOT EXISTS `{cc_rel_table}` (
                `{s_id}` int(10) NOT NULL,
                `{d_id}` int(10) NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
        """)

        self.cursor.execute(create_cc_rel_table)
        cc_rel_df = pd.DataFrame(
            list(cc_rel.items()),
            columns=[s_id, d_id]
        )
        self.insert_dataframe(cc_rel_table, cc_rel_df)

        # Update temp CreditCard table with new Ids
        update_contact_ids = (f"""
            UPDATE {cc_xfer_table}
            INNER JOIN {contact_rel_table}
            ON {cc_xfer_table}.ContactId={contact_rel_table}.{s_id}
            SET {cc_xfer_table}.ContactId={contact_rel_table}.{d_id};
        """)
        self.cursor.execute(update_contact_ids)

        update_cc_ids = (f"""
            UPDATE {cc_xfer_table}
            INNER JOIN {cc_rel_table}
            ON {cc_xfer_table}.Id={cc_rel_table}.{s_id}
            SET {cc_xfer_table}.Id={cc_rel_table}.{d_id};
        """)
        self.cursor.execute(update_cc_ids)

        # Insert rows from temp CreditCard table into real CreditCard table
        insert_credit_cards = (f"""
            INSERT INTO CreditCard
            SELECT * FROM {cc_xfer_table}
            WHERE ContactId IN (SELECT {d_id} FROM {contact_rel_table});
        """)
        self.cursor.execute(insert_credit_cards)

        # Drop temp tables
        drop_contact_rel_table = (f"""
            DROP TABLE IF EXISTS {contact_rel_table},{cc_rel_table};
        """)
        self.cursor.execute(drop_contact_rel_table)
        return cc_rel

    def close(self):
        self.cursor.close()
        self.connection.close()
