import psycopg2

import challenge.constants as constants


class ProcessControl:

    def get_last_process_date(self):
        sql_query = "SELECT process_date from process_control_data where sequential_number = " \
                    "(select max(sequential_number) from process_control_data)"
        process_date = self.get_sql_session(sql_query)
        return process_date

    def get_last_sequential_number(self):
        sql_query = "SELECT sequential_number from process_control_data where sequential_number = " \
                    "(select max(sequential_number) from process_control_data)"
        sequential_number = self.get_sql_session(sql_query)
        return sequential_number

    def update_process_parameters(self, process_date, sequential_number):
        sql_query = 'insert into process_control_data(process_date, sequential_number) values (%s, %s)'
        values = (process_date, sequential_number)
        return self.update_sql_session(sql_query, values)

    @staticmethod
    def get_sql_session(sql_query):

        cur = None
        conn = None
        query_value = None
        try:
            conn = psycopg2.connect(
                host=constants.hostname,
                dbname=constants.database,
                user=constants.username,
                password=constants.pwd,
                port=constants.port_id)

            cur = conn.cursor()
            cur.execute(sql_query)
            query_value = cur.fetchall()

            conn.commit()

        except Exception as error:
            print(error)
        finally:
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()

        return query_value

    @staticmethod
    def update_sql_session(sql_query, values):

        cur = None
        conn = None
        try:
            conn = psycopg2.connect(
                host=constants.hostname,
                dbname=constants.database,
                user=constants.username,
                password=constants.pwd,
                port=constants.port_id)

            cur = conn.cursor()
            cur.execute(sql_query, values)

            conn.commit()

        except Exception as error:
            print(error)
        finally:
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()
