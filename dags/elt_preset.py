from airflow.decorators import task
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
from datetime import timedelta
import logging
import snowflake.connector

"""
Pipeline assumes that there are two other tables in your snowflake DB

- user_session_channel
- session_timestamp
"""

USER_ID = Variable.get("snowflake_userid")
ACCOUNT = Variable.get("snowflake_account")
DATABASE = Variable.get("snowflake_database")
PASSWORD = Variable.get("snowflake_password")
VWH = Variable.get("snowflake_vwh")


def return_snowflake_conn():
    hook = SnowflakeHook(
        snowflake_conn_id="snowflake-conn",
        database=DATABASE,
        warehouse=VWH,
        user_id=USER_ID,
        account=ACCOUNT,
        password=PASSWORD,
    )

    conn = hook.get_conn()
    return conn.cursor()


def _get_temp_table_columns(cur, schema, table):
    temp_table = f"{DATABASE}.{schema}.{table}".upper()
    cur.execute(f"DESC TABLE {temp_table}")

    rows = cur.fetchall()
    cols = [r[0] for r in rows if r[0] and r[2] == "COLUMN"]
    return cols


@task
def run_ctas(schema, table, select_sql, primary_key=None):

    logging.info(table)
    logging.info(select_sql)

    cur = return_snowflake_conn()

    try:
        sql = f"CREATE OR REPLACE TABLE {schema}.temp_{table} AS {select_sql}"
        logging.info(sql)
        cur.execute(sql)

        # duplicate row check across all columns
        all_cols = _get_temp_table_columns(cur, schema, table)
        if not all_cols:
            raise Exception(f"No columns found for {schema}.temp_{table}")

        partition_cols = ", ".join([f'"{c}"' for c in all_cols])  # preserve case
        order_col = all_cols[0]
        dup_count_sql = f"""
            SELECT COUNT(*) AS duplicate_rows
            FROM (
                SELECT ROW_NUMBER() OVER (PARTITION BY {partition_cols}
                                            ORDER BY "{order_col}") AS rn
                FROM {schema}.temp_{table}
            )
            WHERE rn > 1
        """
        logging.info("checking full-row duplicates")
        cur.execute(dup_count_sql)
        dup_count = cur.fetchone()[0]

        # debugging purposes
        if dup_count and int(dup_count) > 0:
            sample_sql = f"""
                SELECT *
                FROM {schema}.temp_{table}
                QUALIFY ROW_NUMBER() OVER (PARTITION BY {partition_cols}
                                            ORDER BY "{order_col}") > 1
                LIMIT 10
            """
            logging.warning(
                f"Found {dup_count} duplicate rows in {schema}.temp_{table}"
            )
            cur.execute(sample_sql)
            samples = cur.fetchall()
            raise Exception(
                f"Duplicate row check failed: {dup_count} duplicate rows detected. Sample rows: {samples}"
            )

        # performing primary key uniqueness check
        if primary_key is not None:
            sql = f"""
                SELECT {primary_key}, COUNT(1) as cnt
                FROM {schema}.temp_{table}
                GROUP BY 1
                ORDER BY 2 DESC
                LIMIT 1"""
            print(sql)
            cur.execute(sql)
            result = cur.fetchone()
            print(result, result[1])
            if int(result[1]) > 1:
                print("!!!!!!!!!!!!!")
                raise Exception(f"Primary key uniqueness failed: {result}")

            main_table_creation_if_not_exists_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table} AS
                SELECT * FROM {schema}.temp_{table} WHERE 1=0;"""
            cur.execute(main_table_creation_if_not_exists_sql)

            swap_sql = (
                f"""ALTER TABLE {schema}.{table} SWAP WITH {schema}.temp_{table};"""
            )
            cur.execute(swap_sql)

    except Exception as e:
        raise


with DAG(
    dag_id="BuildELT_CTAS",
    start_date=datetime(2025, 10, 25),
    catchup=False,
    tags=["ELT"],
    schedule="45 2 * * *",
) as dag:

    schema = "analytics"
    table = "session_summary"
    select_sql = """SELECT u.*, s.ts FROM TRAINING_DB.raw.user_session_channel u
                        JOIN TRAINING_DB.raw.session_timestamp s on
                        u.sessionId=s.sessionId"""

    run_ctas(schema=schema, table=table, select_sql=select_sql, primary_key="sessionId")
