# WAU (Weekly Active User) Analytics

This repository documents the full workflow for creating a **WAU** using **Snowflake**, **Airflow**, and **Preset**.

---

## 1. Import Tables into Snowflake and Create ETL DAG

Imported the following tables from Snowflake into Airflow as an ETL DAG under the `raw` schema:

- `user_session_channel`
- `session_timestamp`

Captured the **Detailed DAG Page** from the Airflow Web AUI.

**ETL DAG Detailed Page**
![etl](https://raw.githubusercontent.com/aakashvardhan/aakash-elt-preset/main/screenshots/etl_dag_log.png)

## 2. Create ELT DAG to Join Tables

Created an ELT DAG in Airflow that joins the two tables above into a new table called `session_summary` under the `analytics` schema.

Added an extra condition to check for duplicate rows:

- The full-row duplicate check verifies whether any rows in the temporary table have identical values across all columns, not just any single key.
- It uses a `ROW_NUMBER()` window function that is partitioned by every column, flagging rows where the same combination of all field values appear more than onces.
- If any such duplicates exist, the pipeline raises an exception and logs sample duplicate rows to prevent inconsistent data from being written to the final table.

**ELT DAG Detailed Page**
![elt1](https://raw.githubusercontent.com/aakashvardhan/aakash-elt-preset/main/screenshots/elt_graph.png)

![elt2]https://raw.githubusercontent.com/aakashvardhan/aakash-elt-preset/main/screenshots/elt_dag_log.png)

## 3. Set up Preset Account and Import Dataset

Snowflake is connected to the Preset Account in order to import the `session_summary` table
