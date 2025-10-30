# WAU (Weekly Active User) Analytics

This repository documents the full workflow for creating a **WAU** using **Snowflake**, **Airflow**, and **Preset**.

---

## 1. Import Tables into Snowflake and Create ETL DAG

Imported the following tables from Snowflake into Airflow as an ETL DAG under the `raw` schema:

- `user_session_channel`
- `session_timestamp`

Captured the **Detailed DAG Page** from the Airflow Web AUI.

**ETL DAG Detailed Page**
`![https://raw.githubusercontent.com/aakashvardhan/aakash-elt-preset/main/screenshots/etl_dag_log.png]`
