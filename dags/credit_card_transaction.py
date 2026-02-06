from datetime import datetime, timedelta
import random
from faker import Faker

from airflow.models import DAG, Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator

fake = Faker()

# Transaction metadata
transaction_types = ['POS_PURCHASE', 'ATM_WITHDRAWAL', 'ONLINE_PAYMENT', 'BALANCE_TRANSFER', 'BILL_PAYMENT']
merchant_categories = ['RESTAURANTS', 'ELECTRONICS', 'TRAVEL', 'FUEL', 'GROCERY', 'HEALTHCARE', 'EDUCATION', 'INSURANCE']

# Function to generate SQL for synthetic credit card transactions
def generate_insert_sql(table_name, n_rows=10):
    region_map = {
        "UE": {"countries": ["DE", "FR", "IT", "ES"], "currency": "EUR"},
        "IN": {"countries": ["IN"], "currency": "INR"},
        "NA": {"countries": ["US", "CA", "MX"], "currency": "USD"}
    }
    values = []
    for _ in range(n_rows):

        # 1. Randomly pick one of your three regions
        reg_code = random.choice(["UE", "IN", "NA"])
        region_data = region_map[reg_code]
        
        # 2. Pick a country and currency based on that region
        country = random.choice(region_data["countries"])
        currency = region_data["currency"]
        
        transaction_date = fake.date_between(start_date='-2y', end_date='today')
        t_type = random.choice(transaction_types)
        merchant = random.choice(merchant_categories)
        amount = round(random.uniform(20.0, 10000.0), 2)
        card_number = f"**** **** **** {random.randint(1000, 9999)}"
        customer_id = fake.uuid4()
        account_id = f"ACCT-{random.randint(100000, 999999)}"
        merchant_id = f"MERCH-{random.randint(10000, 99999)}"
        # currency = random.choice(['USD', 'EUR', 'GBP', 'INR'])
        city = fake.city().replace("'", "")
        # country = fake.country_code().replace("'", "")
        fraud_flag = random.choices([0, 1], weights=[98, 2])[0]  # 2% fraud probability

        values.append(
            f"('{transaction_date}', '{t_type}', '{merchant}', {amount}, '{currency}', "
            f"'{card_number}', '{customer_id}', '{account_id}', '{merchant_id}', '{city}', '{country}', '{reg_code}', {fraud_flag})"
        )

    values_str = ",\n".join(values)
    return f"""
    INSERT INTO {table_name} (
        transaction_date, transaction_type, merchant_category, amount, currency,
        card_number, customer_id, account_id, merchant_id, city, country, region , fraud_flag
    )
    VALUES
    {values_str};
    """

# Configurable number of rows per center from Airflow UI Variables
n_rows_center1 = int(Variable.get("n_rows_center1", default_var=10))
n_rows_center2 = int(Variable.get("n_rows_center2", default_var=10))

# Schema definitions
create_center1_sql = """
CREATE TABLE IF NOT EXISTS credit_card_center_1 (
    transaction_date DATE,
    transaction_type VARCHAR,
    merchant_category VARCHAR,
    amount NUMERIC(12,2),
    currency VARCHAR(3),
    card_number VARCHAR(20),
    customer_id VARCHAR(64),
    account_id VARCHAR(32),
    merchant_id VARCHAR(32),
    city VARCHAR(64),
    country VARCHAR(2),
    region VARCHAR(2),
    fraud_flag SMALLINT
);
"""

create_center2_sql = """
CREATE TABLE IF NOT EXISTS credit_card_center_2 (
    transaction_date DATE,
    transaction_type VARCHAR,
    merchant_category VARCHAR,
    amount NUMERIC(12,2),
    currency VARCHAR(3),
    card_number VARCHAR(20),
    customer_id VARCHAR(64),
    account_id VARCHAR(32),
    merchant_id VARCHAR(32),
    city VARCHAR(64),
    country VARCHAR(2),
    region VARCHAR(2),
    fraud_flag SMALLINT
);
"""

create_combined_sql = """
CREATE TABLE IF NOT EXISTS credit_card_transactions_combined (
    transaction_date DATE,
    transaction_type VARCHAR,
    merchant_category VARCHAR,
    amount NUMERIC(12,2),
    currency VARCHAR(3),
    card_number VARCHAR(20),
    customer_id VARCHAR(64),
    account_id VARCHAR(32),
    merchant_id VARCHAR(32),
    city VARCHAR(64),
    country VARCHAR(2),
    region VARCHAR(2),
    fraud_flag SMALLINT
);
"""

# Combine both center tables
combine_data_sql = """
INSERT INTO credit_card_transactions_combined (
    transaction_date, transaction_type, merchant_category, amount, currency,
    card_number, customer_id, account_id, merchant_id, city, country, region ,fraud_flag
)
SELECT * FROM credit_card_center_1
UNION
SELECT * FROM credit_card_center_2;
"""

# DAG definition
with DAG(
    dag_id='credit_card_transactions_pipeline',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=2)
    }
) as dag:

    create_center1 = PostgresOperator(
        task_id='create_center1',
        postgres_conn_id='postgres_database_conn',
        sql=create_center1_sql
    )

    create_center2 = PostgresOperator(
        task_id='create_center2',
        postgres_conn_id='postgres_database_conn',
        sql=create_center2_sql
    )

    create_combined = PostgresOperator(
        task_id='create_combined',
        postgres_conn_id='postgres_database_conn',
        sql=create_combined_sql
    )

    insert_center1 = PostgresOperator(
        task_id='insert_random_center1',
        postgres_conn_id='postgres_database_conn',
        sql=generate_insert_sql('credit_card_center_1', n_rows_center1)
    )

    insert_center2 = PostgresOperator(
        task_id='insert_random_center2',
        postgres_conn_id='postgres_database_conn',
        sql=generate_insert_sql('credit_card_center_2', n_rows_center2)
    )

    combine_data = PostgresOperator(
        task_id='combine_data',
        postgres_conn_id='postgres_database_conn',
        sql=combine_data_sql
    )

    [create_center1 >> insert_center1,
     create_center2 >> insert_center2] >> create_combined >> combine_data
