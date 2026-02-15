# Import the DAG class from Airflow
# DAG = Directed Acyclic Graph, which defines a workflow in Airflow
from airflow import DAG

# Import PythonOperator, which allows us to run Python functions as tasks
from airflow.operators.python import PythonOperator

# Import datetime to define when the DAG should start running
from datetime import datetime


# -----------------------------
# Step 1: Define Python functions
# Each function represents one task in the pipeline
# -----------------------------

def preprocess_data():
    # This function simulates data preprocessing
    # In real projects, this could clean data, remove nulls, scale features, etc.
    print("Preprocessing data...")

def train_model():
    # This function simulates model training
    # In real projects, this could train an ML model using scikit-learn, TensorFlow, etc.
    print("Training model...")

def evaluate_model():
    # This function simulates model evaluation
    # In real projects, this could calculate accuracy, precision, recall, etc.
    print("Evaluate Models...")


# -----------------------------
# Step 2: Define the DAG (Workflow)
# -----------------------------
# dag_id: Unique name of the DAG in Airflow UI
# start_date: Date from which the DAG is allowed to run
# schedule: How often the DAG should run (@weekly means once every week)
# catchup=False: Do NOT run past missed schedules (no backfilling)
# -----------------------------

with DAG(
    dag_id='ml_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule='@weekly',
    catchup=False,
) as dag:

    # -----------------------------
    # Step 3: Define tasks using PythonOperator
    # Each task runs one Python function
    # -----------------------------

    # Task 1: Preprocess data
    preprocess = PythonOperator(
        task_id="preprocess_task",        # Unique task name in Airflow UI
        python_callable=preprocess_data   # Python function to execute
    )

    # Task 2: Train model
    train = PythonOperator(
        task_id="train_task",
        python_callable=train_model
    )

    # Task 3: Evaluate model
    evaluate = PythonOperator(
        task_id="evaluate_task",
        python_callable=evaluate_model
    )

    # -----------------------------
    # Step 4: Define task dependencies (order of execution)
    # -----------------------------
    # This means:
    # preprocess_task runs first
    # then train_task runs
    # then evaluate_task runs
    #
    # Graphically: preprocess -> train -> evaluate
    # -----------------------------

    preprocess >> train >> evaluate
