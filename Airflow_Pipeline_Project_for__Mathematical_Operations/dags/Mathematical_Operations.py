'''
python file  for performing mathematical operations using apache airflow 3.x version and astro CLI 
Task-1: start with  an initial number (e..g. 20)
Task 2: Add 10 to the number,
Task 3: Multiply the result by 2,
Task 4: Substract 3 from the result,
Task 5: Divide current value with 2,
Task 5: Compute the square of the result.
'''

from airflow.decorators import dag, task
from pendulum import datetime

@dag(
    dag_id="airflow_pipeline_for_mathematical_operations",
    start_date=datetime(2025, 1, 1),
    schedule="@once",
    catchup=False
)
def main_file():
    
    @task
    def start_number():
        print("Starting number 20 assigned")
        return 20

    @task
    def adding_ten(current_value: int):
        new_value = current_value + 10
        print(f"Add 10: {current_value} + 10 = {new_value}")
        return new_value
      
    @task
    def multiply_by_two(current_value: int):
        new_value = current_value * 2
        print(f"Multiply by 2: {current_value} * 2 = {new_value}")
        return new_value

    @task
    def subtract_by_fifty(current_value: int):
        new_value = current_value - 50 
        print(f"Subtract by 50: {current_value} - 50 = {new_value}")
        return new_value
        
    @task
    def divide_by_two(current_value: float):
        new_value = current_value / 2 
        print(f"Divide by 2: {current_value} / 2 = {new_value}")
        return new_value
        
    @task
    def square_of_current_value(current_value: float):
        new_value = current_value ** 2
        print(f"Square of current value: {current_value}^2 = {new_value}")
        return new_value

    # Define dependencies by passing the output of one task to the input of the next
    # Airflow automatically resolves this as an XCom pass and sets up the execution order (t1 >> t2 >> t3...)
    val_start = start_number()
    val_add = adding_ten(val_start)
    val_mult = multiply_by_two(val_add)
    val_sub = subtract_by_fifty(val_mult)
    val_div = divide_by_two(val_sub)
    square_of_current_value(val_div)

# Instantiate the DAG
main_file()