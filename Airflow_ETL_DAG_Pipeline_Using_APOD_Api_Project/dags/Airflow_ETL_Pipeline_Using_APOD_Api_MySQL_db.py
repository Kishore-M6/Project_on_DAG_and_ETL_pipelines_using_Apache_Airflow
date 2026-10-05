"""
ETL pipeline using APOD API - NASA's Astronomy Picture of the Day
"""
from airflow.sdk import dag,task
# from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime
import json

# defining dag logic under @dag decorator
@dag(
        dag_id="Airflow_ETL_Pipeline_Using_APOD_Api_MySQL_db",
        start_date=datetime(2025,1,1),
        schedule="@daily",
        catchup=False
)
def main_function():
    ## step-1: create the table if it doesn't exists
    @task
    def  create_table():
       ## inititalize the MySqlHook
       mysqlhook_connection=MySqlHook(mysql_conn_id="ETL_pipeline_MySQL_connection")
       ## sql query to create table
       query="""
         CREATE TABLE IF NOT EXISTS apod_data (
           id INT AUTO_INCREMENT PRIMARY KEY,
           title VARCHAR(255),
           explanation TEXT,
           url TEXT,
           date DATE,
           media_type VARCHAR(50)
       );
           """
       ## execute table creation query
       mysqlhook_connection.run(query)


    ## step-2: Extract the NASA API Data(APOD)- Astronomy Picture of the Day[extract pipeline] 
    ## link: https://api.nasa.gov/planetary/apod?api_key=LbBAfCdoFKLbf9AjefrkP3tcCOQHigKq1ywTLcCE  
#   extract_apod=SimpleHttpOperator(
    extract_apod=HttpOperator(
         task_id="extract_apod",
         http_conn_id="nasa_api", ## connection ID defined in airflow for NASA api
         endpoint="planetary/apod", ## NASA endpoint for APOD
         method="GET",
         data={"api_key":"{{conn.nasa_api.extra_dejson.api_key}}"}, ## use the api key from nasa site
         response_filter=lambda response:response.json() ## convert response to json
        )
#       


    ## step-3: Transform Data(Fetch information what i want to save into MySQL db)
    @task
    def transform_apod_data_from_pipeline(response):
       apod_data_dict={
           "title": response.get("title",""),
           "explanation": response.get("explanation",""),
           "url": response.get("url",""),
           "date":response.get("date",""),
           "media_type":response.get("media_type","")
       }
       return apod_data_dict;


    ## step-4: Load the data into MySQL SQL DB
    @task
    def load_apod_data_into_mysql_db(apod_data_dict):
       ## initializing the connection
       mysqlhook_connection=MySqlHook(mysql_conn_id="ETL_pipeline_MySQL_connection")

       ## insert query into MySQL table apod_data 
       insert_query="""
              insert into apod_data(title,explanation,url,date,media_type) values(%s,%s,%s,%s,%s);
              """
       ## execute the insert query
       mysqlhook_connection.run(insert_query,parameters=(
          apod_data_dict["title"],
          apod_data_dict["explanation"],
          apod_data_dict["url"],
          apod_data_dict["date"],
          apod_data_dict["media_type"]
       ))

    ## step-5: verify the database 


 ##  step-6: define task dependencies 
    table_creation=create_table() 

    ## extract phase using python right bitshift operator
    table_creation >> extract_apod

    ## transform phase
    result_dict=transform_apod_data_from_pipeline(extract_apod.output)

    ## load phase
    load_apod_data_into_mysql_db(result_dict)



main_function()