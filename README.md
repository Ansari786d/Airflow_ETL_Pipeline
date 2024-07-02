# Airflow_ETL_Pipeline
Build an End to End ETL pipeline using Airflow!
here I are using open weather api 
1.First we are checking is the api available if the 
    API is not available then we will wait until the api is 
    up.
2.Secondly once the api is available then we extract the 
    data using GET method.
3.We transform the data using pandas and save the data 
    in csv format. 