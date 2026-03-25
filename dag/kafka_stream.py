from datetime import datetime
from airflow import DAG 
from airflow.operators.python import PythonOperator #this operator allows us to execute Python functions in our DAG


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026,3,24, 14,00) #start date for the DAG
}

def get_data():
    #function to get data from API
    import json 
    import requests 
    
    res = requests.get('https://randomuser.me/api/')
    res = res.json()
    res = res['results'][0]

    return res 

def format_data(res):
    data={}
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = (
        res['location']['street']['name']
        + ' ' + str(res['location']['street']['number'])
        + ' ' + res['location']['city']
        + ' ' + res['location']['state']
        + ' ' + res['location']['country']
    )
    data['postcode'] = res['location']['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username'] 
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    
    return data 
    


def stream_data(): 
    import json
    from kafka import KafkaProducer
    import logging
    import time

    producer = KafkaProducer(bootstrap_servers='broker:29092', max_block_ms=5000)
    curr_time = time.time()
    
    while True:
        if time.time() > curr_time +60: #1minute
            break
        try:
            res = get_data()
            res = format_data(res)
            producer.send('user_data', json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error(f"Error sending message to Kafka: {e}")
            continue    
    
    

with DAG('user_automation',
        default_args=default_args, #default arguments for the DAG
        schedule='@daily',
        catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data  #function that will stream data from API
    )
