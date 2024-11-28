from datetime import datetime

date = datetime.now().strftime('%Y%m%d')



fileName = f'us-central1-airflow-cluster-36c5c9e7-bucket/data/tmp/product_{date}.json'
print(fileName)
print(type(date))