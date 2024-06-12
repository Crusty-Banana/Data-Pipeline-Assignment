from kafka import KafkaProducer
import csv
import time
import json

producer = KafkaProducer(bootstrap_servers='localhost:9092', 
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

with open('Data/log_action.csv', 'r') as file:
    fieldnames = ['student_code', 'activity', 'numberOfFile', 'timestamp']
    reader = csv.DictReader(file, fieldnames = fieldnames)
    for row in reader:
        row['student_code'] = int(row['student_code'])
        row['numberOfFile'] = int(row['numberOfFile'])
        producer.send('vdt2024', row)
        time.sleep(1)

producer.flush()