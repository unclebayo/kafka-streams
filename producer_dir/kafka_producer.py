from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer
import pandas as pd
import time
import os
import json


# Create the Kafka topic if it doesn't exist

try:
    topic_name= 'my_topic'
    admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092'], client_id='test')
    topic_list = [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
    print(f'topic {topic_name} successfuky created')
            
except Exception as e:
    print(f"topic {topic_name}  is already existing")
    

# Create the Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# get the list of all CSV files in the source_dataset folder
csv_files = [filename for filename in os.listdir('source_dataset') if filename.endswith('.csv')]
#print(csv_files)

# loop through each CSV file and merge with the corresponding JSON file
merged_data = pd.DataFrame()
for csv_file in csv_files:
    # extract the country code from the filename
    country_code = csv_file[:2]
    
    # read the CSV file
    csv_data = pd.read_csv(os.path.join('source_dataset', csv_file), encoding='windows-1256')
    
    # read the corresponding JSON file
    json_file = os.path.join('source_dataset', f'{country_code}_category_id.json')
    with open(json_file, 'r') as f:
        json_data = pd.json_normalize(json.load(f)['items'])
        json_data = json_data.rename(columns={'id': 'category_id', 'snippet.title': 'category'})
    
    # merge the CSV and JSON data
    json_data['category_id'] = json_data['category_id'].astype(int)
    merged_data = pd.concat([merged_data, pd.merge(csv_data, json_data, on='category_id')])


for index, row in merged_data.iterrows():
    data = row.to_dict()
    print(data)
    producer.send(topic_name, data)
    time.sleep(10)

    #1234874687082760938-07934893214285y-084ty-0. kafka (inbuilt buffer)
