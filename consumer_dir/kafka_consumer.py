from kafka import KafkaConsumer
from datetime import datetime
import json
import api_config

import psycopg2
from psycopg2.extras import execute_values
from psycopg2.extensions import register_adapter

register_adapter(dict, json)


connection = psycopg2.connect(user = api_config.user,
    password = api_config.password,
    host = api_config.host,
    port = api_config.port,
    database = api_config.database)

cursor = connection.cursor()


consumer = KafkaConsumer('my_topic', bootstrap_servers=['localhost:9092'],value_deserializer=lambda m: json.loads(m.decode('utf-8')))




for message in consumer:
    data = message.value



    clean_data = [{
                    'video_id': data['video_id'],
                    'trending_date': data['trending_date'],
                    'title': data['title'],
                    'channel_title': data['channel_title'],
                    'category_id': data['category_id'],
                    'publish_time': data['publish_time'],
                    'tags': data['tags'],
                    'views': data['views'],
                    'likes': data['likes'],
                    'dislikes': data['dislikes'],
                    'comment_count': data['comment_count'],
                    'thumbnail_link': data['thumbnail_link'],
                    'comments_disabled': data['comments_disabled'],
                    'ratings_disabled': data['ratings_disabled'],
                    'video_error_or_removed': data['video_error_or_removed'],
                    'description': data['description'],
                    'kind': data['kind'],
                    'etag': data['etag'],
                    'snippet_channelId': data['snippet.channelId'],
                    'category': data['category'],
                    'snippet_assignable': data['snippet.assignable']
                    }]

    

    columns = clean_data[0].keys()
    query = "INSERT INTO youtube_data ({}) VALUES %s".format(','.join(columns))

#convert projects values to sequence of seqeences
    values = [[value for value in tweet.values()] for tweet in clean_data]
    execute_values(cursor, query, values)
    print("successfully inserted")
    connection.commit()
    
    
    
    
    
    data_json = json.dumps(clean_data, indent = 4) 
    #print(data_json)
    print()

