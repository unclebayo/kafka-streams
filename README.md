# Data engineering Task for The Room
This project is designed to pull real data in realtime, stored, create api to get live statistics etc




### My Approach.
###### The `docker-compose.yml` running kafka, kafdrop and postgresql db
- kafdrop is running on `<host>:9000` in this case `localhost:9000`
- kafka is running on `<host>:9020` in this case `localhost:9020`
- PostgreSQL is running on `<host>:54320`  in this case `localhost:54320`

all of this can be started by cd directory into the project home 
and running `docker-compose`. This assumes docker is installed.




There are three other python scripts.

- `kafka_producer.py` stored in the producer directory. it sends data in real time to kafka. To run it,
open a new terminal, create a virtual env;
run `pip install -r requirements.txt`
then `python kafka_producer.py`

- `kafka_consumer.py` stored in the consumer directory. it get data in real time from kafka to the postgres db. 
open a new terminal, connect to the virtual env already created above
run `pip install -r requirements.txt`
then `python kafka_consumer.py`
