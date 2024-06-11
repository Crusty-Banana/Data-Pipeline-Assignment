# Deploy data pipeline system

First turn up docker compose using

```
docker compose up -d
```

You can turn off docker compose when done

```
docker compose down -v
```

# Stream content to topic

Then stream content to kafka from your computer

```
python Kafka/kafka_producer.py
```

# Process Data

When you want to process data from HDFS using Spark, run

```
docker exec -it spark-master sh -c "spark-submit --master spark://spark-master:7077 /opt/bitnami/spark/process_data.py"
```

# Save to local computer

When you want to download processed data from HDFS to local computer at /Hadoop, run

```
docker exec -it namenode sh -c "hdfs dfs -copyToLocal /output /Hadoop"
```

# Some useful command

```
python Kafka/kafka_consumer.py
docker exec -it namenode sh -c "hdfs dfs -rm -r /output/*"
docker exec -it namenode sh -c "hdfs dfs -rm -r /raw_zone/fact/activity/*"
```
