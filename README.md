Apache Beam Pipelines
---------------------

Setting up dev environment:

```
conda env create -f conda.yml
source activate cloud-pipeline
```

Credentials:
```
rm ~/.config/gcloud/application_default_credentials.json
gcloud auth login
gcloud auth application-default login
```

Running locally:

```
python pipeline.py \
    --project=zuhlkecloudhack \
    --topic=projects/zuhlkecloudhack/topics/flight_messages \
    --dataset=cloudhack
    --table_name=flight_messages_python_pipeline
    --requirements_file requirements.txt
```

Running on Dataflow:

```
python pipeline.py \
    --project=zuhlkecloudhack \
    --topic=projects/zuhlkecloudhack/topics/flight_messages \
    --response_topic=projects/zuhlkecloudhack/topics/flight_messages_response \
    --dataset=cloudhack \
    --table_name=flight_messages_python_pipeline \
    --requirements_file requirements.txt \
    --temp_location=gs://flight_messages/tmp/ \
    --runner=DataflowRunner
```


Publish message into topic:
```
gcloud beta pubsub topics publish flight_messages --message '
{
"flight-number" : "CH5634",
"message" : "Fly me to the moon",
"message-type" : "INFO",
"timestamp" : "2012-04-23T18:25:43.511Z"
}
'
```
