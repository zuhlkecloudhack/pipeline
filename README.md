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
python store_pipeline.py \
    --project=zuhlkecloudhack \
    --topic=projects/zuhlkecloudhack/topics/flight_messages \
    --dataset=cloudhack
```

Running on Dataflow:

```
python store_pipeline.py \
    --project=zuhlkecloudhack \
    --topic=projects/zuhlkecloudhack/topics/flight_messages \
    --dataset=cloudhack \
    --temp_location gs://flight_messages/tmp/ \
    --numWorkers=3 \
    --runner DataflowRunner
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
