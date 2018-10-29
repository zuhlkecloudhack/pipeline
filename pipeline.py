import logging
import argparse

import apache_beam as beam
from apache_beam.options.pipeline_options import StandardOptions, PipelineOptions

SCHEMA = 'flight_number:STRING, message:STRING, message_type:STRING, hash:STRING, total_no_of_words:INTEGER, processing_time:TIMESTAMP, event_time:TIMESTAMP'


def parse_message(line):
    import json
    from datetime import datetime
    record = json.loads(line)

    return (record["flight-number"],
            record["message"],
            record["message-type"],
            datetime.strptime(record["timestamp"][:-5], '%Y-%m-%dT%H:%M:%S'),
            datetime.now())


def process_message(msg_dict):
    import bcrypt
    message = msg_dict["message"]

    msg_dict["hash"] = bcrypt.hashpw(message.encode(), bcrypt.gensalt(14))

    msg_dict['total_no_of_words'] = len(message.split(" "))
    return msg_dict


def convert_to_dict(values):
    TIMESTAMP_FMT = '%Y-%m-%d %H:%M:%S.000'

    flight_number, message, message_type, event_time, processing_time = values

    return {'flight_number': flight_number,
            'message': message,
            'message_type': message_type,
            'processing_time': processing_time.strftime(TIMESTAMP_FMT),
            'event_time': event_time.strftime(TIMESTAMP_FMT)
            }


def to_json(msg_dict):
    import json
    return json.dumps(msg_dict).encode()


def run(argv=None):
  parser = argparse.ArgumentParser()
  parser.add_argument('--topic')
  parser.add_argument('--response_topic')
  parser.add_argument('--dataset')
  parser.add_argument('--table_name')
  args, pipeline_args = parser.parse_known_args(argv)

  options = PipelineOptions(pipeline_args)
  options.view_as(StandardOptions).streaming = True

  with beam.Pipeline(options=options) as p:
    messages = (p | 'ReadPubSub' >> beam.io.ReadFromPubSub(args.topic)
                  | "ParseRawMessage" >> beam.Map(parse_message)
                  | "ConvertRadiogram" >> beam.Map(convert_to_dict)
                  | "ProcessMessage" >> beam.Map(process_message)
                )

    (messages | "ConvertForPubSub" >> beam.Map(to_json)
              | "WriteToPubSub" >> beam.io.WriteToPubSub(args.response_topic)
    )

    messages | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
                    args.table_name, args.dataset,
                    schema=SCHEMA,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()