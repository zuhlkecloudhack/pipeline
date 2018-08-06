import datetime
import time

import argparse
import json
import logging

import apache_beam as beam
from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms import trigger


# Derived from https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/complete/game/game_stats.py

def timestamp2str(t, fmt='%Y-%m-%d %H:%M:%S.000'):
  """Converts a unix timestamp into a formatted string."""
  return datetime.datetime.fromtimestamp(t).strftime(fmt)

class WriteToBigQuery(beam.PTransform):
    """Generate, format, and write BigQuery table row information."""
    def __init__(self, table_name, dataset, schema):
        """Initializes the transform.
        Args:
          table_name: Name of the BigQuery table to use.
          dataset: Name of the dataset to use.
          schema: Dictionary in the format {'column_name': 'bigquery_type'}
        """
        super(WriteToBigQuery, self).__init__()
        self.table_name = table_name
        self.dataset = dataset
        self.schema = schema

    def get_schema(self):
        """Build the output table schema."""
        return ', '.join(
            '%s:%s' % (col, self.schema[col]) for col in self.schema)

    def expand(self, pcoll):
        project = pcoll.pipeline.options.view_as(GoogleCloudOptions).project
        return (
            pcoll
            | 'ConvertToRow' >> beam.Map(
                lambda elem: {col: elem[col] for col in self.schema})
            | beam.io.WriteToBigQuery(
                self.table_name, self.dataset, project, self.get_schema()))


class ParseFlightEventFn(beam.DoFn):
    """Parses the raw game event info into a Python dictionary.
    Each event line has the following format:
    username,teamname,score,timestamp_in_ms,readable_time
    e.g.:
    user2_AsparagusPig,AsparagusPig,10,1445230923951,2015-11-02 09:09:28.224
    The human-readable time string is not used here.
    """
    def __init__(self):
        super(ParseFlightEventFn, self).__init__()
        self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_errors')

    def process(self, elem):
        try:
            row = json.loads(elem)
            event_dt = datetime.datetime.strptime(row["timestamp"][:-5],  '%Y-%m-%dT%H:%M:%S')
            row["timestamp"] = time.mktime(event_dt.timetuple())
            row["no-of-words"] = len(row["message"].split(" "))
            yield row
        except Exception as e:  # pylint: disable=bare-except
            # Log and count parse errors
            self.num_parse_errors.inc()
            logging.error('Parse error on "%s"', elem)
            logging.error(e)


class CalculateFlightScores(beam.PTransform):
    """Extract user/score pairs from the event stream using processing time, via
    global windowing. Get periodic updates on all users' running scores.
    """
    def __init__(self, allowed_lateness):
        super(CalculateFlightScores, self).__init__()
        self.allowed_lateness_seconds = allowed_lateness * 60

    def expand(self, pcoll):
        # NOTE: the behavior does not exactly match the Java example
        # TODO: allowed_lateness not implemented yet in FixedWindows
        # TODO: AfterProcessingTime not implemented yet, replace AfterCount
        return (
                pcoll
                # Get periodic results every ten events.
                | 'LeaderboardUserGlobalWindows' >> beam.WindowInto(
                        beam.window.GlobalWindows(),
                            trigger=trigger.Repeatedly(trigger.AfterCount(10)),
                            accumulation_mode=trigger.AccumulationMode.ACCUMULATING)
                # Extract and sum flight/score pairs from the event data.
                | 'ExtractAndSumScore' >> ExtractAndSumWords())


class ExtractAndSumWords(beam.PTransform):

    def expand(self, pcoll):
        return (pcoll
                | beam.Map(lambda elem: (elem['flight-number'], elem["no-of-words"]))
                | beam.CombinePerKey(sum))


class FlightScoreDict(beam.DoFn):
    """Formats the data into a dictionary of BigQuery columns with their values
    Receives a (team, score) pair, extracts the window start timestamp, and
    formats everything together into a dictionary. The dictionary is in the format
    {'bigquery_column': value}
    """
    def process(self, flight_score, window=beam.DoFn.WindowParam):
        (flight, score) = flight_score
        yield {
            'flight': flight,
            'total_no_of_words': score,
            'processing_time': timestamp2str(int(time.time()))

        }


def run(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument('--topic',
                        type=str,
                        help='Pub/Sub topic to read from')
    parser.add_argument('--dataset',
                      type=str,
                      required=True,
                      help='BigQuery Dataset to write tables to. '
                      'Must already exist.')
    parser.add_argument('--table_name',
                      default='flight_messages',
                      help='The BigQuery table name. Should not already exist.')
    parser.add_argument('--allowed_lateness',
                        type=int,
                        default=1,
                        help='Numeric value of allowed data lateness, in minutes')

    args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    options.view_as(SetupOptions).save_main_session = True

    # Enforce that this pipeline is always run in streaming mode
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        # Read game events from Pub/Sub using custom timestamps, which are extracted
        # from the pubsub data elements, and parse the data.

        scores = p | 'ReadPubSub' >> beam.io.ReadFromPubSub(topic=args.topic)

        events = (
            scores
            | 'ParseFlightEventFn' >> beam.ParDo(ParseFlightEventFn())
            | 'AddEventTimestamps' >> beam.Map(
                lambda elem: beam.window.TimestampedValue(elem, elem['timestamp'])))

        def format_flight_score_sums(flight_score):
            (flight, score) = flight_score
            return {'flight': flight,
                    'total_no_of_words': score,
                    'processing_time': timestamp2str(int(time.time()))
                    }

        # Get user scores and write the results to BigQuery
        (events  # pylint: disable=expression-not-assigned
         | 'CalculateUserScores' >> CalculateFlightScores(args.allowed_lateness)
         | 'FlightScoresDict' >> beam.Map(format_flight_score_sums)
         # | 'FlightScoresDict' >> beam.ParDo(FlightScoreDict())
         | 'WriteUserScoreSums' >> WriteToBigQuery(
             args.table_name, args.dataset, {
                 'flight': 'STRING',
                 'total_no_of_words': 'INTEGER',
                 'processing_time': 'STRING',
             }))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()