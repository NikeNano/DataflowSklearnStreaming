
"""
    A streaming example using DoFn.setup to a sklearn model used for streaming predictions. 
"""


import argparse
import logging
import datetime
import pickle

import apache_beam as beam
import apache_beam.transforms.window as window

from google.cloud import storage
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from apache_beam.options.pipeline_options import SetupOptions

from apache_beam.options.pipeline_options import StandardOptions
from sklearn.ensemble import RandomForestClassifier


def run(argv=None):
    """Build and run the pipeline."""
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--input_topic',
        help=('Input PubSub topic of the form '
              '"projects/<PROJECT>/topics/<TOPIC>".'))
    group.add_argument(
        '--input_subscription',
        help=('Input PubSub subscription of the form '
              '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."'))
    parser.add_argument(
        '--bucket_project_id', required=True, help='The project id')
    parser.add_argument(
        '--bucket_name', required=True, help='The name of the bucket')
    parser.add_argument(
        '--model_path', required=True, help='The path to the model that should be loaded')
    parser.add_argument(
        '--destination_name', required=True, help='The destination name of the files')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(StandardOptions).streaming = True
    logging.info("Pipeline arguments: {}".format(pipeline_args))
    p = beam.Pipeline(options=pipeline_options)

    # Read from PubSub into Pcollection
    if known_args.input_subscription:
        messages = (p
                    | "Read messages from pubsub, subscription" >> beam.io.ReadFromPubSub(
                        subscription=known_args.input_subscription, with_attributes=True))
    else:
        messages = (p
                    | "Read messages from pubsub, subscription" >> beam.io.ReadFromPubSub(
                        topic=known_args.input_topic, with_attributes=True))

    _ = (messages | "format the data correctly" >> beam.ParDo(FormatInput())
                  | "transform the data" >> beam.ParDo(PredictSklearn(project=known_args.bucket_project_id, bucket_name=known_args.bucket_name, model_path=known_args.model_path, destination_name=known_args.destination_name))
                  | "print the data" >> beam.Map(printy)
         )
    result = p.run()
    result.wait_until_finish()


# Function to dowload model from bucekt.
def download_blob(bucket_name=None, source_blob_name=None, project=None, destination_file_name=None):
    storage_client = storage.Client(project)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)


class FormatInput(beam.DoFn):
    """ Format the input to the desired shape"""

    def process(self, element):
        output = {
            "data": eval(element.data),
            "process_time": str(datetime.datetime.now()).encode('utf-8'),
        }
        return [output]


class PredictSklearn(beam.DoFn):
    """ Format the input to the desired shape"""

    def __init__(self, project=None, bucket_name=None, model_path=None, destination_name=None):
        self._model = None
        self._project = project
        self._bucket_name = bucket_name
        self._model_path = model_path
        self._destination_name = destination_name

    # Load once or very few times
    def setup(self):
        logging.info(
            "Sklearn model initialisation {}".format(self._model_path))
        download_blob(bucket_name=self._bucket_name, source_blob_name=self._model_path,
                      project=self._project, destination_file_name=self._destination_name)
        # unpickle sklearn model
        self._model = pickle.load(open(self._destination_name, 'rb'))

    def process(self, element):
        element["prediction"] = self._model.predict(element["data"])
        return [element]

# log the output


def printy(x):
    logging.info("predictions:{}".format(x))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
