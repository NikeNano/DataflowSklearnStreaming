import argparse
import logging
import datetime

import apache_beam as beam
import apache_beam.transforms.window as window

from google.cloud import storage
from joblib import load
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions

model = None

def download_blob(bucket_name, source_blob_name):
    """Downloads a blob from the bucket."""
    destination_file_name = source_blob_name
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    blob.download_to_filename(destination_file_name)


class FormatInput(beam.DoFn):
    """ Format the input to the desired shape"""
    def process(self,element):
        output = {
            "data": eval(element.data),
            "process_time":str(datetime.datetime.now()).encode('utf-8'),
        }
        return [output]

class PredictSklearn(beam.DoFn):
    """ Format the input to the desired shape"""

    def __init__(self):
        self._model = None

    def setup(self):
        model_name = "model.joblib"
        download_blob(bucket_name="dataflowsklearnstreaming",source_blob_name=model_name)
        self._model = joblib.load(model_name)

    def process(self,element):
        element["prediction"] = self._model.predict(element["data"])
        return [element]

def printy(x):
        logging.info("predictions:{}".format(x))

def get_cloud_pipeline_options():
    """Get apache beam pipeline options to run with Dataflow on the cloud
    """
    options = {
        'runner': 'DataflowRunner',
        'job_name': 'test',
        'staging_location': "gs://STAGING/",
        'temp_location':  "gs://TEMP/",
        'project': "PROJECTID",
        'region': 'europe-west1',
        'autoscaling_algorithm' :  'THROUGHPUT_BASED',
        'max_num_workers':7,
        'setup_file': './setup.py',
    }
    return beam.pipeline.PipelineOptions(flags=[], **options)


def run(argv=None):
    """Build and run the pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
      '--cloud', required=True,
      help="Do you like to go cloud")
    parser.add_argument(
      '--input_topic'
      ,required=False
      ,help='"projects/<PROJECTID>/topics/<TOPIC>".')
    known_args, pipeline_args = parser.parse_known_args(argv)

    if known_args.cloud == "y":
        pipeline_options = get_cloud_pipeline_options()
    else:
        pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(StandardOptions).streaming = True
    p = beam.Pipeline(options=pipeline_options)

    input_pubsub = ( p | 'Read from PubSub 2' >> beam.io.gcp.pubsub.ReadFromPubSub(topic=known_args.input_topic,with_attributes=True))
    _ = (input_pubsub | "format the data correctly" >> beam.ParDo(FormatInput())
                  | "transform the data" >> beam.ParDo(PredictSklearn())
                  | "print the data" >> beam.Map(printy)
        )
    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()