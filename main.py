import argparse
import logging
import datetime
import logging
import pickle

import apache_beam as beam
import apache_beam.transforms.window as window

from google.cloud import storage
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from sklearn.ensemble import RandomForestClassifier


model = None


def download_blob(bucket_name, source_blob_name):
    """Downloads a blob from the bucket."""
    destination_file_name = source_blob_name
    storage_client = storage.Client("iotpubsub-1536350750202")
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

    def __init__(self):
        self._model = None

    def setup(self):
        logging.info("Do it start up, BRAAA")
        model_name = "rf_model.sav"
        download_blob(bucket_name="dataflowsklearnstreaming",
                      source_blob_name=model_name)
        self._model = pickle.load(open(model_name, 'rb'))

    def process(self, element):
        element["prediction"] = self._model.predict(element["data"])
        return [element]


def printy(x):
    print(x)
    logging.info("predictions:{}".format(x))


def get_cloud_pipeline_options():
    """Get apache beam pipeline options to run with Dataflow on the cloud
    """
    options = {
        'runner': 'DataflowRunner',
        'job_name': 'streaming5',
        'staging_location': "gs://dataflowsklearnstreaming/",
        'temp_location':  "gs://dataflowsklearnstreaming/",
        'project': "iotpubsub-1536350750202",
        'region': 'europe-west1',
        'autoscaling_algorithm': 'THROUGHPUT_BASED',
        'max_num_workers': 7,
        'setup_file': './setup.py',
    }
    return beam.pipeline.PipelineOptions(flags=[], **options)


def run(argv=None):
    """Build and run the pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--cloud', required=False,
        help="Do you like to go cloud")
    parser.add_argument(
        '--topic', required=False, help='"projects/<PROJECTID>/topics/<TOPIC>".', default="projects/iotpubsub-1536350750202/topics/SklearnStreamingDataflow")
    known_args, pipeline_args = parser.parse_known_args(argv)

    # if known_args.cloud == "y":
    #    pipeline_options = get_cloud_pipeline_options()
    # else:
    #    pipeline_options = PipelineOptions(pipeline_args)
    #pipeline_options.view_as(SetupOptions).save_main_session = True
    #pipeline_options.view_as(StandardOptions).streaming = True
    options = {
        "runner": "PortableRunner",
        "job_endpoint": "localhost:8099"
    }
    options_flink = beam.pipeline.PipelineOptions(flags=[], **options)
    p = beam.Pipeline(options=options_flink)

    input_pubsub = (p | 'Read from PubSub 2' >> beam.io.gcp.pubsub.ReadFromPubSub(
        topic=known_args.topic, with_attributes=True))
    _ = (input_pubsub | "format the data correctly" >> beam.ParDo(FormatInput())
         | "transform the data" >> beam.ParDo(PredictSklearn())
         | "print the data" >> beam.Map(printy)
         )
    result = p.run()
    # result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
