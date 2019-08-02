import click
import time 
from google.cloud import pubsub_v1

@click.command('emit', help='Publish messages to topic')
@click.option('--frequency', type=click.INT, required=True, help='What frequeny would you like for the messages publishing? ')
@click.option('--topic', type=click.STRING, required=True, help='Which topic should be published to? ')
@click.option('--project', type=click.STRING, required=True, help='The project Id ')
def run_publisher(frequency,project,topic):
    data = "[[5.1, 3.5]]"
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project, topic)
    while True:
        future = publisher.publish(topic_path, data=data.encode('utf-8'))
        print('Published {} of message ID {}.'.format(data, future.result()))
        time.sleep(1/frequency) # sleep arg is in seconds

if __name__ == '__main__':
    run_publisher()