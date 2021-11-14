import json

import ujson
from flask import current_app
from kombu import Connection, Exchange, Queue
from kombu.mixins import ConsumerMixin

from listenbrainz.spark.handlers import (
    handle_candidate_sets,
    handle_dataframes,
    handle_dump_imported, handle_model,
    handle_recommendations,
    handle_user_daily_activity,
    handle_user_entity,
    handle_user_listening_activity,
    handle_sitewide_entity,
    notify_artist_relation_import,
    notify_mapping_import,
    handle_missing_musicbrainz_data,
    notify_cf_recording_recommendations_generation,
    handle_similar_users)
from listenbrainz.utils import get_fallback_connection_name
from listenbrainz.webserver import create_app

response_handler_map = {
    'user_entity': handle_user_entity,
    'user_listening_activity': handle_user_listening_activity,
    'user_daily_activity': handle_user_daily_activity,
    'sitewide_entity': handle_sitewide_entity,
    'import_full_dump': handle_dump_imported,
    'import_incremental_dump': handle_dump_imported,
    'cf_recommendations_recording_dataframes': handle_dataframes,
    'cf_recommendations_recording_model': handle_model,
    'cf_recommendations_recording_candidate_sets': handle_candidate_sets,
    'cf_recommendations_recording_recommendations': handle_recommendations,
    'import_mapping': notify_mapping_import,
    'import_artist_relation': notify_artist_relation_import,
    'missing_musicbrainz_data': handle_missing_musicbrainz_data,
    'cf_recommendations_recording_mail': notify_cf_recording_recommendations_generation,
    'similar_users': handle_similar_users,
}

RABBITMQ_HEARTBEAT_TIME = 60 * 60  # 1 hour, in seconds


class SparkReader(ConsumerMixin):

    def __init__(self):
        self.app = create_app()  # creating a flask app for config values and logging to Sentry
        self.connection = None
        self.result_exchange = Exchange(self.app.config["SPARK_RESULT_EXCHANGE"], type="fanout", durable=False)
        self.result_queue = Queue(self.app.config["SPARK_RESULT_QUEUE"], self.result_exchange)

    def get_consumers(self, Consumer, channel):
        return [Consumer(channel, queues=[self.result_queue], callbacks=[self.callback])]

    def init_rabbitmq_connection(self):
        """ Initializes the connection to RabbitMQ.

        Note: this is a blocking function which keeps retrying if it fails
        to connect to RabbitMQ
        """
        while True:
            try:
                self.connection = Connection(
                    hostname=self.app.config["RABBITMQ_HOST"],
                    userid=self.app.config["RABBITMQ_USERNAME"],
                    port=self.app.config["RABBITMQ_PORT"],
                    password=self.app.config["RABBITMQ_PASSWORD"],
                    virtual_host=self.app.config["RABBITMQ_VHOST"],
                    heartbeat=RABBITMQ_HEARTBEAT_TIME,
                    transport_options={"client_properties": {"connection_name": get_fallback_connection_name()}}
                )
                break
            except Exception:
                logger.error("Error while connecting to RabbitMQ", exc_info=True)
                sleep(1)

    def process_response(self, response):
        try:
            response_type = response["type"]
        except KeyError:
            current_app.logger.error("Bad response sent to spark_reader: %s", json.dumps(response, indent=4),
                                     exc_info=True)
            return

        try:
            response_handler = response_handler_map[response_type]
        except Exception:
            current_app.logger.error("Unknown response type: %s, doing nothing.", response_type,
                                     exc_info=True)
            return

        try:
            response_handler(response)
        except Exception:
            current_app.logger.error("Error in the spark reader response handler: data: %s",
                                     json.dumps(response, indent=4), exc_info=True)
            return

    def callback(self, body, message):
        """ Handle the data received from the queue and
            insert into the database accordingly.
        """
        current_app.logger.debug("Received a message, processing...")
        response = ujson.loads(body)
        self.process_response(response)
        message.ack()
        current_app.logger.debug("Done!")

    def start(self):
        """ initiates RabbitMQ connection and starts consuming from the queue
        """
        with self.app.app_context():
            self.app.logger.info('Spark consumer has started!')
            self.init_rabbitmq_connection()
            self.run()


if __name__ == '__main__':
    SparkReader().start()
