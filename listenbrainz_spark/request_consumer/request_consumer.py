# listenbrainz-labs
#
# Copyright (C) 2019 Param Singh <iliekcomputers@gmail.com>
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

import json
import logging
import time

from kombu import Queue, Exchange
from kombu.mixins import ConsumerProducerMixin

import listenbrainz_spark
import listenbrainz_spark.query_map
from listenbrainz_spark import config, hdfs_connection
from listenbrainz_spark.utils import init_rabbitmq

RABBITMQ_HEARTBEAT_TIME = 2 * 60 * 60  # 2 hours -- a full dump import takes 40 minutes right now
logger = logging.getLogger(__name__)


class RequestConsumer(ConsumerProducerMixin):

    def __init__(self):
        self.connection = init_rabbitmq(
            username=config.RABBITMQ_USERNAME,
            password=config.RABBITMQ_PASSWORD,
            host=config.RABBITMQ_HOST,
            port=config.RABBITMQ_PORT,
            vhost=config.RABBITMQ_VHOST,
            heartbeat=RABBITMQ_HEARTBEAT_TIME,
        )
        self.request_exchange = Exchange(config.SPARK_REQUEST_EXCHANGE, type="fanout", durable=False)
        self.request_queue = Queue(config.SPARK_REQUEST_QUEUE, self.request_exchange)
        self.result_exchange = Exchange(config.SPARK_RESULT_EXCHANGE, type="fanout", durable=False)
        self.result_queue = Queue(config.SPARK_RESULT_QUEUE, self.result_exchange)

    def get_result(self, request):
        try:
            query = request["query"]
            params = request.get("params", {})
        except KeyError:
            logger.error("Bad query sent to spark request consumer: %s", json.dumps(request), exc_info=True)
            return None

        logger.info("Query: %s", query)
        logger.info("Params: %s", params)

        try:
            query_handler = listenbrainz_spark.query_map.get_query_handler(query)
        except KeyError:
            logger.error("Bad query sent to spark request consumer: %s", query, exc_info=True)
            return None
        except Exception:
            logger.error("Error while mapping query to function:", exc_info=True)
            return None

        try:
            # initialize connection to HDFS, the request consumer is a long running process
            # so we try to create a connection everytime before executing a query to avoid
            # affecting subsequent queries in case there's an intermittent connection issue
            hdfs_connection.init_hdfs(config.HDFS_HTTP_URI)
            return query_handler(**params)
        except TypeError:
            logger.error("TypeError in the query handler for query '%s', maybe bad params:", query, exc_info=True)
            return None
        except Exception:
            logger.error("Error in the query handler for query '%s':", query, exc_info=True)
            return None

    def push_to_result_queue(self, messages):
        logger.debug("Pushing result to RabbitMQ...")
        num_of_messages = 0
        total_size_of_message = 0
        for message in messages:
            num_of_messages += 1
            body = json.dumps(message)
            total_size_of_message += len(body)
            if message:
                self.producer.publish(
                    body,
                    exchange=self.result_exchange,
                    routing_key="",
                    delivery_mode=2,
                    content_type="application/json",
                    content_encoding="utf-8",
                )

        if num_of_messages:
            avg_size_of_message = total_size_of_message // num_of_messages
        else:
            avg_size_of_message = 0
            logger.warning("No messages calculated")

        logger.info("Done!")
        logger.info("Number of messages sent: %d", num_of_messages)
        logger.info("Average size of message: %d bytes", avg_size_of_message)

    def callback(self, body, message):
        request = json.loads(body)
        logger.info("Received a request!")
        messages = self.get_result(request)
        if messages:
            self.push_to_result_queue(messages)
        message.ack()
        logger.info("Request done!")

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=[self.request_queue], callbacks=[self.callback], prefetch_count=1)]


def main(app_name):
    listenbrainz_spark.init_spark_session(app_name)
    while True:
        try:
            logger.info("Starting Request Consumer!")
            RequestConsumer().run()
        except Exception:
            logger.error("Error in the request consumer:", exc_info=True)
            time.sleep(5)


if __name__ == "__main__":
    try:
        main("spark-writer")
    except KeyboardInterrupt:
        pass
