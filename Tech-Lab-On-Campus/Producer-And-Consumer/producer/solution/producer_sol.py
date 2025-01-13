import pika
import os

class mqProducer(mqProducerInterface):
    def __init__(self, routing_key: str, exchange_name: str) -> None:
        self.routing_key = routing_key
        self.exchange_name = exchange_name
        self.setupRMQConnection()

    def setupRMQConnection(self) -> None:
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        connection = pika.BlockingConnection(parameters=con_params)
        channel = connection.channel()
        # if channel.exchange None: 
        exchange = channel.exchange_declare(exchange="Exchange Name")

    def publishOrder(self, message: str) -> None:
        channel.basic_publish(
            exchange="Exchange Name",
            routing_key="Routing Key",
            body="Message",
        )
        channel.close()
        connection.close()


