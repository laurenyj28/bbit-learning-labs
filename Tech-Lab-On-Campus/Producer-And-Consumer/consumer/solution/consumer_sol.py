import pika
import os

class mqConsumer(mqConsumerInterface):
    def __init__(self, binding_key: str, exchange_name: str, queue_name: str) -> None:
        self.binding_key = binding_key
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.setup_RMQConnection()
    
    def setupRMQConnection(self) -> None:
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        connection = pika.BlockingConnection(parameters=con_params)
        channel = connection.channel()
        # if queue not present...
        channel.queue_declare(queue="Queue Name")
        # if exchange not present...
        exchange = channel.exchange_declare(exchange="Exchange Name")
        channel.queue_bind(
            queue= "Queue Name",
            routing_key= "Routing Key",
            exchange="Exchange Name",
        )
        channel.basic_consume(
            "Queue Name", Function Name, auto_ack=False
        )
    
    def on_message_callback(self, channel, method_frame, header_fram, body) -> None:
        channel.basic_ack(method_frame.delivery_tag, False)
        message = json.loads(body)
        print(message)
    
    def startconsuming(self) -> None:
        print('[*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()
    
    def __del__(self) -> None:
        print('Closing RMQ connection on destruction')
        channel.close()
        connection.close()