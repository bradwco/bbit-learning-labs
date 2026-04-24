import pika

from consumer_interface import mqConsumerInterface

class mqConsumer(mqConsumerInterface):
    def __init__(self, binding_key, exchange_name, queue_name) -> None:
        self.binding_key = binding_key
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        # self.connection = None
        # self.channel = None
        
    def setupRMQConnection(self):
        # Set up connection
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        
        # Establish channel
        self.channel = self.connection.channel()

        # Create queue
        self.channel.queue_declare(queue=self.queue_name)

        # Create exchange
        self.channel.exchange_declare(exchange=self.exchange_name)

        # Bind the binding key to the queue on the exchange
        self.channel.queue_bind(exchange=self.exchange_name, queue=self.queue_name, routing_key=self.binding_key)
        
        # Set up callback function for receiving messages
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=self.on_message_callback, auto_ack=False)
    
    
    def on_message_callback(self, channel, method_frame, header_frame, body):
        #Acknowledge messaage
        channel.basic_ack(delivery_tag=method_frame.delivery_tag, multiple=False)

        #Print message
        print(f"Message: {body}")

    def startConsuming(self):
        #Print the waiting message
        print(" [*] Waiting for messages. To exit press CTRL+C")

        #Start consuming messages
        self.setupRMQConnection()
        self.channel.start_consuming()

    def __del__(self):
        #Print end statement
        print("Closing RMQ connection on destruction")

        #Close Channel and Connection
        self.channel.close()
        self.connection.close()