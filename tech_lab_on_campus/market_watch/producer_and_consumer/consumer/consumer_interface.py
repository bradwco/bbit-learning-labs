class mqConsumerInterface:
    def __init__(
        self, binding_key: str, exchange_name: str, queue_name: str
    ) -> None:
        # Save parameters to class variables

        # Call setupRMQConnection
        pass

    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service

        # Establish Channel

        # Create Queue if not already present

        # Create the exchange if not already present

        # Bind Binding Key to Queue on the exchange

        # Set-up Callback function for receiving messages
        pass

    def on_message_callback(
        self, channel, method_frame, header_frame, body
    ) -> None:
        # Acknowledge message

        #Print message (The message is contained in the body parameter variable)

        pass

    def startConsuming(self) -> None:
        # Print " [*] Waiting for messages. To exit press CTRL+C"

        # Start consuming messages
        pass
    
    def __del__(self) -> None:
        # Print "Closing RMQ connection on destruction"
        
        # Close Channel

        # Close Connection
        
        pass
