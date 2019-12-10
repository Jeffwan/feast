from typing import Optional, Union


class ProducerInterface:
    """
    Interface for Kafka producers
    """

    def __init__(self, brokers: str):
        self.brokers = brokers

    def produce(self, topic: str, data: str):
        message = "{} should implement a produce method".format(
            self.__class__.__name__)
        raise NotImplementedError(message)

    def flush(self, timeout: int):
        message = "{} should implement a flush method".format(
            self.__class__.__name__)
        raise NotImplementedError(message)


class ConfluentProducer(ProducerInterface):
    """
    Concrete implementation of Confluent Kafka producer (confluent-kafka)
    """

    def __init__(self, brokers: str):
        from confluent_kafka import Producer
        self.producer = Producer({"bootstrap.servers": brokers})

    def produce(self, topic: str, value: bytes):
        """
        Generic produce that implements confluent-kafka's produce method to
        push a byte encoded object into a Kafka topic.

        Args:
            topic (str): Kafka topic.
            value (bytes): Byte encoded object.

        Returns:
            None: None.
        """
        return self.producer.produce(topic, value=value)

    def flush(self, timeout: Optional[int]):
        """
        Generic flush that implements confluent-kafka's flush method.

        Args:
            timeout (Optional[int]): timeout in seconds to wait for completion.

        Returns:
            int: Number of messages still in queue.
        """
        self.producer.flush(timeout=timeout)


class KafkaPythonProducer(ProducerInterface):
    """
    Concrete implementation of Python Kafka producer (kafka-python)
    """

    def __init__(self, brokers: str):
        from kafka import KafkaProducer
        self.producer = KafkaProducer(bootstrap_servers=[brokers])

    def produce(self, topic: str, value: bytes):
        """
        Generic produce that implements kafka-python's send method to push a
        byte encoded object into a Kafka topic.

        Args:
            topic (str): Kafka topic.
            value (bytes): Byte encoded object.

        Returns:
            FutureRecordMetadata: resolves to RecordMetadata

        Raises:
            KafkaTimeoutError: if unable to fetch topic metadata, or unable
                to obtain memory buffer prior to configured max_block_ms
        """
        self.producer.send(topic, value=value)

    def flush(self, timeout: Optional[int]):
        """
        Generic flush that implements kafka-python's flush method.

        Args:
            timeout (Optional[int]): timeout in seconds to wait for completion.

        Returns:
            None

        Raises:
            KafkaTimeoutError: failure to flush buffered records within the
                provided timeout
        """
        self.producer.flush(timeout=timeout)


def get_producer(brokers: str) -> Union[ConfluentProducer, KafkaPythonProducer]:
    """
    Simple context helper function that returns a ProducerInterface object when
    invoked.

    This helper function will try to import confluent-kafka as a producer first.

    This helper function will fallback to kafka-python if it fails to import
    confluent-kafka.

    Args:
        brokers (str): Kafka broker information with hostname and port.

    Returns:
        Union[ConfluentProducer, KafkaPythonProducer]:
            Concrete implementation of a Kafka producer. Ig can be:
                * confluent-kafka producer
                * kafka-python producer
    """
    try:
        return ConfluentProducer(brokers)
    except ImportError as e:
        print("Unable to import confluent-kafka, falling back to kafka-python")
        return KafkaPythonProducer(brokers)
