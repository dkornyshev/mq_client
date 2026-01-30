"""Клиент для взаимодействия с шиной RabbitMQ."""
import collections.abc
import contextlib

from mq_client import consumer
from mq_client import models
from mq_client import producer


def new_async_consumer(mq_config: models.RabbitMQConfig) -> consumer.AsyncConsumer:
    """Создать новый экземпляр асинхронного потребителя сообщений из шины RabbitMQ.

    Returns:
        Потребитель сообщений из шины RabbitMQ
    """
    return consumer.AsyncConsumer(mq_config)


@contextlib.asynccontextmanager
async def new_async_consumer_cm(
    mq_config: models.RabbitMQConfig,
) -> collections.abc.AsyncIterator[consumer.AsyncConsumer]:
    """Создать новый экземпляр асинхронного потребителя сообщений из шины RabbitMQ.

    Формирует асинхронный контекстный менеджер, возвращающий новый экземпляр потребителя сообщений.
    При завершении контекстного менеджера выполняет деинициализацию клиента с закрытием всех соединений.

    Yields:
        Потребитель сообщений из шины RabbitMQ
    """
    mq_consumer = new_async_consumer(mq_config)
    connection_manager = mq_consumer.connection_manager

    #  Проверяем доступность шины RabbitMQ
    await connection_manager.connect()

    try:
        yield mq_consumer
    finally:
        await connection_manager.disconnect()


def new_async_producer(mq_config: models.RabbitMQConfig) -> producer.AsyncProducer:
    """Создать новый экземпляр асинхронного формирователя сообщений в шину RabbitMQ.

    Returns:
        Формирователь сообщений в шину RabbitMQ
    """
    return producer.AsyncProducer(mq_config)


@contextlib.asynccontextmanager
async def new_async_producer_cm(
    mq_config: models.RabbitMQConfig,
) -> collections.abc.AsyncIterator[producer.AsyncProducer]:
    """Создать новый экземпляр асинхронного формирователя сообщений в шину RabbitMQ.

    Формирует асинхронный контекстный менеджер, возвращающий новый экземпляр формирователя сообщений.
    При завершении контекстного менеджера выполняет деинициализацию клиента с закрытием всех соединений.

    Yields:
        Формирователь сообщений в шину RabbitMQ
    """
    mq_producer = new_async_producer(mq_config)
    connection_manager = mq_producer.connection_manager

    #  Проверяем доступность шины RabbitMQ
    await connection_manager.connect()

    try:
        yield mq_producer
    finally:
        await connection_manager.disconnect()
