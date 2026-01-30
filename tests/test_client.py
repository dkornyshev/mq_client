import typing
import unittest.mock

import aio_pika
import aio_pika.abc
import pytest
import pytest_mock

from mq_client import client
from mq_client import consumer
from mq_client import manager
from mq_client import models
from mq_client import producer


@pytest.fixture
def stub_mq_config() -> models.RabbitMQConfig:
    return models.RabbitMQConfig(
        broker_dns='amqp://foo:bar@test:1234',
        queue_name='test_queue',
        prefetch_limit=1,
        max_reconnect_attempts=5,
        reconnect_delay=1,
    )


@pytest.fixture
def mock_async_channel(mocker: pytest_mock.MockFixture) -> typing.Any:
    return mocker.NonCallableMock(spec=aio_pika.abc.AbstractRobustChannel)


@pytest.fixture
def mock_async_connection(
    mocker: pytest_mock.MockFixture,
    mock_async_channel: aio_pika.abc.AbstractRobustChannel,
) -> typing.Any:
    return mocker.AsyncMock(
        spec=aio_pika.abc.AbstractRobustConnection,
        channel=mocker.AsyncMock(
            spec=aio_pika.abc.AbstractRobustChannel,
            return_value=mock_async_channel,
        ),
    )


@pytest.fixture
def patch_connect_robust(
    mock_async_connection: aio_pika.abc.AbstractRobustConnection,
    mocker: pytest_mock.MockFixture,
) -> typing.Any:
    return mocker.patch(
        'aio_pika.connect_robust',
        new=mocker.AsyncMock(
            spec=aio_pika.connect_robust,
            return_value=mock_async_connection,
        ),
    )


@pytest.fixture
def patch_connection_manager_class(mocker: pytest_mock.MockFixture) -> typing.Any:
    return mocker.patch(
        'mq_client.manager.RabbitMQAsyncConnectionManager',
        side_effect=[
            mocker.NonCallableMock(spec=manager.RabbitMQAsyncConnectionManager),
        ],
    )


class TestAsyncConsumer:

    def test_new_async_producer(
        self,
        stub_mq_config: models.RabbitMQConfig,
        patch_connection_manager_class: manager.RabbitMQAsyncConnectionManager,
        mocker: pytest_mock.MockFixture,
    ) -> None:
        mq_consumer = client.new_async_consumer(stub_mq_config)

        assert isinstance(mq_consumer, consumer.AsyncConsumer)
        assert patch_connection_manager_class.call_args == mocker.call(
            models.RabbitMQConfig(
                broker_dns='amqp://foo:bar@test:1234',
                queue_name='test_queue',
                prefetch_limit=1,
                max_reconnect_attempts=5,
                reconnect_delay=1,
            ),
        )


class TestAsyncConsumerContextManager:

    async def test_new_async_consumer_cm(
        self,
        stub_mq_config: models.RabbitMQConfig,
        patch_connect_robust: unittest.mock.AsyncMock,
        mock_async_connection: aio_pika.abc.AbstractRobustConnection,
        mock_async_channel: aio_pika.abc.AbstractRobustChannel,
        mocker: pytest_mock.MockFixture,
    ) -> None:
        async with client.new_async_consumer_cm(stub_mq_config) as mq_consumer:
            assert isinstance(mq_consumer, consumer.AsyncConsumer)

            # Соединение инициализируется после создания producer
            connection_manager = mq_consumer.connection_manager
            assert connection_manager.connection is mock_async_connection
            assert connection_manager.channel is mock_async_channel

            assert mock_async_connection.close.await_args is None

        # Соединение закрывается при выходе из контекстного менеджера
        assert mock_async_connection.close.await_args == mocker.call()

        assert patch_connect_robust.call_args == mocker.call('amqp://foo:bar@test:1234/')


class TestAsyncProducer:

    def test_new_async_producer(
        self,
        stub_mq_config: models.RabbitMQConfig,
        patch_connection_manager_class: unittest.mock.NonCallableMock,
        mocker: pytest_mock.MockFixture,
    ) -> None:
        mq_producer = client.new_async_producer(stub_mq_config)

        assert isinstance(mq_producer, producer.AsyncProducer)
        assert patch_connection_manager_class.call_args == mocker.call(
            models.RabbitMQConfig(
                broker_dns='amqp://foo:bar@test:1234',
                queue_name='test_queue',
                prefetch_limit=1,
                max_reconnect_attempts=5,
                reconnect_delay=1,
            ),
        )


class TestAsyncProducerContextManager:

    async def test_new_async_producer_cm(
        self,
        stub_mq_config: models.RabbitMQConfig,
        patch_connect_robust: unittest.mock.AsyncMock,
        mock_async_connection: aio_pika.abc.AbstractRobustConnection,
        mock_async_channel: aio_pika.abc.AbstractRobustChannel,
        mocker: pytest_mock.MockFixture,
    ) -> None:
        async with client.new_async_producer_cm(stub_mq_config) as mq_producer:
            assert isinstance(mq_producer, producer.AsyncProducer)

            # Соединение инициализируется после создания producer
            connection_manager = mq_producer.connection_manager
            assert connection_manager.connection is mock_async_connection
            assert connection_manager.channel is mock_async_channel

            assert mock_async_connection.close.await_args is None

        # Соединение закрывается при выходе из контекстного менеджера
        assert mock_async_connection.close.await_args == mocker.call()

        assert patch_connect_robust.call_args == mocker.call('amqp://foo:bar@test:1234/')