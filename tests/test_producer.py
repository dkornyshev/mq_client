import asyncio
import logging
import typing
import unittest.mock

import aio_pika
import aio_pika.exceptions
import pytest
import pytest_mock

from mq_client import manager
from mq_client import models
from mq_client import producer


LOG_NAME = 'mq_client.producer'


class MockAsyncProducer:

    @pytest.fixture
    def stub_mq_config(self) -> models.RabbitMQConfig:
        return models.RabbitMQConfig(
            broker_dsn='amqp://foo:bar@test:1234',
            queue_name='test_queue',
            prefetch_limit=1,
            max_reconnect_attempts=5,
            reconnect_delay=1,
        )

    @pytest.fixture
    def stub_mq_producer(self, stub_mq_config: models.RabbitMQConfig) -> producer.AsyncProducer:
        return producer.AsyncProducer(stub_mq_config)

    @pytest.fixture
    def stub_message(self) -> dict[str, typing.Any]:
        return {"foo": "bar"}

    @pytest.fixture
    def mock_async_channel(self, mocker: pytest_mock.MockFixture) -> typing.Any:
        return mocker.NonCallableMock(
            spec=aio_pika.abc.AbstractRobustChannel,
            default_exchange=mocker.AsyncMock(),
            is_closed=mocker.Mock(return_value=False),
            set_qos=mocker.AsyncMock(),
        )

    @pytest.fixture
    def mock_async_connection_manager(
        self,
        mocker: pytest_mock.MockFixture,
        mock_async_channel: aio_pika.abc.AbstractRobustChannel,
    ) -> typing.Any:
        return mocker.NonCallableMock(
            spec_set=manager.RabbitMQAsyncConnectionManager,
            get_channel=mocker.AsyncMock(
                spec_set=manager.RabbitMQAsyncConnectionManager.get_channel,
                side_effect=[mock_async_channel],
            ),
        )

    @pytest.fixture
    def mock_event_loop(self, mocker: pytest_mock.MockFixture) -> asyncio.AbstractEventLoop:
        return mocker.AsyncMock(spec=asyncio.AbstractEventLoop)

    @pytest.fixture(autouse=True)
    def patch_json_dumps(self, mocker: pytest_mock.MockFixture) -> typing.Any:
        return mocker.patch('json.dumps', return_value='"json-dumped message"')

    @pytest.fixture(autouse=True)
    def patch_async_connection_manager(
        self,
        mocker: pytest_mock.MockFixture,
        mock_async_connection_manager: manager.RabbitMQAsyncConnectionManager,
    ) -> typing.Any:
        return mocker.patch.object(
            manager,
            attribute='RabbitMQAsyncConnectionManager',
            side_effect=[mock_async_connection_manager],
        )

    @pytest.fixture
    def caplog(self, caplog: pytest.LogCaptureFixture) -> pytest.LogCaptureFixture:
        caplog.set_level(logging.DEBUG, logger=LOG_NAME)
        return caplog


class TestAsyncProducer(MockAsyncProducer):

    async def test_initialize(
        self,
        stub_mq_config: models.RabbitMQConfig,
        patch_async_connection_manager: unittest.mock.AsyncMock,
        mock_async_connection_manager: manager.RabbitMQAsyncConnectionManager,
        mocker: pytest_mock.MockFixture,
    ) -> None:
        mq_producer = producer.AsyncProducer(stub_mq_config)

        assert mq_producer.connection_manager is mock_async_connection_manager
        assert mq_producer.queue_name == 'test_queue'

        assert patch_async_connection_manager.call_args == mocker.call(stub_mq_config, None)

    async def test_initialize_with_loop(
        self,
        stub_mq_config: models.RabbitMQConfig,
        patch_async_connection_manager: unittest.mock.AsyncMock,
        mock_event_loop: asyncio.AbstractEventLoop,
        mock_async_connection_manager: manager.RabbitMQAsyncConnectionManager,
        mocker: pytest_mock.MockFixture,
    ) -> None:
        mq_producer = producer.AsyncProducer(stub_mq_config, mock_event_loop)

        assert mq_producer.connection_manager is mock_async_connection_manager
        assert mq_producer.queue_name == 'test_queue'

        assert patch_async_connection_manager.call_args == mocker.call(stub_mq_config, mock_event_loop)

    async def test_publish(
        self,
        stub_message: dict[str, typing.Any],
        stub_mq_producer: producer.AsyncProducer,
        mock_async_connection_manager: manager.RabbitMQAsyncConnectionManager,
        mock_async_channel: aio_pika.abc.AbstractRobustChannel,
        mocker: pytest_mock.MockFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        await stub_mq_producer.publish(stub_message)

        assert caplog.record_tuples == [
            (LOG_NAME, logging.DEBUG, "Event published to RabbitMQ with routing key test_queue: {'foo': 'bar'}"),
        ]

        assert mock_async_connection_manager.get_channel.call_args == mocker.call()

        assert mock_async_channel.default_exchange.publish.call_args.args[0].body == b'"json-dumped message"'
        assert mock_async_channel.default_exchange.publish.call_args.kwargs['routing_key'] == 'test_queue'

    async def test_publish_routing_key(
        self,
        stub_message: dict[str, typing.Any],
        stub_mq_producer: producer.AsyncProducer,
        mock_async_channel: aio_pika.abc.AbstractRobustChannel,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        await stub_mq_producer.publish(stub_message, routing_key='spam')

        assert mock_async_channel.default_exchange.publish.call_args.kwargs['routing_key'] == 'spam'

        assert caplog.record_tuples == [
            (LOG_NAME, logging.DEBUG, "Event published to RabbitMQ with routing key spam: {'foo': 'bar'}"),
        ]

    async def test_publish_get_channel_error(
        self,
        stub_message: dict[str, typing.Any],
        stub_mq_producer: producer.AsyncProducer,
        mock_async_connection_manager: manager.RabbitMQAsyncConnectionManager,
        mock_async_channel: aio_pika.abc.AbstractRobustChannel,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        get_channel_error = aio_pika.exceptions.AMQPError('some error')
        mock_async_connection_manager.get_channel.side_effect = [get_channel_error]

        await stub_mq_producer.publish(stub_message)

        assert not mock_async_channel.default_exchange.publish.called

        assert caplog.record_tuples == [
            (LOG_NAME, logging.ERROR, 'Failed to publish event to RabbitMQ: some error'),
        ]

    async def test_publish_get_channel_error_raise(
        self,
        stub_message: dict[str, typing.Any],
        stub_mq_producer: producer.AsyncProducer,
        mock_async_connection_manager: manager.RabbitMQAsyncConnectionManager,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        get_channel_error = aio_pika.exceptions.AMQPError('some error')
        mock_async_connection_manager.get_channel.side_effect = [get_channel_error]

        with pytest.raises(aio_pika.exceptions.AMQPError) as exc_info:
            await stub_mq_producer.publish(stub_message, raise_err=True)

        assert exc_info.value is get_channel_error
        assert caplog.record_tuples == [
            (LOG_NAME, logging.ERROR, 'Failed to publish event to RabbitMQ: some error'),
        ]

    async def test_publish_error(
        self,
        stub_message: dict[str, typing.Any],
        stub_mq_producer: producer.AsyncProducer,
        mock_async_channel: aio_pika.abc.AbstractRobustChannel,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        publish_error = aio_pika.exceptions.AMQPError('some error')
        mock_async_channel.default_exchange.publish.side_effect = [publish_error]

        await stub_mq_producer.publish(stub_message)

        assert caplog.record_tuples == [
            (LOG_NAME, logging.ERROR, 'Failed to publish event to RabbitMQ: some error'),
        ]

    async def test_publish_error_raise(
        self,
        stub_message: dict[str, typing.Any],
        stub_mq_producer: producer.AsyncProducer,
        mock_async_channel: aio_pika.abc.AbstractRobustChannel,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        publish_error = aio_pika.exceptions.AMQPError('some error')
        mock_async_channel.default_exchange.publish.side_effect = [publish_error]

        with pytest.raises(aio_pika.exceptions.AMQPError) as exc_info:
            await stub_mq_producer.publish(stub_message, raise_err=True)

        assert exc_info.value is publish_error
        assert caplog.record_tuples == [
            (LOG_NAME, logging.ERROR, 'Failed to publish event to RabbitMQ: some error'),
        ]

    async def test_publish_unknown_error(
        self,
        stub_message: dict[str, typing.Any],
        stub_mq_producer: producer.AsyncProducer,
        mock_async_channel: aio_pika.abc.AbstractRobustChannel,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        unknown_error = ValueError('foo')
        mock_async_channel.default_exchange.publish.side_effect = [unknown_error]

        with pytest.raises(ValueError, match='foo') as exc_info:
            await stub_mq_producer.publish(stub_message)

        assert exc_info.value is unknown_error
        assert caplog.record_tuples == [
            (LOG_NAME, logging.CRITICAL, 'Unexpected error: foo'),
        ]
