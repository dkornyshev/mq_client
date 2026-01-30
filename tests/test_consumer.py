import json
import logging
import typing
import unittest.mock

import aio_pika
import aio_pika.abc
import aio_pika.exceptions
import pytest
import pytest_mock

from mq_client import consumer
from mq_client import manager
from mq_client import models


LOG_NAME = 'mq_client.consumer'


class MockAsyncConsumer:

    @pytest.fixture
    def stub_mq_config(self) -> models.RabbitMQConfig:
        return models.RabbitMQConfig(
            broker_dns='amqp://foo:bar@test:1234',
            queue_name='test_queue',
            prefetch_limit=1,
            max_reconnect_attempts=5,
            reconnect_delay=1,
        )

    @pytest.fixture
    def mock_callback(self, mocker: pytest_mock.MockFixture) -> typing.Any:
        return mocker.AsyncMock()

    @pytest.fixture
    def mock_message(self, mocker: pytest_mock.MockFixture) -> typing.Any:
        return mocker.AsyncMock(
            spec=aio_pika.abc.AbstractIncomingMessage,
            body=json.dumps({
                'connection_request_url': 'https://test',
                'cpe_id': 'test-cpe-123',
                'username': 'foo',
                'password': 'bar',
                'originator': 'user',
                'timestamp': '2025-12-12 12:12:12',
            }).encode(),
        )

    @pytest.fixture
    def mock_async_queue(
        self,
        mock_message: aio_pika.abc.AbstractIncomingMessage,
        mocker: pytest_mock.MockFixture,
    ) -> typing.Any:
        async def msg_generator():
            yield mock_message

        return mocker.NonCallableMock(
            spec=aio_pika.abc.AbstractQueue,
            iterator = mocker.PropertyMock(
                return_value=mocker.AsyncMock(
                    spec=aio_pika.abc.AbstractQueueIterator,
                    __aenter__=mocker.AsyncMock(return_value=msg_generator()),
                ),
            ),
        )

    @pytest.fixture
    def mock_async_channel(
        self,
        mock_async_queue: aio_pika.abc.AbstractQueue,
        mocker: pytest_mock.MockFixture,
    ) -> typing.Any:
        return mocker.NonCallableMock(
            spec=aio_pika.abc.AbstractRobustChannel,
            is_closed=mocker.Mock(return_value=False),
            get_queue=mocker.AsyncMock(return_value=mock_async_queue),
        )

    @pytest.fixture
    def mock_async_connection_manager(
        self,
        mock_async_channel: aio_pika.abc.AbstractRobustChannel,
        mocker: pytest_mock.MockFixture,
    ) -> typing.Any:
        return mocker.NonCallableMock(
            spec_set=manager.RabbitMQAsyncConnectionManager,
            get_channel=mocker.AsyncMock(
                spec_set=manager.RabbitMQAsyncConnectionManager.get_channel,
                side_effect=[mock_async_channel],
            ),
        )

    @pytest.fixture(autouse=True)
    def patch_async_connection_manager_class(
        self,
        mock_async_connection_manager: manager.RabbitMQAsyncConnectionManager,
        mocker: pytest_mock.MockFixture,
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


class TestAsyncConsumer(MockAsyncConsumer):

    async def test_consumer(self, stub_mq_config: models.RabbitMQConfig) -> None:
        mq_consumer = consumer.AsyncConsumer(stub_mq_config)

        assert mq_consumer.queue_name == 'test_queue'
        assert mq_consumer.connection_manager is not None

    async def test_start_consuming_normal_flow(
        self,
        stub_mq_config: models.RabbitMQConfig,
        mock_async_channel: aio_pika.abc.AbstractRobustChannel,
        mock_callback: unittest.mock.AsyncMock,
        mocker: pytest_mock.MockFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        mq_consumer = consumer.AsyncConsumer(stub_mq_config)
        await mq_consumer.start_consuming(mock_callback)

        assert mock_async_channel.get_queue.call_args == mocker.call('test_queue')
        assert mock_callback.call_count == 1

    async def test_start_consuming_json_decode_error(
        self,
        stub_mq_config: models.RabbitMQConfig,
        mock_message: aio_pika.abc.AbstractIncomingMessage,
        mock_callback: unittest.mock.AsyncMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        mock_message.body = b"invalid json"

        mq_consumer = consumer.AsyncConsumer(stub_mq_config)
        await mq_consumer.start_consuming(mock_callback)

        assert caplog.record_tuples == [
            (LOG_NAME, logging.ERROR, 'Failed to decode message: Expecting value: line 1 column 1 (char 0)'),
        ]

        assert not mock_callback.called

    async def test_start_consuming_callback_error(
        self,
        stub_mq_config: models.RabbitMQConfig,
        mock_message: aio_pika.abc.AbstractIncomingMessage,
        mock_callback: unittest.mock.AsyncMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        unknown_error = ValueError('foo')
        mock_callback.side_effect = [unknown_error]

        mq_consumer = consumer.AsyncConsumer(stub_mq_config)
        await mq_consumer.start_consuming(mock_callback)

        assert caplog.record_tuples == [
            (LOG_NAME, logging.ERROR, 'Error processing message: foo'),
        ]

    async def test_start_consuming_get_channel_error(
        self,
        stub_mq_config: models.RabbitMQConfig,
        mock_async_connection_manager: manager.RabbitMQAsyncConnectionManager,
        mock_callback: unittest.mock.AsyncMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        get_channel_error = aio_pika.exceptions.AMQPError('some error')
        mock_async_connection_manager.get_channel.side_effect = [get_channel_error]

        mq_consumer = consumer.AsyncConsumer(stub_mq_config)
        with pytest.raises(aio_pika.exceptions.AMQPError):
            await mq_consumer.start_consuming(mock_callback)

        assert caplog.record_tuples == [
            (LOG_NAME, logging.ERROR, 'Failed to consume events from RabbitMQ: some error'),
        ]

    @pytest.mark.asyncio
    async def test_start_consuming_unknown_error(
        self,
        stub_mq_config: models.RabbitMQConfig,
        mock_async_channel: aio_pika.abc.AbstractRobustChannel,
        mock_callback: unittest.mock.AsyncMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        unknown_error = ValueError('foo')
        mock_async_channel.get_queue.side_effect = [unknown_error]

        mq_consumer = consumer.AsyncConsumer(stub_mq_config)
        with pytest.raises(ValueError, match='foo') as exc_info:
            await mq_consumer.start_consuming(mock_callback)

        assert exc_info.value is unknown_error
        assert caplog.record_tuples == [
            (LOG_NAME, logging.CRITICAL, 'Unexpected error: foo'),
        ]

    async def test_start_consuming_message_process(
        self,
        stub_mq_config: models.RabbitMQConfig,
        mock_message: aio_pika.abc.AbstractIncomingMessage,
        mock_callback: unittest.mock.AsyncMock,
    ) -> None:
        mq_consumer = consumer.AsyncConsumer(stub_mq_config)
        await mq_consumer.start_consuming(mock_callback)

        assert mock_message.process.call_count == 1
