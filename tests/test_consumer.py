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
    def stub_mq_consumer(self, stub_mq_config: models.RabbitMQConfig) -> consumer.AsyncConsumer:
        return consumer.AsyncConsumer(stub_mq_config)

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

        return mocker.AsyncMock(
            spec=aio_pika.abc.AbstractQueue,
            iterator=mocker.PropertyMock(
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
        return mocker.AsyncMock(
            spec=aio_pika.abc.AbstractRobustChannel,
            is_closed=mocker.Mock(return_value=False),
            get_queue=mocker.AsyncMock(return_value=mock_async_queue),
        )

    @pytest.fixture
    def mock_async_connection(
        self,
        mock_async_channel: aio_pika.abc.AbstractRobustChannel,
        mocker: pytest_mock.MockFixture,
    ) -> typing.Any:
        return mocker.AsyncMock(
            spec=aio_pika.abc.AbstractRobustConnection,
            channel=mocker.Mock(return_value=mock_async_channel),
            is_closed=mocker.Mock(return_value=False),
            close=mocker.AsyncMock(),
        )


    @pytest.fixture
    def mock_async_connection_manager(
        self,
        mock_async_connection: aio_pika.abc.AbstractRobustConnection,
        mock_async_channel: aio_pika.abc.AbstractRobustChannel,
        mocker: pytest_mock.MockFixture,
    ) -> typing.Any:
        return mocker.AsyncMock(
            spec=manager.RabbitMQAsyncConnectionManager,
            connection=mock_async_connection,
            get_channel=mocker.AsyncMock(return_value=mock_async_channel),
        )

    @pytest.fixture(autouse=True)
    def patch_async_connection_manager_class(
        self,
        mock_async_connection_manager: manager.RabbitMQAsyncConnectionManager,
        mocker: pytest_mock.MockFixture,
    ) -> typing.Any:
        return mocker.patch(
            'mq_client.manager.RabbitMQAsyncConnectionManager',
            return_value=mock_async_connection_manager,
        )

    @pytest.fixture
    def patch_consuming_attribute(
        self,
        stub_mq_consumer: consumer.AsyncConsumer,
        mocker: pytest_mock.MockFixture,
    ) -> typing.Any:
        class _AlmostAlwaysTrue:
            def __init__(self, total_iterations: int = 1) -> None:
                self.total_iterations = total_iterations
                self.current_iteration = 0

            def __bool__(self) -> bool:
                if self.current_iteration < self.total_iterations:
                    self.current_iteration += 1
                    return bool(1)

                return bool(0)

        return mocker.patch.object(stub_mq_consumer, '_consuming', new=_AlmostAlwaysTrue(1))

    @pytest.fixture
    def caplog(self, caplog: pytest.LogCaptureFixture) -> pytest.LogCaptureFixture:
        caplog.set_level(logging.DEBUG, logger=LOG_NAME)
        return caplog


class TestAsyncConsumer(MockAsyncConsumer):

    async def test_consumer(self, stub_mq_consumer: consumer.AsyncConsumer) -> None:
        assert stub_mq_consumer.queue_name == 'test_queue'
        assert stub_mq_consumer.connection_manager is not None

    async def test_start_consuming_normal_flow(
        self,
        stub_mq_consumer: consumer.AsyncConsumer,
        patch_consuming_attribute: unittest.mock.MagicMock,
        mock_async_connection: aio_pika.abc.AbstractRobustConnection,
        mock_async_channel: aio_pika.abc.AbstractRobustChannel,
        mock_callback: unittest.mock.AsyncMock,
        mocker: pytest_mock.MockFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        await stub_mq_consumer.start_consuming(mock_callback)

        assert mock_async_channel.get_queue.call_args == mocker.call('test_queue')
        assert mock_callback.call_count == 1

    async def test_start_consuming_json_decode_error(
        self,
        stub_mq_consumer: consumer.AsyncConsumer,
        patch_consuming_attribute: unittest.mock.MagicMock,
        mock_message: aio_pika.abc.AbstractIncomingMessage,
        mock_callback: unittest.mock.AsyncMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        mock_message.body = b"invalid json"

        await stub_mq_consumer.start_consuming(mock_callback)

        assert caplog.record_tuples == [
            (LOG_NAME, logging.ERROR, 'Failed to decode message: Expecting value: line 1 column 1 (char 0)'),
        ]

        assert not mock_callback.called

    async def test_start_consuming_callback_error(
        self,
        stub_mq_consumer: consumer.AsyncConsumer,
        patch_consuming_attribute: unittest.mock.MagicMock,
        mock_message: aio_pika.abc.AbstractIncomingMessage,
        mock_callback: unittest.mock.AsyncMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        unknown_error = ValueError('some error')
        mock_callback.side_effect = [unknown_error]

        await stub_mq_consumer.start_consuming(mock_callback)

        assert caplog.record_tuples == [
            (LOG_NAME, logging.ERROR, 'Error processing message: some error'),
        ]

    async def test_start_consuming_get_channel_error(
        self,
        stub_mq_consumer: consumer.AsyncConsumer,
        patch_consuming_attribute: unittest.mock.MagicMock,
        mock_async_connection_manager: manager.RabbitMQAsyncConnectionManager,
        mock_callback: unittest.mock.AsyncMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        get_channel_error = aio_pika.exceptions.AMQPError('some error')
        mock_async_connection_manager.get_channel.side_effect = [get_channel_error]

        await stub_mq_consumer.start_consuming(mock_callback)

        assert caplog.record_tuples == [
            (LOG_NAME, logging.ERROR, 'Failed to consume events from RabbitMQ: some error'),
        ]

    @pytest.mark.asyncio
    async def test_start_consuming_unknown_error(
        self,
        stub_mq_consumer: consumer.AsyncConsumer,
        patch_consuming_attribute: unittest.mock.MagicMock,
        mock_async_channel: aio_pika.abc.AbstractRobustChannel,
        mock_callback: unittest.mock.AsyncMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        unknown_error = ValueError('some error')
        mock_async_channel.get_queue.side_effect = [unknown_error]

        await stub_mq_consumer.start_consuming(mock_callback)

        assert caplog.record_tuples == [
            (LOG_NAME, logging.CRITICAL, 'Unexpected error: some error'),
        ]

    async def test_start_consuming_message_process(
        self,
        stub_mq_consumer: consumer.AsyncConsumer,
        patch_consuming_attribute: unittest.mock.MagicMock,
        mock_message: aio_pika.abc.AbstractIncomingMessage,
        mock_callback: unittest.mock.AsyncMock,
    ) -> None:
        await stub_mq_consumer.start_consuming(mock_callback)

        assert mock_message.process.call_count == 1

    async def test_stop_consuming(
        self,
        stub_mq_consumer: consumer.AsyncConsumer,
        mock_async_connection_manager: manager.RabbitMQAsyncConnectionManager,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        assert stub_mq_consumer._consuming is True

        await stub_mq_consumer.stop_consuming()

        assert stub_mq_consumer._consuming is False

        assert caplog.record_tuples == [
            (LOG_NAME, logging.INFO, 'Consumption has been stopped'),
        ]

        assert mock_async_connection_manager.disconnect.call_count == 1
