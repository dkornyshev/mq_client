import logging
import typing
import unittest.mock

import aio_pika
import aio_pika.abc
import pytest
import pytest_mock

from mq_client import manager
from mq_client import models


LOG_NAME = 'mq_client.manager'


class MockRabbitMQAsyncConnectionManager:

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
    def mock_async_channel(self, mocker: pytest_mock.MockFixture) -> typing.Any:
        return mocker.NonCallableMock(
            spec=aio_pika.abc.AbstractRobustChannel,
            is_closed=mocker.Mock(return_value=False),
            declare_queue=mocker.AsyncMock(),
            set_qos=mocker.AsyncMock(),
        )

    @pytest.fixture
    def mock_async_connection(
        self,
        mocker: pytest_mock.MockFixture,
        mock_async_channel: aio_pika.abc.AbstractRobustChannel,
    ) -> typing.Any:
        return mocker.AsyncMock(
            spec=aio_pika.abc.AbstractRobustConnection,
            channel=mocker.AsyncMock(
                spec=aio_pika.abc.AbstractChannel,
                return_value=mock_async_channel,
            ),
        )

    @pytest.fixture
    def patch_connect_robust(
        self,
        mocker: pytest_mock.MockFixture,
        mock_async_connection: aio_pika.abc.AbstractRobustConnection,
    ) -> typing.Any:
        return mocker.patch(
            'aio_pika.connect_robust',
            new=mocker.AsyncMock(
                spec=aio_pika.connect_robust,
            ),
        )

    @pytest.fixture(autouse=True)
    def patch_sleep(self, mocker: pytest_mock.MockFixture) -> typing.Any:
        return mocker.patch('asyncio.sleep', new=mocker.AsyncMock())

    @pytest.fixture
    def caplog(self, caplog: pytest.LogCaptureFixture) -> pytest.LogCaptureFixture:
        caplog.set_level(logging.DEBUG, logger=LOG_NAME)
        return caplog


class TestRabbitMQAsyncConnectionManager(MockRabbitMQAsyncConnectionManager):

    def test_manager(self, stub_mq_config: models.RabbitMQConfig) -> None:
        mq_manager = manager.RabbitMQAsyncConnectionManager(stub_mq_config)

        assert mq_manager.connection is None
        assert mq_manager.channel is None

    async def test_connect(
        self,
        stub_mq_config: models.RabbitMQConfig,
        patch_connect_robust: unittest.mock.AsyncMock,
        mock_async_connection: aio_pika.abc.AbstractRobustConnection,
        mock_async_channel: aio_pika.abc.AbstractRobustChannel,
        mocker: pytest_mock.MockFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        patch_connect_robust.return_value = mock_async_connection

        mq_manager = manager.RabbitMQAsyncConnectionManager(stub_mq_config)
        await mq_manager.connect()

        assert isinstance(mq_manager.connection, aio_pika.abc.AbstractRobustConnection)
        assert isinstance(mq_manager.channel, aio_pika.abc.AbstractRobustChannel)

        assert caplog.record_tuples == [
            (LOG_NAME, logging.INFO, 'Successfully connected to RabbitMQ'),
        ]

        assert patch_connect_robust.call_args == mocker.call('amqp://foo:bar@test:1234/')
        assert mock_async_connection.channel.call_args == mocker.call()
        assert mock_async_channel.set_qos.call_args == mocker.call(prefetch_count=1)
        assert mock_async_channel.declare_queue.call_args == mocker.call('test_queue', durable=True)

    async def test_connect_retry(
        self,
        stub_mq_config: models.RabbitMQConfig,
        patch_connect_robust: unittest.mock.AsyncMock,
        patch_sleep: unittest.mock.AsyncMock,
        mock_async_connection: aio_pika.abc.AbstractRobustConnection,
        mocker: pytest_mock.MockFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        stub_mq_config.max_reconnect_attempts = 4
        stub_mq_config.reconnect_delay = 2

        patch_connect_robust.side_effect = [
            aio_pika.exceptions.AMQPConnectionError('some error'),  # попытка 1
            aio_pika.exceptions.AMQPConnectionError('another error'),  # попытка 2
            aio_pika.exceptions.AMQPConnectionError('one more error'),  # попытка 3
            mock_async_connection,  # попытка 4
        ]

        mq_manager = manager.RabbitMQAsyncConnectionManager(stub_mq_config)
        await mq_manager.connect()

        assert mq_manager.connection is not None
        assert mq_manager.channel is not None

        assert caplog.record_tuples == [
            (LOG_NAME, logging.ERROR, 'AMQP Connection Error: some error. Retrying in 2 seconds...'),
            (LOG_NAME, logging.ERROR, 'AMQP Connection Error: another error. Retrying in 4 seconds...'),
            (LOG_NAME, logging.ERROR, 'AMQP Connection Error: one more error. Retrying in 8 seconds...'),
            (LOG_NAME, logging.INFO, 'Successfully connected to RabbitMQ'),
        ]

        assert patch_sleep.call_args_list == [
            mocker.call(2),
            mocker.call(4),
            mocker.call(8),
        ]

    async def test_connect_failed(
        self,
        stub_mq_config: models.RabbitMQConfig,
        patch_connect_robust: unittest.mock.AsyncMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        stub_mq_config.max_reconnect_attempts = 2
        stub_mq_config.reconnect_delay = 2

        exceptions = [
            aio_pika.exceptions.AMQPConnectionError('some error'),  # попытка 1
            aio_pika.exceptions.AMQPConnectionError('we instantly crash'),  # попытка 2
        ]
        patch_connect_robust.side_effect = list(exceptions)

        mq_manager = manager.RabbitMQAsyncConnectionManager(stub_mq_config)
        with pytest.raises(aio_pika.exceptions.AMQPConnectionError) as exc_info:
            await mq_manager.connect()

        exc_value = exc_info.value
        assert exc_value is exceptions[-1]

        assert caplog.record_tuples == [
            (LOG_NAME, logging.ERROR, 'AMQP Connection Error: some error. Retrying in 2 seconds...'),
            (LOG_NAME, logging.ERROR, 'AMQP Connection Error: we instantly crash. Retrying in 4 seconds...'),
            (LOG_NAME, logging.CRITICAL, 'Max reconnection attempts reached. Failed to connect to RabbitMQ.'),
        ]

    async def test_connect_unknown_error(
        self,
        stub_mq_config: models.RabbitMQConfig,
        patch_connect_robust: unittest.mock.AsyncMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        stub_mq_config.max_reconnect_attempts = 2
        stub_mq_config.reconnect_delay = 2

        unknown_exception = ValueError('foo')
        patch_connect_robust.side_effect = [unknown_exception]

        mq_manager = manager.RabbitMQAsyncConnectionManager(stub_mq_config)
        with pytest.raises(ValueError, match='foo') as exc_info:
            await mq_manager.connect()

        exc_value = exc_info.value
        assert exc_value is unknown_exception

        assert caplog.record_tuples == [
            (LOG_NAME, logging.CRITICAL, 'Unexpected error: foo'),
        ]

    async def test_disconnect(
        self,
        stub_mq_config: models.RabbitMQConfig,
        mock_async_connection: aio_pika.abc.AbstractRobustConnection,
        mocker: pytest_mock.MockFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        mq_manager = manager.RabbitMQAsyncConnectionManager(stub_mq_config)
        mq_manager.connection = mock_async_connection

        await mq_manager.disconnect()

        assert mock_async_connection.close.call_args == mocker.call()

        assert caplog.record_tuples == [
            (LOG_NAME, logging.INFO, 'RabbitMQ connection closed'),
        ]

    async def test_disconnect_no_connection(
        self,
        stub_mq_config: models.RabbitMQConfig,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        mq_manager = manager.RabbitMQAsyncConnectionManager(stub_mq_config)
        mq_manager.connection = None

        await mq_manager.disconnect()

        assert caplog.record_tuples == []

    async def test_get_channel(
        self,
        stub_mq_config: models.RabbitMQConfig,
        patch_connect_robust: unittest.mock.AsyncMock,
        mock_async_connection: aio_pika.abc.AbstractRobustConnection,
        mock_async_channel: aio_pika.abc.AbstractRobustChannel,
    ) -> None:
        mq_manager = manager.RabbitMQAsyncConnectionManager(stub_mq_config)
        mq_manager.channel = mock_async_channel

        patch_connect_robust.return_value = mock_async_connection

        mq_channel = await mq_manager.get_channel()

        assert mq_channel is mock_async_channel

    async def test_get_channel_no_channel(
        self,
        stub_mq_config: models.RabbitMQConfig,
        patch_connect_robust: unittest.mock.AsyncMock,
        mock_async_connection: aio_pika.abc.AbstractRobustConnection,
    ) -> None:
        mq_manager = manager.RabbitMQAsyncConnectionManager(stub_mq_config)
        mq_manager.connection = mock_async_connection
        mq_manager.channel = None

        mq_channel = await mq_manager.get_channel()

        assert mq_manager.channel is not None
        assert mq_manager.channel is mq_channel
        assert mq_manager.connection is not mock_async_connection
        assert patch_connect_robust.called

    async def test_get_channel_closed(
        self,
        stub_mq_config: models.RabbitMQConfig,
        patch_connect_robust: unittest.mock.AsyncMock,
        mock_async_connection: aio_pika.abc.AbstractRobustConnection,
        mock_async_channel: aio_pika.abc.AbstractRobustChannel,
    ) -> None:
        mock_async_channel.is_closed.return_value = True

        mq_manager = manager.RabbitMQAsyncConnectionManager(stub_mq_config)
        manager.connection = mock_async_connection
        manager.channel = mock_async_channel

        mq_channel = await mq_manager.get_channel()

        assert mq_channel is not mock_async_channel
        assert mq_manager.channel is mq_channel
        assert mq_manager.connection is not mock_async_connection
        assert patch_connect_robust.called
