"""Менеджер соединений с RabbitMQ."""
import asyncio
import logging

import aio_pika
import aio_pika.abc
import aio_pika.exceptions

from mq_client import models


logger = logging.getLogger(__name__)


class RabbitMQAsyncConnectionManager:
    """Менеджер соединений с RabbitMQ."""

    def __init__(self, mq_config: models.RabbitMQConfig) -> None:
        """Создать экземпляр менеджера соединений."""
        self.mq_config = mq_config

        self.connection: aio_pika.abc.AbstractRobustConnection | None = None
        self.channel: aio_pika.abc.AbstractRobustChannel | None = None

    async def connect(self) -> None:
        """Установить соединение с RabbitMQ."""
        attempt = 0
        last_exception = None

        while attempt < self.mq_config.max_reconnect_attempts:
            try:
                self.connection = await aio_pika.connect_robust(self.mq_config.broker_dsn)
                self.channel = await self.connection.channel()
                await self.channel.set_qos(prefetch_count=self.mq_config.prefetch_limit)
                await self.channel.declare_queue(self.mq_config.queue_name, durable=True)
                logger.info('Successfully connected to RabbitMQ')
                return

            except aio_pika.exceptions.AMQPConnectionError as e:
                attempt += 1
                last_exception = e

                delay = self.mq_config.reconnect_delay * (2 ** (attempt - 1))
                logger.error(f'AMQP Connection Error: {e}. Retrying in {delay} seconds...')
                await asyncio.sleep(delay)

            except Exception as e:
                logger.critical(f'Unexpected error: {e}')
                raise

        logger.critical('Max reconnection attempts reached. Failed to connect to RabbitMQ.')
        if last_exception is not None:
            raise last_exception

    async def disconnect(self) -> None:
        """Закрыть соединение с RabbitMQ."""
        if self.connection:
            await self.connection.close()
            logger.info('RabbitMQ connection closed')

    async def get_channel(self) -> aio_pika.abc.AbstractRobustChannel:
        """Получить канал соединения с RabbitMQ.

        Returns:
            Канал соединения
        """
        if self.channel is None or self.channel.is_closed:
            await self.connect()

        return self.channel
