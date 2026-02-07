"""Производитель сообщений и публикатор в RabbitMQ."""
import asyncio
import json
import logging
import typing

import aio_pika
import aio_pika.exceptions

from mq_client import manager
from mq_client import models


logger = logging.getLogger(__name__)


class AsyncProducer:
    """Производитель (producer) сообщений в шину RabbitMQ."""

    def __init__(
        self,
        mq_config: models.RabbitMQConfig,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        """Создать экземпляр производителя сообщений."""
        self.connection_manager = manager.RabbitMQAsyncConnectionManager(mq_config, loop)
        self.queue_name = mq_config.queue_name

    async def publish(
        self,
        msg_data: dict[str, typing.Any],
        *,
        routing_key: str | None = None,
        raise_err: bool = False,
    ) -> None:
        """Отправить сообщение в шину данных.

        Raises:
            AMQPError: При ошибках взаимодействия с RabbitMQ
        """
        if routing_key is None:
            routing_key = self.queue_name

        try:
            channel = await self.connection_manager.get_channel()
            msg_body = json.dumps(msg_data).encode()
            msg = aio_pika.Message(msg_body)
            await channel.default_exchange.publish(msg, routing_key=routing_key)

        except aio_pika.exceptions.AMQPError as e:
            logger.error(f'Failed to publish event to RabbitMQ: {e}')
            if raise_err:
                raise e

        except Exception as e:
            logger.critical(f'Unexpected error: {e}')
            raise

        else:
            logger.debug(f'Event published to RabbitMQ with routing key {routing_key}: {msg_data}')
