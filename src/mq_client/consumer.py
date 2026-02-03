"""Потребитель сообщений из RabbitMQ."""
import json
import logging
import typing

import aio_pika
import aio_pika.exceptions

from mq_client import manager
from mq_client import models


logger = logging.getLogger(__name__)


class AsyncConsumer:
    """Потребитель (consumer) сообщений из шины RabbitMQ."""

    def __init__(self, mq_config: models.RabbitMQConfig) -> None:
        """Создать экземпляр потребителя сообщений."""
        self.connection_manager =  manager.RabbitMQAsyncConnectionManager(mq_config)
        self.queue_name = mq_config.queue_name

        self._consuming = True

    async def start_consuming(
        self,
        callback: typing.Callable[[dict[str, typing.Any]], typing.Awaitable[None]],
    ) -> None:
        """Начать потребление сообщений из очереди RabbitMQ.

        Args:
            callback: Асинхронная функция для обработки сообщений
        """
        while self._consuming:
            try:
                channel = await self.connection_manager.get_channel()
                queue = await channel.get_queue(self.queue_name)
                async with queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        await self._process_message(message, callback)

            except aio_pika.exceptions.AMQPError as e:
                logger.error(f'Failed to consume events from RabbitMQ: {e}')

            except Exception as e:
                logger.critical(f'Unexpected error: {e}')

    async def _process_message(
        self,
        message: aio_pika.abc.AbstractIncomingMessage,
        callback: typing.Callable[[dict[str, typing.Any]], typing.Awaitable[None]],
    ) -> None:
        try:
            async with message.process():
                body = message.body.decode()
                msg_data = json.loads(body)

                await callback(msg_data)

        except json.JSONDecodeError as e:
            logger.error(f'Failed to decode message: {e}')

        except Exception as e:
            logger.error(f'Error processing message: {e}')

    async def stop_consuming(self) -> None:
        """Остановить потребление сообщений из очереди RabbitMQ."""
        self._consuming = False
        await self.connection_manager.disconnect()
        logger.info('Consumption has been stopped')
