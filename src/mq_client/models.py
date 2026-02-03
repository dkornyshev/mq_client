"""Модели клиента взаимодействия с шиной данных RabbitMQ."""
import typing

import pydantic


StrongAmqpDsn: typing.TypeAlias = typing.Annotated[
    pydantic.AmqpDsn,
    pydantic.UrlConstraints(
        host_required=True,
        default_port=5672,
        default_path='/',
    ),
]


def validate_amqp_dns(amqp_dsn_string: str) -> str:
    amqp_dsn_adapter = pydantic.TypeAdapter(StrongAmqpDsn)
    amqp_dsn: pydantic.AmqpDsn = amqp_dsn_adapter.validate_python(amqp_dsn_string)

    if not amqp_dsn.username:
        error_msg = f'username is required in {amqp_dsn_string}'
        raise ValueError(error_msg)
    if not amqp_dsn.password:
        error_msg = f'password is required in {amqp_dsn_string}'
        raise ValueError(error_msg)

    return str(amqp_dsn)


class RabbitMQConfig(pydantic.BaseModel):
    """Модель конфигурации брокера сообщений."""

    broker_dns: typing.Annotated[str, pydantic.AfterValidator(validate_amqp_dns)]  #: Строка подключения к брокеру MQ
    queue_name: str  #: Имя очереди сообщений
    prefetch_limit: int  #: Ограничение предварительной выборки
    max_reconnect_attempts: int  #: Максимальное число попыток подключения
    reconnect_delay: int  #: Начальная задержка в секундах
