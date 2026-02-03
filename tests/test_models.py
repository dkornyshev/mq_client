import functools

import pydantic
import pytest

from mq_client import models


class TestRabbitMQConfig:

    @pytest.fixture
    def stub_mq_config(self) -> functools.partial[models.RabbitMQConfig]:
        return functools.partial(
            models.RabbitMQConfig,
            broker_dns='amqp://user:pass@mq.host:5672',
            queue_name='test_queue',
            prefetch_limit=1,
            max_reconnect_attempts=5,
            reconnect_delay=1,
        )

    def test_model(self, stub_mq_config: models.RabbitMQConfig) -> None:
        model = models.RabbitMQConfig(
            broker_dns='amqp://user:pass@mq.host:5672',
            queue_name='test_queue',
            prefetch_limit=1,
            max_reconnect_attempts=5,
            reconnect_delay=1,
        )

        assert model.broker_dns == 'amqp://user:pass@mq.host:5672/'
        assert model.queue_name == 'test_queue'
        assert model.prefetch_limit == 1
        assert model.max_reconnect_attempts == 5
        assert model.reconnect_delay == 1

    @pytest.mark.parametrize(('broker_dsn', 'validated_broker_dsn'), [
        ('amqp://user:pass@mq.host:5672', 'amqp://user:pass@mq.host:5672/'),
        ('amqp://user:pass@mq.host', 'amqp://user:pass@mq.host:5672/'),
    ])
    def test_broker_dsn(
        self,
        stub_mq_config: functools.partial[models.RabbitMQConfig],
        broker_dsn: str,
        validated_broker_dsn: str,
    ) -> None:
        mq_config = stub_mq_config(brocker_dns=broker_dsn)

        assert mq_config.broker_dns == validated_broker_dsn  # noqa: SIM300 (yoda-conditions)

    @pytest.mark.parametrize('broker_dsn', [
        'amqp://user@mq.host:5672/',  # no password
        'amqp://:pass@mq.host:5672/',  # no user
        'amqp://user:pass@/',  # no host
    ])
    def test_broker_dsn_invalid(
        self,
        stub_mq_config: functools.partial[models.RabbitMQConfig],
        broker_dsn: str,
    ) -> None:
        with pytest.raises(pydantic.ValidationError, match='broker_dns'):
            stub_mq_config(broker_dns=broker_dsn)
