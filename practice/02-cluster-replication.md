### Урок

> Шаг 1.
> Почему лидеры разные?

Лидеры разные, чтобы распределить нагрузку.

> Шаг 2. Остановите брокер 2
> Предскажите: у каких партиций сменится лидер, что станет с Replicas, что — с Isr?

Предсказание:
Сменится лидер у Partition 1. Лидер будет не 2, а 1 или 3
Replicas будет 1,2,3 в разном порядке, не знаю в каком
- у partition 1 не могу предсказать
Isr будет 1,3 в разном порядке

Факт:
Replicas не изменилось. Isr стало 1,3 в разном порядке. Первая цифра - лидел

> Шаг 3. Запишите в sandbox, пока брокер 2 стоит.
> Консольный продюсер по умолчанию использует acks=all. Пройдёт ли запись?

Запись пройдёт потому что min.insync.replicas=1
А у каждой партиции есть лидер (т.е. как минимум 1 реплика)

Ошибки не было

> Шаг 4.
> Предскажите результат. Ошибка может появиться через несколько секунд

Тут сложно не догадаться. Правильный ответ уже находится в задании.
У нас только 2 доступных брокера: не получится сделать так, чтобы запись была скопирована на 3 реплики

[2026-11-22 20:14:19,380] WARN [Producer clientId=console-producer] Got error produce response with correlation id 5 on topic-partition sandbox-0, retrying (2 attempts left). Error: NOT_ENOUGH_REPLICAS (org.apache.kafka.clients.producer.internals.Sender)
[2026-11-22 20:14:19,483] WARN [Producer clientId=console-producer] Got error produce response with correlation id 6 on topic-partition sandbox-0, retrying (1 attempts left). Error: NOT_ENOUGH_REPLICAS (org.apache.kafka.clients.producer.internals.Sender)
[2026-11-22 20:14:19,715] WARN [Producer clientId=console-producer] Got error produce response with correlation id 7 on topic-partition sandbox-0, retrying (0 attempts left). Error: NOT_ENOUGH_REPLICAS (org.apache.kafka.clients.producer.internals.Sender)
[2026-11-22 20:14:20,132] ERROR Error when sending message to topic sandbox with key: 5 bytes, value: 9 bytes with error: (org.apache.kafka.clients.producer.internals.ErrorLoggingCallback)
org.apache.kafka.common.errors.NotEnoughReplicasException: Messages are rejected since there are fewer in-sync replicas than required.

> Шаг 5
> Вернулся ли брокер 2 в Isr? Стал ли он снова лидером своей партиции?

Он мог бы стать лидером для Partition 2.
--
Мог бы, но не стал. kafka3 всё ещё лидер двух партиций.
--
А нет! Стал =)

```bash
docker compose exec -T kafka1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka1:9092 \
  --describe --entity-type brokers --entity-name 1 --all | grep auto.leader.rebalance.enable
```

Результат:

> auto.leader.rebalance.enable=true sensitive=false synonyms={DEFAULT_CONFIG:auto.leader.rebalance.enable=true}

```bash
docker compose exec -T kafka1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka1:9092 \
  --describe --entity-type brokers --entity-name 1 --all | grep leader.imbalance.check.interval.seconds
```

Результат:

> leader.imbalance.check.interval.seconds=300 sensitive=false synonyms={DEFAULT_CONFIG:leader.imbalance.check.interval.seconds=300}

### Ответы на вопросы самопроверки

1. При RF=3, minISR=2, ISR={1,2,3} гарантировано существует 3 копии.
        min.insync.replicas влияет на приём: оно показывает сколько минимально должно существовать синхронизированных реплик.
2. Брокеры 1 и 2 примут запись при minISR <= 2, будет 2 копии
3. minISR=3 при RF=3 плохо потому что при выпадении одного из брокеров запись не будет осуществляться
        minISR=1 рискует тем, что будет только одна копия, а при выпадении этого брокера сразу после записи эти данные пропадут (потому что сменится лидер)
        Поправка:
            Лидер выбирается только среди ISR !
            Поэтому лидер не сможет быть выбран при падении сразу же, в случае когда minISR=1
                это если он сообщил об успехе продьюсеру и упал
                партиция при этом не будет доступна ни на чтение, ни на запись
4. Я хочу ответить на вопрос "да". Но в этом случае будет отброшен хвост упавшего лидера.
        Если бы ответ был "нет", то тогда нельзя было ничего записать в эту партицию!
        Поправка:
            Если другой фолловер в ISR то она станет лидером
            А если и она не в ISR, то лидер не сможет быть избран. Будет потеря времени, а не данных.
5. Кластер может выбирать лидера и с 1 и с 2 нодами. Для выборов лидера нужно большинство.
        Удобнее, когда нод нечётное количество более или равно 3. Но и 1 нода - это лидер. А с двумя нодами нужно чтобы обе согласились с выбором лидера.
        Поправка:
            «Большинство» считается не от живых узлов, а от списка голосующих!
            А значит максимальное количество упавших узлов = 1
6. Адрес брокера 2 - это localhost:29092, kafka2:9092 - это внутренний адрес во внутренней подсети.
        Должен возвращаться EXTERNAL адрес, чтобы можно было обратиться извне

### Получившиеся topics

```
$ ./hw.sh topics
Topic: events.dlt       TopicId: ish-4oURRwSMm5C5ua_XCA PartitionCount: 3       ReplicationFactor: 3 Configs: min.insync.replicas=1,unclean.leader.election.enable=false
        Topic: events.dlt       Partition: 0    Leader: 1       Replicas: 1,2,3 Isr: 1,2,3      Elr:  LastKnownElr: 
        Topic: events.dlt       Partition: 1    Leader: 2       Replicas: 2,3,1 Isr: 2,3,1      Elr:  LastKnownElr: 
        Topic: events.dlt       Partition: 2    Leader: 3       Replicas: 3,1,2 Isr: 3,1,2      Elr:  LastKnownElr: 
Topic: events   TopicId: R5ZrNmNTSgy7MB5guCiY5w PartitionCount: 3       ReplicationFactor: 3    Configs: min.insync.replicas=2,unclean.leader.election.enable=false
        Topic: events   Partition: 0    Leader: 2       Replicas: 2,3,1 Isr: 2,3,1      Elr:    LastKnownElr: 
        Topic: events   Partition: 1    Leader: 3       Replicas: 3,1,2 Isr: 3,1,2      Elr:    LastKnownElr: 
        Topic: events   Partition: 2    Leader: 1       Replicas: 1,2,3 Isr: 1,2,3      Elr:    LastKnownElr: 
```
