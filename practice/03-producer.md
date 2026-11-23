### Урок

Справочник конфигурации

* https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
* Там есть значения по умолчанию

| ключ | значение | тип |
|---|---|---|---|
| request.required.acks | -1 | integer |
| enable.idempotence | false | boolean |
| message.timeout.ms | 300000 | integer |
| linger.ms | 5 | integer |
| max.in.flight | 1000000 | integer |

produced, delivered, failed и in_queue

> Шаг 2. Сделайте так, чтобы брокер отказывал в записи: minISR=3 и один брокер остановлен.
> Предскажите, как через 30 секунд изменятся produced, delivered, failed и in_queue.

Предсказание:
- produced должно увеличиваться; delivered - оставаться прежним
- failed должно начать расти вместо delivered после 300000 ms (`message.timeout.ms`) ???
    - обновление: ещё, наверное, если in_queue будет больше `max.in.flight`, то будет ошибка (но я не воспроизведу)
    - обновление: так и случилось !
- in_queue (а это `len(producer)`) должно дорасти до `max.in.flight` ???

> Шаг 3. Остановите продюсер.
> Предскажите, что будет в producer.stopped и что напишет librdkafka.

Первая запись была такая:
```
producer-1-1  | {"time": "2026-11-23T18:43:20.753+00:00", "event": "producer.started", "topic": "events", "key_mode": "user_id", "events_per_sec": 10.0, "users": ["p1-1d1483-u000", "p1-1d1483-u001", "p1-1d1483-u002", "..."]}
```

Наверное после останова будет тоже самое, кроме `"event": "producer.stopped"`

Нет. В логах было вот что:

```
producer-1-1  | {"time": "2026-11-23T18:58:55.929+00:00", "event": "producer.stopped", "produced": 9323, "delivered": 4947, "failed": 1380, "in_queue": 2996}
producer-1-1  | %4|1795460335.929|TERMINATE|producer-1#producer-1| [thrd:app]: Producer terminating with 2996 messages (647710 bytes) still in queue or transit: use flush() to wait for outstanding message delivery
```

Было потеряно **2996** сообщений. Про это никто не узнал бы. Я не знаю куда их можно сохранить!

> Шаг 5 — после того как напишете код. 

```
producer-1-1  | {"time": "2026-11-23T21:38:03.353+00:00", "event": "producer.stats", "produced": 401, "delivered": 228, "failed": 0, "in_queue": 173}
producer-1-1  | {"time": "2026-11-23T21:38:04.081+00:00", "event": "signal.received", "signal": "SIGTERM"}
producer-1-1  | {"time": "2026-11-23T21:38:24.082+00:00", "event": "producer.stopped", "produced": 408, "delivered": 228, "failed": 0, "in_queue": 180}
producer-1-1  | %4|1795469904.082|TERMINATE|producer-1#producer-1| [thrd:app]: Producer terminating with 180 messages (38645 bytes) still in queue or transit: use flush() to wait for outstanding message delivery
producer-1-1 exited with code 0
```

### Ответы на вопросы самопроверки

1. produce() без исключения означает, что сообщение поставлено в очередь
2. poll(0) нужен, чтобы вызывались callback-и; без них сообщение ушло в очередь и о нем больше ничего не будет известно
3. после потери в партиции будет следующая пачка. скорее всего придёт и потерянная, но она придёт уже позже
        обновление: если продьюсер не получит ответ, то он отправил сообщение заново
        обновление: идемпотентность не даст выстать уже отправленное сообщение!
4. если возникнет ошибка, то сообщение может быть обработано позже (когда продьюсер его пошлёт заново). seq должен быть возрастающим
        обновление: seq не должно дублироваться
        обновление-2: count_deduplicated посчитает если сначала придёт 5,6 потом 7,8, а потом опять 5,6
5. от повторной обработки консьюмером. и после перезапуска продьюсера он получает новый id (что будет с момента остановки и момента старта - я не знаю)
6. я тут не совсем понял. через 5 минут начинает увеличиваться failed, а len(producer) остаётся примерно на одном уровне. через 10 минут будет тоже самое, что и через 5 и 6
        обновление-2: при 15 секундах в message.timeout.md будет чуть менее 15*10 = 150 штук
7. значение параметра метода flush должно быть меньше или равно stop_grace_period. иначе flush не дождётся и не запишет в логи
        обновление: producer.stopped вообще не появится (потому что процесс будет убит до того)
8. если produce_with_backpressure не возвращает true, то producer останавливается. это потеря
        обновление: после изменения on_delivery - увидит в логах!
        обновление-2: verify не увидит seq после обработки BufferError, будет пропуск. а если seq последний, то пропуск заметен не будет
