# kafkaping

kafkaping is like IP ping, but for Kafka. To be precise, it's producing
into a broker's topic, and consuming, measuring the latency between them.

### Usage

```
kafkaping [-g] [-t topic] [-c count] [-i interval_ms] [broker list]

  -g        Enables debugging prints
  -t TOPIC  What Kafka topic to use for producing/consuming messages [kafkaping]
  -c COUNT  Number of messages to send & receive before quitting [unlimited]
  -i MSECS  time to wait between to pings
```

### Example

```sh
# kafkaping -c4 localhost
Received from localhost: topic=kafkaping partition=0 offset=307 time=247 ms
Received from localhost: topic=kafkaping partition=0 offset=308 time=17 ms
Received from localhost: topic=kafkaping partition=0 offset=309 time=14 ms
Received from localhost: topic=kafkaping partition=0 offset=310 time=14 ms
```
