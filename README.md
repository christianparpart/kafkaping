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
Received from localhost topic kafkaping partition 0 offset 144 payload 228ms
Received from localhost topic kafkaping partition 0 offset 145 payload 15ms
Received from localhost topic kafkaping partition 0 offset 146 payload 18ms
Received from localhost topic kafkaping partition 0 offset 147 payload 19ms
```
