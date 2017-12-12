# kafkaping

kafkaping is like IP ping, but for Kafka. To be precise, it's producing
into a broker's topic, and consuming, measuring the latency between them.

### Usage

```
Usage: kafkaping [-g] [-t topic] [-c count] [-i interval_ms] [broker list]

  -g        Increases debugging print verbosity
  -t TOPIC  What Kafka topic to use for producing/consuming messages [kafkaping]
  -c COUNT  Number of messages to send & receive before quitting [unlimited]
  -i MSECS  Time to wait between to pings
```

### Example

```sh
# kafkaping -c4 localhost
Received from localhost: topic=kafkaping partition=0 offset=385 time=126 ms
Received from localhost: topic=kafkaping partition=0 offset=386 time=17 ms
Received from localhost: topic=kafkaping partition=0 offset=387 time=12 ms
Received from localhost: topic=kafkaping partition=0 offset=388 time=13 ms

--- localhost ping statistics ---
4 messages received, time 5222ms
rtt min/avg/max = 12/42/126 ms
```

### Build time dependencies
```sh
sudo apt install autoconf automake make g++ pkg-config librdkafka-dev
```

### How to build
```sh
./autogen.sh
./configure
make
sudo make install
```
