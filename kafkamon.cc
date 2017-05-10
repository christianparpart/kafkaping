// This file is part of the "kafkamon" project
//   <http://github.com/christianparpart/kafkamon>
//   (c) 2017 Christian Parpart <christian@parpart.family>
//
// Licensed under the MIT License (the "License"); you may not use this
// file except in compliance with the License. You may obtain a copy of
// the License at: http://opensource.org/licenses/MIT
#include <iostream>
#include <string>
#include <thread>
#include <cstdio>
#include <cinttypes>
#include <librdkafka/rdkafkacpp.h>
#include <getopt.h>
#include <unistd.h>

// TODO
// - [ ] expose replication lag (in ms)
// - [ ] expose number of hearbeats not flushed yet
// - [ ] expose number of hearbeats confirmed to be flushed
// - [ ] expose number of hearbeats consumed
// - [ ] expose failure-$type count
// - [ ] support partitioning

void dumpConfig(RdKafka::Conf* conf, const std::string& msg) {
  std::cout << "============================== " << msg << std::endl;
  auto d = conf->dump();
  for (auto i = d->begin(), e = d->end(); i != e;) {
    std::cout << *i++ << " = " << *i++ << std::endl;
  }
  std::cout << std::endl;
}

class Kafkamon : public RdKafka::EventCb,
                 public RdKafka::DeliveryReportCb,
                 public RdKafka::ConsumeCb {
 public:
  Kafkamon(const std::string& brokers, const std::string& topic);
  ~Kafkamon();

  void producerLoop();
  void consumerLoop();

  void event_cb(RdKafka::Event& event) override;
  void dr_cb(RdKafka::Message& message) override;
  void consume_cb(RdKafka::Message& msg, void* opaque) override;

  template<typename... Args>
  void logError(const std::string& msg, Args... args) {
    fprintf(stderr, (msg + "\n").c_str(), args...);
    fflush(stderr);
  }

  template<typename... Args>
  void logDebug(const std::string& msg, Args... args) {
    fprintf(stderr, (msg + "\n").c_str(), args...);
    fflush(stderr);
  }

  template<typename... Args>
  void logInfo(const std::string& msg, Args... args) {
    fprintf(stdout, (msg + "\n").c_str(), args...);
  }

 private:
  RdKafka::Conf* confGlobal_;
  RdKafka::Conf* confTopic_;
  std::string topicStr_;
  int64_t startOffset_;
};

Kafkamon::Kafkamon(const std::string& brokers, const std::string& topic)
    : confGlobal_(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)),
      confTopic_(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC)),
      topicStr_(topic),
      startOffset_(RdKafka::Topic::OFFSET_BEGINNING) {
  std::string errstr;
  confGlobal_->set("metadata.broker.list", brokers, errstr);
  confGlobal_->set("event_cb", (RdKafka::EventCb*) this, errstr);
  confGlobal_->set("dr_cb", (RdKafka::DeliveryReportCb*) this, errstr);

  dumpConfig(confGlobal_, "global");
  dumpConfig(confTopic_, "topic");
}

Kafkamon::~Kafkamon() {
}

void Kafkamon::producerLoop() {
  const int32_t partition = RdKafka::Topic::PARTITION_UA;
  std::string errstr;

  RdKafka::Producer* producer = RdKafka::Producer::create(confGlobal_, errstr);
  if (!producer) {
    logError("Failed to create producer. %s", errstr.c_str());
    exit(1);
  }
  std::cout << "$ Created producer " << producer->name() << std::endl;

  RdKafka::Topic* topic = RdKafka::Topic::create(producer, topicStr_, confTopic_, errstr);
  if (!topic) {
    std::cerr << "Failed to create topic: " << errstr << std::endl;
    exit(1);
  }
  std::cout << "$ Created topic " << topicStr_ << std::endl;

  for (unsigned long iteration = 0;; ++iteration) {
    std::string payload = std::to_string(iteration);

    // produce
    RdKafka::ErrorCode resp = producer->produce(
        topic, partition, RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
        const_cast<char*>(payload.c_str()), payload.size(), nullptr, nullptr);
    if (resp != RdKafka::ERR_NO_ERROR)
      std::cerr << "% Produce failed: " << RdKafka::err2str(resp) << std::endl;
    else
      std::cerr << "% Produced message (" << payload.size() << " bytes): \""
                << payload << "\"" << std::endl;

    // wait for ACK
    while (producer->outq_len() > 0) {
      std::cerr << "Waiting for " << producer->outq_len() << std::endl;
      producer->poll(10000);
    }

    sleep(1);
  }
}

void Kafkamon::consumerLoop() {
  const int32_t partition = 0;

  std::string errstr;
  RdKafka::Consumer *consumer = RdKafka::Consumer::create(confGlobal_, errstr);
  if (!consumer) {
    logError("Failed to create consumer: %s", errstr.c_str());
    abort();
  }
  logDebug("Created consumer %s", consumer->name().c_str());

  RdKafka::Topic *topic = RdKafka::Topic::create(consumer, topicStr_, confTopic_, errstr);
  if (!topic) {
    logError("Failed to create consumer topic: %s", errstr.c_str());
    abort();
  }

  RdKafka::ErrorCode resp = consumer->start(topic, partition, startOffset_);
  if (resp != RdKafka::ERR_NO_ERROR) {
    logError("Failed to start consumer (%d): %s", resp, errstr.c_str());
    abort();
  }

  for (;;) {
    consumer->consume_callback(topic, partition, 1000, this, nullptr);
    consumer->poll(0);
  }

  consumer->stop(topic, partition);
  consumer->poll(1000);

  delete topic;
  delete consumer;
}

void Kafkamon::event_cb(RdKafka::Event& event) {
  switch (event.type()) {
  case RdKafka::Event::EVENT_ERROR:
    logError("ERROR (%s): %s", RdKafka::err2str(event.err()).c_str(), event.str().c_str());
    if (event.err() == RdKafka::ERR__ALL_BROKERS_DOWN)
      abort();
    break;
  case RdKafka::Event::EVENT_STATS:
    logDebug("STATS: %s", event.str().c_str());
    break;
  case RdKafka::Event::EVENT_LOG:
    logDebug("LOG-%i-%s: %s", event.severity(), event.fac().c_str(), event.str().c_str());
    break;
  default:
    std::cerr << "EVENT " << event.type() << " ("
              << RdKafka::err2str(event.err()) << "): " << event.str()
              << std::endl;
    break;
  }
}

void Kafkamon::dr_cb(RdKafka::Message& message) {
  printf("[DeliveryReport] topic:%s, offset:%" PRIi64 ", err:%d (%s); \"%s\"\n",
      message.topic_name().c_str(),
      message.offset(),
      message.err(),
      message.errstr().c_str(),
      std::string((const char*) message.payload()).c_str());

  if (message.key())
    std::cout << "Key: " << *(message.key()) << ";" << std::endl;
}

void Kafkamon::consume_cb(RdKafka::Message& message, void* opaque) {
  switch (message.err()) {
    case RdKafka::ERR__TIMED_OUT:
      break;

    case RdKafka::ERR_NO_ERROR:
      /* Real message */
      std::cout << "Read msg at offset " << message.offset() << std::endl;
      if (message.key()) {
        std::cout << "Key: " << *message.key() << std::endl;
      }
      printf("%.*s\n",
        static_cast<int>(message.len()),
        static_cast<const char *>(message.payload()));
      break;

    case RdKafka::ERR__PARTITION_EOF:
      /* Last message */
      break;

    case RdKafka::ERR__UNKNOWN_TOPIC:
    case RdKafka::ERR__UNKNOWN_PARTITION:
      std::cerr << "Consume failed: " << message.errstr() << std::endl;
      break;

    default:
      /* Errors */
      std::cerr << "Consume failed: " << message.errstr() << std::endl;
      break;
  }
}

void printHelp() {
  printf("Usage: kafkamon [-t topic] [-b brokers]\n");
}

int main(int argc, char* const argv[]) {
  std::string topic = "kafkamon";
  std::string brokers = "localhost:9092";

  for (bool done = false; !done;) {
    switch (getopt(argc, argv, "b:t:h")) {
      case 'b':
        brokers = optarg;
        break;
      case 't':
        topic = optarg;
        break;
      case 'h':
        printHelp();
        return 0;
      case -1:
        done = true;
        break;
      default:
        printHelp();
        return 1;
    }
  }

  Kafkamon kafkamon(brokers, topic);

  std::thread producer(std::bind(&Kafkamon::producerLoop, &kafkamon));
  std::thread consumer(std::bind(&Kafkamon::consumerLoop, &kafkamon));

  producer.join();
  consumer.join();

  return 0;
}
