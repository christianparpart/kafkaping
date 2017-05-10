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
#include <vector>
#include <cstdio>
#include <cinttypes>
#include <librdkafka/rdkafkacpp.h>
#include <getopt.h>
#include <unistd.h>
#include <mutex>
#include "LogMessage.h"

// TODO
// - [ ] use logging objects with << operator overload (see demo proj)
// - [ ] expose produer/consumer lag (in ms)
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
                 public RdKafka::ConsumeCb,
                 public RdKafka::OffsetCommitCb {
 public:
  Kafkamon(const std::string& brokers, const std::string& topic);
  ~Kafkamon();

  void producerLoop();
  void consumerLoop();

  void event_cb(RdKafka::Event& event) override;
  void dr_cb(RdKafka::Message& message) override;
  void consume_cb(RdKafka::Message& msg, void* opaque) override;
  void offset_commit_cb(RdKafka::ErrorCode err,
                        std::vector<RdKafka::TopicPartition*>& offsets) override;

  LogMessage logError() {
    return LogMessage("[ERROR] ", std::bind(&Kafkamon::logPrinter, this, std::placeholders::_1));
  }

  LogMessage logInfo() {
    return LogMessage("[INFO] ", std::bind(&Kafkamon::logPrinter, this, std::placeholders::_1));
  }

  LogMessage logDebug() {
    return LogMessage("[DEBUG] ", std::bind(&Kafkamon::logPrinter, this, std::placeholders::_1));
  }

 private:
  void logPrinter(const std::string& msg) {
    std::lock_guard<decltype(stderrLock_)> _lk(stderrLock_);
    fprintf(stderr, "%s\n", msg.c_str());
  }

  void configureGlobal(const std::string& key, const std::string& val);
  void configureTopic(const std::string& key, const std::string& val);

 private:
  RdKafka::Conf* confGlobal_;
  RdKafka::Conf* confTopic_;
  std::string topicStr_;
  int64_t startOffset_;
  std::mutex stderrLock_;
};

Kafkamon::Kafkamon(const std::string& brokers, const std::string& topic)
    : confGlobal_(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)),
      confTopic_(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC)),
      topicStr_(topic),
      startOffset_(RdKafka::Topic::OFFSET_STORED) { // _BEGINNING, _END, _STORED
  std::string errstr;
  confGlobal_->set("event_cb", (RdKafka::EventCb*) this, errstr);
  confGlobal_->set("dr_cb", (RdKafka::DeliveryReportCb*) this, errstr);
  confGlobal_->set("metadata.broker.list", brokers, errstr);

  configureGlobal("client.id", "kafkamon");
  configureGlobal("group.id", "kafkamon");
  configureGlobal("auto.offset.reset", "latest");
  configureGlobal("enable.auto.commit", "true");
  configureGlobal("auto.commit.interval.ms", "true");
  configureGlobal("request.timeout.ms", "5000");
  configureGlobal("connections.max.idle.ms", "10000");
  configureGlobal("message.send.max.retries", "10000");

  dumpConfig(confGlobal_, "global");
  dumpConfig(confTopic_, "topic");
}

void Kafkamon::configureGlobal(const std::string& key, const std::string& val) {
  std::string errstr;
  confGlobal_->set(key, val, errstr);
}

void Kafkamon::configureTopic(const std::string& key, const std::string& val) {
  std::string errstr;
  confTopic_->set(key, val, errstr);
}

Kafkamon::~Kafkamon() {
}

void Kafkamon::producerLoop() {
  const int32_t partition = RdKafka::Topic::PARTITION_UA;
  std::string errstr;

  RdKafka::Producer* producer = RdKafka::Producer::create(confGlobal_, errstr);
  if (!producer) {
    logError() << "Failed to create producer " << errstr;
    exit(1);
  }
  logDebug() << "Created producer " << producer->name();

  RdKafka::Topic* topic = RdKafka::Topic::create(producer, topicStr_, confTopic_, errstr);
  if (!topic) {
    logError() << "Failed to create topic: " << errstr;
    exit(1);
  }
  logDebug() << "Created topic " << topicStr_;

  for (unsigned long iteration = 0;; ++iteration) {
    std::string payload = std::to_string(iteration);

    // produce
    RdKafka::ErrorCode resp = producer->produce(
        topic, partition, RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
        const_cast<char*>(payload.c_str()), payload.size(), nullptr, nullptr);
    if (resp != RdKafka::ERR_NO_ERROR)
      logError() << "Produce failed: " << RdKafka::err2str(resp);
    else
      logError() << "Produced message (" << payload.size() << " bytes): \""
                << payload << "\"";

    // wait until flushed (no need to send another message if we can't even handle that)
    while (producer->outq_len() > 0) {
      producer->poll(1000);
    }

    sleep(1);
  }
}

void Kafkamon::consumerLoop() {
  const int32_t partition = 0;

  std::string errstr;
  RdKafka::Consumer *consumer = RdKafka::Consumer::create(confGlobal_, errstr);
  if (!consumer) {
    logError() << "Failed to create consumer: " << errstr;
    abort();
  }
  logDebug() << "Created consumer " << consumer->name();

  RdKafka::Topic *topic = RdKafka::Topic::create(consumer, topicStr_, confTopic_, errstr);
  if (!topic) {
    logError() << "Failed to create consumer topic: " << errstr;
    abort();
  }

  RdKafka::ErrorCode resp = consumer->start(topic, partition, startOffset_);
  if (resp != RdKafka::ERR_NO_ERROR) {
    logError() << "Failed to start consumer (" << resp << "): " << RdKafka::err2str(resp);
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
    logError() << RdKafka::err2str(event.err()) << ": " << event.str();
    if (event.err() == RdKafka::ERR__ALL_BROKERS_DOWN)
      abort();
    break;
  case RdKafka::Event::EVENT_STATS:
    logDebug() << "STATS: " << event.str();
    break;
  case RdKafka::Event::EVENT_LOG:
    logDebug() << "LOG-" << event.severity() << "-" << event.fac() << ": " << event.str();
    break;
  default:
    logError() << "EVENT " << event.type() << " ("
               << RdKafka::err2str(event.err()) << "): " << event.str();
    break;
  }
}

void Kafkamon::dr_cb(RdKafka::Message& message) {
  logDebug() << "[DeliveryReport]"
      << " topic:" << message.topic_name()
      << " partition:" << message.partition()
      << " offset:" << message.offset()
      << " err:" << message.err()
      << " (" << message.errstr() << ")"
      << "; \"" << std::string((const char*) message.payload()) << "\""
      ;

  if (message.key())
    logDebug() << "Key: " << *(message.key()) << ";";
}

void Kafkamon::consume_cb(RdKafka::Message& message, void* opaque) {
  switch (message.err()) {
    case RdKafka::ERR__TIMED_OUT:
      break;
    case RdKafka::ERR_NO_ERROR:
      logDebug() << "Consumed message at offset " << message.offset();
      break;
    case RdKafka::ERR__PARTITION_EOF:
      /* Last message */
      break;
    default:
      logError() << "Consume failed: " << message.errstr();
      break;
  }
}

void Kafkamon::offset_commit_cb(RdKafka::ErrorCode err,
                                std::vector<RdKafka::TopicPartition*>& offsets) {
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
