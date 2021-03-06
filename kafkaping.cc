// This file is part of the "kafkaping" project
//   <http://github.com/christianparpart/kafkaping>
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
#include <atomic>
#include "LogMessage.h"

void dumpConfig(RdKafka::Conf* conf, const std::string& msg) {
  std::cout << "============================== " << msg << std::endl;
  auto d = conf->dump();
  for (auto i = d->begin(), e = d->end(); i != e;) {
    std::cout << *i++ << " = " << *i++ << std::endl;
  }
  std::cout << std::endl;
}

class StatsSink {
 public:
  virtual ~StatsSink() {}

  virtual void fill(int partition,
                    int64_t offset,
                    int64_t ts,
                    unsigned latencyMs) = 0;
};

class ConsoleSink : public StatsSink {
 public:
  ConsoleSink(const std::string& broker, const std::string& topic);
  ~ConsoleSink();

  void fill(int partition,
            int64_t offset,
            int64_t ts,
            unsigned latencyMs) override;

 private:
  std::string broker_;
  std::string topic_;
  timespec startedAt_;
  std::list<unsigned> latencies_;
};

ConsoleSink::ConsoleSink(const std::string& broker, const std::string& topic)
    : broker_(broker),
      topic_(topic),
      startedAt_(),
      latencies_() {
  clock_gettime(CLOCK_MONOTONIC, &startedAt_);
}

ConsoleSink::~ConsoleSink() {
  timespec finishedAt;
  clock_gettime(CLOCK_MONOTONIC, &finishedAt);

  int64_t timeMs = (finishedAt.tv_sec * 1000 + finishedAt.tv_nsec / 1000000) -
                   (startedAt_.tv_sec * 1000 + startedAt_.tv_nsec / 1000000);

  unsigned totalMs = 0, minMs = 0, maxMs = 0;
  for (unsigned ms: latencies_) {
    if (ms > maxMs)
      maxMs = ms;

    if (ms < minMs || !minMs)
      minMs = ms;

    totalMs += ms;
  }
  unsigned avgMs = totalMs / latencies_.size();

  std::cout << std::endl
            << "--- " << broker_ << " ping statistics ---" << std::endl
            << latencies_.size() << " messages received, time " << timeMs << "ms" << std::endl
            << "rtt min/avg/max = " << minMs << "/" << avgMs << "/" << maxMs << " ms" << std::endl;
}

void ConsoleSink::fill(int partition,
                       int64_t offset,
                       int64_t ts,
                       unsigned latencyMs) {
  std::cout << "Received from " << broker_ << ":"
            << " topic=" << topic_
            << " partition=" << partition
            << " offset=" << offset
            << " time=" << latencyMs << " ms" << std::endl;

  latencies_.emplace_back(latencyMs);
}

class Kafkaping : public RdKafka::EventCb,
                  public RdKafka::DeliveryReportCb,
                  public RdKafka::ConsumeCb,
                  public RdKafka::OffsetCommitCb {
 public:
  Kafkaping(const std::string& brokers,
            const std::string& topic,
            int count,
            int interval,
            bool debug,
            StatsSink* sink);
  ~Kafkaping();

  void producerLoop();
  void consumerLoop();

  void event_cb(RdKafka::Event& event) override;
  void dr_cb(RdKafka::Message& message) override;
  void consume_cb(RdKafka::Message& msg, void* opaque) override;
  void offset_commit_cb(RdKafka::ErrorCode err,
                        std::vector<RdKafka::TopicPartition*>& offsets) override;

  LogMessage logError() {
    return LogMessage("[ERROR] ", std::bind(&Kafkaping::logPrinter, this, std::placeholders::_1));
  }

  LogMessage logInfo() {
    return LogMessage("[INFO] ", std::bind(&Kafkaping::logPrinter, this, std::placeholders::_1));
  }

  LogMessage logDebug() {
    if (debug_) {
      return LogMessage("[DEBUG] ", std::bind(&Kafkaping::logPrinter, this, std::placeholders::_1));
    } else {
      return LogMessage("", [](auto m) {});
    }
  }

 private:
  void logPrinter(const std::string& msg) {
    std::lock_guard<decltype(stderrLock_)> _lk(stderrLock_);

    char ts[20];
    time_t t = time(nullptr);
    struct tm tm;
    localtime_r(&t, &tm);
    int timed = strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", &tm);

    if (timed) {
      fprintf(stderr, "[%s] %s\n", ts, msg.c_str());
    } else {
      fprintf(stderr, "%s\n", msg.c_str());
    }
  }

  void configureGlobal(const std::string& key, const std::string& val);
  void configureTopic(const std::string& key, const std::string& val);

 private:
  RdKafka::Conf* confGlobal_;
  RdKafka::Conf* confTopic_;
  std::string topicStr_;
  std::atomic<int> count_;
  int interval_;
  bool debug_;
  int64_t startOffset_;
  std::mutex stderrLock_;
  StatsSink* sink_;
};

Kafkaping::Kafkaping(const std::string& brokers,
                     const std::string& topic,
                     int count,
                     int interval,
                     bool debug,
                     StatsSink* sink)
    : confGlobal_(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)),
      confTopic_(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC)),
      topicStr_(topic),
      count_(count),
      interval_(interval),
      debug_(debug),
      startOffset_(RdKafka::Topic::OFFSET_END),
      sink_(sink) {
  std::string errstr;
  confGlobal_->set("event_cb", (RdKafka::EventCb*) this, errstr);
  confGlobal_->set("dr_cb", (RdKafka::DeliveryReportCb*) this, errstr);
  confGlobal_->set("metadata.broker.list", brokers, errstr);

  configureGlobal("client.id", "kafkaping");
  configureGlobal("group.id", "kafkaping");

  configureGlobal("request.timeout.ms", "5000");
  configureGlobal("connections.max.idle.ms", "10000");
  configureGlobal("message.send.max.retries", "10");

  configureGlobal("queue.buffering.max.ms", "1"); // don't buffer inflights messages

  // XXX only interesting if we wanna use the broker's offset store
  // configureGlobal("enable.auto.commit", "true");
  // configureGlobal("auto.commit.interval.ms", "true");
  // configureGlobal("auto.offset.reset", "latest");

  if (debug_) {
    dumpConfig(confGlobal_, "global");
    dumpConfig(confTopic_, "topic");
  }
}

void Kafkaping::configureGlobal(const std::string& key, const std::string& val) {
  std::string errstr;
  confGlobal_->set(key, val, errstr);
}

void Kafkaping::configureTopic(const std::string& key, const std::string& val) {
  std::string errstr;
  confTopic_->set(key, val, errstr);
}

Kafkaping::~Kafkaping() {
}

void Kafkaping::producerLoop() {
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

  while (count_.load() != 0) {
    timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    std::string payload = std::to_string(ts.tv_sec) + "." + std::to_string(ts.tv_nsec);

    // produce
    RdKafka::ErrorCode resp = producer->produce(
        topic, partition, RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
        const_cast<char*>(payload.c_str()), payload.size() + 1, nullptr, nullptr);
    if (resp != RdKafka::ERR_NO_ERROR)
      logError() << "Produce failed. " << RdKafka::err2str(resp);
    else
      logDebug() << "Produced payload:" << payload;

    // wait until flushed (no need to send another message if we can't even handle that)
    if (producer->outq_len()) {
      do producer->poll(1000);
      while (producer->outq_len() > 0);
    }
    if (interval_) {
      usleep(interval_ * 1000);
    }
  }

  while (producer->outq_len() > 0) {
    producer->poll(1000);
  }

  delete topic;
  //FIXME(doesn't return): delete producer;
}

void Kafkaping::consumerLoop() {
  const int32_t partition = 0;

  std::string errstr;
  std::unique_ptr<RdKafka::Consumer> consumer = RdKafka::Consumer::create(confGlobal_, errstr);
  if (!consumer) {
    logError() << "Failed to create consumer: " << errstr;
    abort();
  }
  logDebug() << "Created consumer " << consumer->name();

  std::unique_ptr<RdKafka::Topic> topic(RdKafka::Topic::create(consumer.get(),
                                                               topicStr_,
                                                               confTopic_,
                                                               errstr));
  if (!topic) {
    logError() << "Failed to create consumer topic: " << errstr;
    abort();
  }

  RdKafka::ErrorCode resp = consumer->start(topic, partition, startOffset_);
  if (resp != RdKafka::ERR_NO_ERROR) {
    logError() << "Failed to start consumer (" << resp << "): " << RdKafka::err2str(resp);
    abort();
  }

  while (count_.load() != 0) {
    consumer->consume_callback(topic, partition, 1000, this, nullptr);
    consumer->poll(0);
  }

  logDebug() << "Stopping consumer";
  consumer->stop(topic, partition);
  consumer->poll(1000);

  logDebug() << "Stopped consumer";
}

void Kafkaping::event_cb(RdKafka::Event& event) {
  switch (event.type()) {
  case RdKafka::Event::EVENT_ERROR:
    logError() << RdKafka::err2str(event.err()) << ": " << event.str();
    // if (event.err() == RdKafka::ERR__ALL_BROKERS_DOWN)
    //   abort();
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

void Kafkaping::dr_cb(RdKafka::Message& message) {
  if (message.err()) {
    logError() << "Devliery Report Failure. " << message.errstr();
  } else {
    logDebug()
        << "Delivered offset:" << message.offset()
        << " payload:" << std::string((const char*) message.payload(), message.len());
  }

  if (message.key())
    logDebug() << "Key: " << *(message.key()) << ";";
}

void Kafkaping::consume_cb(RdKafka::Message& message, void* opaque) {
  switch (message.err()) {
    case RdKafka::ERR__TIMED_OUT:
      break;
    case RdKafka::ERR_NO_ERROR: {
      if (count_.load() > 0) {
        --count_;
      }

      timespec now;
      clock_gettime(CLOCK_MONOTONIC, &now);

      long secs = 0, nsecs = 0;
      std::sscanf((char*) message.payload(), "%ld.%ld", &secs, &nsecs);
      timespec beg{secs, nsecs};

      int64_t diff = (now.tv_sec * 1000 + now.tv_nsec / 1000000) -
                     (beg.tv_sec * 1000 + beg.tv_nsec / 1000000);

      std::string broker = "<unknown>";
      confGlobal_->get("metadata.broker.list", broker);

      sink_->fill(message.partition(), message.offset(),
                  message.timestamp().timestamp, diff);
      break;
    }
    case RdKafka::ERR__PARTITION_EOF:
      /* Last message */
      break;
    default:
      logError() << "Consume failed. " << message.errstr();
      break;
  }
}

void Kafkaping::offset_commit_cb(RdKafka::ErrorCode err,
                                std::vector<RdKafka::TopicPartition*>& offsets) {
}

void printHelp() {
  printf("Usage: kafkaping [-g] [-t topic] [-c count] [-i interval_ms] [broker list]\n"
         "\n"
         "  -g        Increases debugging print verbosity\n"
         "  -t TOPIC  What Kafka topic to use for producing/consuming messages [kafkaping]\n"
         "  -c COUNT  Number of messages to send & receive before quitting [unlimited]\n"
         "  -i MSECS  Time to wait between to pings\n");
}

int main(int argc, char* const argv[]) {
  std::string topic = "kafkaping";
  std::string broker; // = "localhost:9092";
  int count = -1;
  int interval = 1000; // ms
  bool debug = false;

  for (bool done = false; !done;) {
    switch (getopt(argc, argv, "t:c:i:hg:S:")) {
      case 'g':
        debug = true;
        break;
      case 't':
        topic = optarg;
        break;
      case 'c':
        count = std::atoi(optarg);
        break;
      case 'i':
        interval = std::atoi(optarg);
        break;
      // case 'S':
      //   sink.reset(new StatsdSink(optarg));
      //   break;
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
  if (optind < argc) {
    while (optind < argc) {
      if (!broker.empty()) {
        broker += ",";
      }
      broker += argv[optind++];
    }
  }

  if (broker.empty()) {
    fprintf(stderr, "No broker passed.\n");
    printHelp();
    return 1;
  }

  std::unique_ptr<StatsSink> sink(new ConsoleSink(broker, topic));

  Kafkaping kafkaping(broker, topic, count, interval, debug, sink.get());

  std::thread producer(std::bind(&Kafkaping::producerLoop, &kafkaping));
  std::thread consumer(std::bind(&Kafkaping::consumerLoop, &kafkaping));

  producer.join();
  consumer.join();

  return 0;
}
