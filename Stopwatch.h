#pragma once
#include <time.h>
#include "LogMessage.h"

class Stopwatch {
 public:
  Stopwatch();
  Stopwatch(timespec start, timespec stop);

  void start();
  void stop();
  unsigned elapsedMs();

 private:
  timespec startedAt_;
  timespec stoppedAt_;
};

LogMessage& operator<<(const LogMessage& log, const Stopwatch& w);
