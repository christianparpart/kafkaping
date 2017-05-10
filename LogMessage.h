// This file is part of the "x0" project
//   <http://github.com/christianparpart/x0>
//   (c) 2017 Christian Parpart <christian@parpart.family>
//
// Licensed under the MIT License (the "License"); you may not use this
// file except in compliance with the License. You may obtain a copy of
// the License at: http://opensource.org/licenses/MIT
#pragma once

#include <string>
#include <sstream>
#include <cstdint>

class LogMessage {
 public:
  LogMessage(const std::string& prefix,
             std::function<void(const std::string&)> flusher)
      : message_(),
        flusher_(flusher) {
    append(prefix);
  }

  LogMessage(LogMessage&& m)
      : message_(std::move(m.message_)),
        flusher_(std::move(m.flusher_)) {}

  ~LogMessage() {
    flusher_(message_.str());
  }

  template<typename T>
  void append(T part) const { // XXX must be const
    message_ << part;
  }

 private:
  mutable std::stringstream message_;
  std::function<void(const std::string&)> flusher_;
};

inline const LogMessage& operator<<(const LogMessage& log, const char* message) {
  log.append(message);
  return log;
}

inline const LogMessage& operator<<(const LogMessage& log, const std::string& message) {
  log.append(message);
  return log;
}

inline const LogMessage& operator<<(const LogMessage& log, int value) {
  log.append(std::to_string(value));
  return log;
}

inline const LogMessage& operator<<(const LogMessage& log, size_t value) {
  log.append(std::to_string(value));
  return log;
}

inline const LogMessage& operator<<(const LogMessage& log, int64_t value) {
  log.append(std::to_string(value));
  return log;
}
