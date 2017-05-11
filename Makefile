CXX = clang++-3.9
CXXFLAGS = -pthread -lpthread -O0 -ggdb #-stdlib=libc++

all: kafkaping

kafkaping: kafkaping.cc LogMessage.h
	$(CXX) $(CXXFLAGS) -o $@ $< -std=c++14 -lrdkafka -lrdkafka++

clean:
	rm -f kafkaping

.PHONY: clean all
