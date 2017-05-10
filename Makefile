CXX = clang++-3.9
CXXFLAGS = -pthread -lpthread -O0 -ggdb #-stdlib=libc++

all: kafkamon

kafkamon: kafkamon.cc
	$(CXX) $(CXXFLAGS) -o $@ $< -std=c++14 -lrdkafka -lrdkafka++

clean:
	rm -f kafkamon

.PHONY: clean all
