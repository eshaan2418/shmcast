# shmcast: common entry points. J defaults to half the logical CPUs.
J ?= $(shell n=$$( (sysctl -n hw.ncpu 2>/dev/null || nproc) ); echo $$(( n/2 > 2 ? n/2 : 2 )))
BUILD ?= build
ITCH ?= $(HOME)/quantfactory/_data/itch/01302019.NASDAQ_ITCH50.gz

.PHONY: build test sanitize tsan bench lint fuzz demo data experiments clean

build:
	cmake -S . -B $(BUILD) -G Ninja -DCMAKE_BUILD_TYPE=Release
	cmake --build $(BUILD) -j $(J)

test: build
	ctest --test-dir $(BUILD) -j 2 --output-on-failure

sanitize:
	cmake -S . -B build-asan -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo -DSHMCAST_SANITIZE=address,undefined
	cmake --build build-asan -j $(J)
	ctest --test-dir build-asan -j 2 --output-on-failure

tsan:
	cmake -S . -B build-tsan -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo -DSHMCAST_SANITIZE=thread
	cmake --build build-tsan -j $(J)
	TSAN_OPTIONS=halt_on_error=1 ctest --test-dir build-tsan -j 1 --output-on-failure

# Latency benchmarks spin up to 9 cores: run on an otherwise idle machine.
bench: build
	$(BUILD)/shmcast-bench all 200000 | tee bench/results/bench.json

lint:
	@command -v clang-format >/dev/null && clang-format --dry-run --Werror include/shmcast/*.hpp tools/*.cpp tests/*.cpp fuzz/*.cpp || echo "clang-format not installed; skipping"
	@command -v clang-tidy >/dev/null && clang-tidy -p $(BUILD) tools/*.cpp || echo "clang-tidy not installed; skipping"

FUZZ_CXX ?= clang++
fuzz:
	cmake -S . -B build-fuzz -G Ninja -DCMAKE_CXX_COMPILER=$(FUZZ_CXX) -DCMAKE_BUILD_TYPE=RelWithDebInfo -DSHMCAST_FUZZ=ON
	cmake --build build-fuzz --target fuzz-reader -j $(J)
	mkdir -p fuzz/corpus && build-fuzz/fuzz-reader fuzz/corpus -max_total_time=$${FUZZ_SECONDS:-300} -rss_limit_mb=2048

# NASDAQ ITCH sample (public, ~4.6 GB gzip), same file as itchbook.
data:
	mkdir -p $(dir $(ITCH)) && curl -fL -o $(ITCH) https://emi.nasdaq.com/ITCH/Nasdaq%20ITCH/01302019.NASDAQ_ITCH50.gz

# Full-day replay to 4 consumers + 1 deliberately slow consumer, verified by checksum.
demo: build
	$(BUILD)/shmcast-itch $(ITCH) --consumers 4 --slow 1

experiments: build
	bash scripts/experiments.sh $(ITCH) | tee research/results/producer_experiments.txt

clean:
	rm -rf build build-asan build-tsan build-fuzz
