# Reproduce LinkBench Results of LiveGraph

As [LinkBench](https://github.com/facebookarchive/linkbench) is implemented in Java and LiveGraph is in C++, we record traces collected from LinkBench Java driver and replay them in a driver of C++ version, so that LiveGraph can perform those queries.

These repositories are used to run LinkBench on LiveGraph:

* A [modified LinkBench Java driver](https://github.com/coolerzxw/LinkBench), with a wrapper implementation which processes the queries correctly (by calling MySQL implementation inside) and also records the operations of each thread into files.
* A [C++ driver](https://github.com/coolerzxw/LinkBench-Cpp), which reads the queries from files, calls LiveGraph (or other implementations LMDB and RocksDB), and gives performance statistics.

Detailed steps of reproducing are listed as follows. For questions, you may want to open an issue or contact Jiping Yu (yjp19@mails.tsinghua.edu.cn). Also, we are currently working on implementing LinkBench directly in C++, to make reproducing the results easier.

## Recording

Build, config, and run the [modified Java driver](https://github.com/coolerzxw/LinkBench) in the same way as the [original driver](https://github.com/facebookarchive/linkbench). After the run, the recorded traces are written to `record` directory of the current working directory.

## Replaying

Clone and build the C++ driver.

```
git clone https://github.com/coolerzxw/LinkBench-Cpp.git
cd LinkBench-Cpp
mkdir build
cd build
cmake ..
make
```

You can run each implementation as follows. `[#loaders]` and `[#requesters]` should equal the numbers during recording.

```
./livegraph [database path] [max #vertices] [trace directory path] [#loaders] [#requesters]
./lmdb [trace directory path] [#loaders] [#requesters] [database path]
./rocksdb [trace directory path] [#loaders] [#requesters] [database path]
```
