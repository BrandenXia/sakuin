# sakuin

A self-hostable DHT indexing system and tracker, designed to be performant,
storage-efficient, and scalable.

**Warning**: this project is still in development, and it's not yet functional.

**Why re-invent the wheel?**

Existing solution such as [bitmagnet](https://github.com/bitmagnet-io/bitmagnet)
is limited in its ability to be scalable and storage-efficient due to its reliance
on a single centralized database. On the other hand, sakuin is designed to be a
distributed system, with the indexer, storage, search, and API treated as separate
components that can be scaled independently.

**What Sakuin aims to achieve?**

- Distributed system of indexers, storage, search, and API components that can
  be scaled independently
- Incremental compression of data over time to archive a comfort point between
  storage efficiency and search performance
- Robust storage system that's portable and crash-safe
- Configurable retention policies for indexed data
- Global and per crawler network traffic control to avoid overloading the network

## Build

Currently, you need to build Sakuin from source.

Sakuin depends on [openssl](https://github.com/openssl/openssl),
[zstd](https://github.com/facebook/zstd), [asio](https://github.com/chriskohlhoff/asio),
[toml++](https://github.com/marzer/tomlplusplus), [llhttp](https://github.com/nodejs/llhttp),
[nlohmann/json](https://github.com/nlohmann/json), and [spdlog](https://github.com/gabime/spdlog).

[Xmake](https://xmake.io) should be able to handle all the dependencies automatically.
Meanwhile, C++20 module support is required.

```bash
git clone https://github.com/BrandenXia/sakuin
cd sakuin/

# MacOS
# since Apple clang does not yet have module support, you need to install llvm
# via homebrew and use it as the toolchain
xmake f --toolchain=clang --sdk=/opt/homebrew/opt/llvm
# Linux
# The latest gcc have complete module support. It should be available on most
# Linux distributions.
xmake f --toolchain=gcc

xmake build
```
