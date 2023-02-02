# tatris
[![Build Status](https://github.com/tatris-io/tatris/actions/workflows/build.yml/badge.svg)](https://github.com/tatris-io/tatris/actions/workflows/build.yml)
![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)

Time-aware storage and search system


## Getting started

### Build binary from source
**Prerequisites**
* Unix-like System
* `GNU Make` and `git`
* `golang` dev environment

**Downloading the source**
```shell
git@github.com:tatris-io/tatris.git
cd tatris
```

**Executing and checking out the binaries**
```shell
make
```
You can use the `fast-build` target to accelerate this phase. It takes less build time by ignoring various checks.
```shell
make fast-build
```
All binaries lie under the `./bin` directory.

### Build docker image
**Prerequisites**
* Unix-like System
* `GNU Make` and `git`
* `Docker`

**Downloading the source**
```shell
git@github.com:tatris-io/tatris.git
cd tatris
```

**Building image**
```shell
make docker-image
```
The abovementioned command is a default image-building target and outputs an anonymous image. You can use the following optional args to adjust this phase.
* **`TARGETPLATFORM`** enables cross-platform building and is usually set to `linux/amd64` or `linux/arm64`.
* **`TAG`** specifies tag of the output docker image.

A more practical example:
```shell
make docker-image TAG=tatris:0.1.0 TARGETPLATFORM=linux/amd64
```

### Starting via source code
```
git clone git@github.com:tatris-io/tatris.git && cd tatris && make
./bin/tatris-server --conf.logging=conf/log-conf.yml --conf.server=conf/server-conf.yml
```

### Starting via docker
Comming soon ...