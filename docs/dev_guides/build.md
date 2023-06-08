# Building Tatris

## Build binary from source
**Prerequisites**
* Unix-like System
* `GNU Make` and `git`
* `golang` dev environment

**Download the source**
```shell
git clone git@github.com:tatris-io/tatris.git
cd tatris
```

**Execute and checking out the binaries**
```shell
make
```
You can use the `fast-build` target to accelerate this phase. It takes less build time by ignoring various checks.
```shell
make fast-build
```
All binaries lie under the `./bin` directory.

## Build docker image
**Prerequisites**
* Unix-like System
* `GNU Make` and `git`
* `Docker`

**Download the source**
```shell
git clone git@github.com:tatris-io/tatris.git
cd tatris
```

**Build image**
```shell
make docker-image
```
The above-mentioned command is a default image-building target and outputs an anonymous image. You can use the following optional args to adjust this phase.
* **`TARGETPLATFORM`** enables cross-platform building and is usually set to `linux/amd64` or `linux/arm64`.
* **`TAG`** specifies tag of the output docker image.

A more practical example:
```shell
make docker-image TAG=tatris:0.1.0 TARGETPLATFORM=linux/amd64
```

Users in China can use GOPROXY to speed up building:
```shell
GOPROXY=https://goproxy.cn,direct make docker-image
```

## Configure and launch Tatris
Get the details in [configuring guide](/docs/user_guides/configure.md)
