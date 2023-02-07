# Configuring Tatris
Tatris ships with good defaults and requires very little configuration. However, with the help of config files, you can still modify the behavior of Tatris in advanced use cases.

## Config files format and location
Tatris has two configuration files:
* `server-conf.yml` for configuring the `Tatris` server process
* `log-conf.yml` for configuring `Tatris` logging

The configuration format is [YAML](https://yaml.org). Here are the example files: [server-conf.yml](/conf/server-conf.yml) and [log-conf.yml](/conf/log-conf.yml).

These files are located in the config directory. For the archive distribution (tar.gz or zip), the config directory location defaults to `$TATRIS_HOME/conf`. A typical directory organization would be something like this:
```
─── tatris
    ├── bin
    │   ├── start-server.sh
    │   └── tatris-server
    ├── conf
    │   ├── log-conf.yml
    │   └── server-conf.yml
    └── logs
```

Note that the above-mentioned default location is just a convention. The binary does not search the directory structure for its config file. It accepts and only accepts the explicitly specified command line inputs `--conf.logging` and `--conf.server`. If you are trying to start the tatris server with the binary, please specify these two command line arguments.

Alternatively, we hereby provide a bootstrap script `start-server.sh`, which eases the complicated usage and adapts for different scenarios. `start-server.sh` searches and decides which configuration file the binary should use and composes the command line arguments.

With the mechanism provided by `start-server.sh`, the location of the config files can be changed via the `TATRIS_PATH_SERVER_CONF` and `TATRIS_PATH_LOGGING_CONF` environment variables. You can `export` the `TATRIS_PATH_SERVER_CONF` and `TATRIS_PATH_LOGGING_CONF` environment variables via the command line or your shell profile.

```shell
export TATRIS_PATH_SERVER_CONF=/path/to/my/server/config
export TATRIS_PATH_LOGGING_CONF=/path/to/my/logging/config
./bin/start-server.sh
```

The bootstrap script `start-server.sh` organizes the command line arguments in the following order of precedence:
1. Environment variables: `TATRIS_PATH_SERVER_CONF` and `TATRIS_PATH_LOGGING_CONF`
2. Config files in the conventional `./bin/conf` directory

## Configuring Tatris on `Docker` Locally
The entry point of `Tatris` official docker image is `bin/start-server.sh`. So if you are going to use the default config, nothing else should be done. If customized config files are wanted, you can compose mechanisms that docker provides (docker -e and docker -v) to accomplish the goal. as the follows

```
sudo docker run -v /local/source/conf/path:/dest/conf/path \
  --env TATRIS_PATH_SERVER_CONF=/dest/conf/path/${SERVER_CONF_NAME} \
  --env TATRIS_PATH_LOGGING_CONF=/dest/conf/path/${LOGGING_CONF_NAME} \
  ${tatris-image-tag} 
```

## Configuring Tatris on `Kubernetes`
Like configuring on `Docker`, the underlying mechanism for config customization is the environment variables: `TATRIS_PATH_SERVER_CONF` and `TATRIS_PATH_LOGGING_CONF`. On `Kubernetes`, you can compose the `ConfigMap` (configMaps can be mounted as data volumes) and environment variable setting mechanisms to accomplish the goal.

Refer to:
* [Using configmaps as files](https://kubernetes.io/docs/concepts/configuration/configmap/#using-configmaps-as-files-from-a-pod)
* [Define Environment Variables for a Container](https://kubernetes.io/docs/tasks/inject-data-application/define-environment-variable-container)
