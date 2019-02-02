# ODPS Console

## Requirements

- Java 6+

## Build

```shell
git clone ...
cd aliyun-odps-console
mvn clean package -DskipTests
```

## Run Unittest

- you will have to configure there odps_config.ini files in source tree:

```
odps-console-tunnel/src/test/resources/odps_config.ini
odps-console-dist-public/odps_config.ini
odps-console-public/src/test/resources/odps_config.ini
odps-console-mr/src/test/resources/odps_config.ini
odps-console-sql/src/test/resources/odps_config.ini
odps-console-xflow/src/test/resources/odps_config.ini
odps-console-auth/src/test/resources/odps_config.ini
odps-console-basic/src/test/resources/odps_config.ini
odps-console-dship/src/test/resources/odps_config.ini
odps-console-resource/src/test/resources/odps_config.ini
```

- `mvn clean test`

## Authors && Contributors

- [Wang Shenggong](https://github.com/shellc)
- [Ni Zheming](https://github.com/nizheming)
- [Li Ruibo](https://github.com/lyman)
- [Zhong Wang](https://github.com/cornmonster)

## License

licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html)
