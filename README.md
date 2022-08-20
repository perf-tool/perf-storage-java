# perf-storage-java
inspired by [yscb](https://github.com/brianfrankcooper/YCSB)
## command
```bash
curl localhost:20004/actuator/prometheus
```

## start skywalking

configure environment:

`SW_AGENT_ENABLE`: open skywalking agent, switch `true`, `false`, default `false`.

`SW_SERVICE_NAME`: skywalking trace service name, default `perf-storage`.

`SW_COLLECTOR_URL`: skywalking collector url, default `localhost:11800`.
