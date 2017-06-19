# presto-fluentd
This is the Plugin for Presto to send queries into fluentd.

# build
```
mvn clean package
```

# deploy
create event-listener.properties

for example, /pato/to/presto-server-0.179/etc/event-listener.properties
```
event-listener.name=presto-fluentd
event-listener.fluentd-host=localhost
event-listener.fluentd-port=24224
event-listener.fluentd-tag=presto.query
```

copy jar
```
# ls -1 /pato/to/presto-server-0.179/plugin/presto-fluentd/
fluency-1.3.0.jar
guava-21.0.jar
jackson-annotations-2.8.1.jar
jackson-core-2.7.1.jar
jackson-databind-2.7.1.jar
jackson-dataformat-msgpack-0.8.12.jar
log-0.148.jar
msgpack-core-0.8.12.jar
phi-accural-failure-detector-0.0.4.jar
presto-fluentd-0.0.1.jar
slf4j-api-1.7.22.jar
```

# reference
- https://prestodb.io/docs/current/develop/event-listener.html
- https://github.com/zz22394/presto-audit
- https://github.com/twitter-forks/presto/pull/58
