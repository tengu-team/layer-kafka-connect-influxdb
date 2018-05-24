# Kafka-connect-influxdb

Kafka Connect is a framework to stream data into and out of Kafka. For more information see the [documentation](https://docs.confluent.io/current/connect/concepts.html#concepts).

## Overview
This charm sets up a Kafka connect cluster in Kubernetes and configures it to send Kafka topic data (source) to InfluxDB (sink).
The connector used is made by [Landoop](http://www.landoop.com/kafka/connectors/), and can be found [here](https://github.com/Landoop/stream-reactor).

Version kafka-connect-influxdb-0.2.6-3.2.2 is used to be compatible with the [Kafka Bigtop charm](https://jujucharms.com/kafka/).

By default the charm configures the connector to expect JSON formatted messages from Kafka. To overwrite this behaviour set the `worker-config` **before** deploying the charm.

## How to use
```bash
# Deploy and add relation
juju deploy kafka-connect-influxdb connect
juju add-relation connect kafka
juju add-relation influxdb connect
juju add-relation kubernetes-deployer connect

# Configure the connect charm
juju config connect "topic=topic1"
juju config connect "database=foo"
juju config connect "kcql=INSERT INTO fooMeasure SELECT * FROM topic1 WITHTIMESTAMP sys_time()"
```

## Authors
This software was created in the [IDLab research group](https://www.ugent.be/ea/idlab/en) of [Ghent University](https://www.ugent.be/en) in Belgium. This software is used in [Tengu](https://tengu.io), a project that aims to make experimenting with data frameworks and tools as easy as possible.

- Sander Borny <sander.borny@ugent.be>