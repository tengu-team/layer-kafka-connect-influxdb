import os
import requests
from charms.layer import status
from charms.reactive import (
    when,
    when_any,
    when_not,
    set_flag,
    clear_flag,
    endpoint_from_flag,
)
from charmhelpers.core.hookenv import config, log
from charms.layer.kafka_connect_helpers import (
    set_worker_config, 
    register_connector,
    unregister_connector,
    get_configs_topic,
    get_offsets_topic,
    get_status_topic,
)

conf = config()
JUJU_UNIT_NAME = os.environ['JUJU_UNIT_NAME']
MODEL_NAME = os.environ['JUJU_MODEL_NAME']
# Unique kafka connector name
INFLUXDB_CONNECTOR_NAME = (MODEL_NAME + 
                          JUJU_UNIT_NAME.split('/')[0] +
                          "-influxdb")

@when_not('influxdb.available')
def waiting_influxdb():
    status.blocked('Waiting for influxdb relation')


@when_not('config.set.kcql')
def waiting_kcql():
    status.blocked('Waiting for KCQL statement.')


@when_not('config.set.database')
def waiting_database():
    status.blocked('Waiting for database config.')


@when('kafka-connect-base.ready',
      'config.set.database',
      'config.set.kcql',
      'config.set.max-tasks',  # TODO check if topics also needs to be in here
      'influxdb.available')
def status_set_ready():
    # This is mainly so multiple subordinate charms don't
    # get stuck in blocked state when only the leader
    # may communicate with the base layer.
    status.active('ready')


@when_any('config.changed.topics',
          'config.changed.max-tasks',
          'config.changed.kcql',
          'config.changed.database')
def config_changed():
    # Clear the running flag so it can resubmit the changed config 
    clear_flag('kafka-connect-influxdb.running')


@when('influxdb.available',
      'kafka-connect-base.topic-created',
      'leadership.is_leader')
@when_not('kafka-connect-influxdb.configured')
def configure_kc_influxdb():
    # Kafka connect worker config
    worker_configs = {
        'key.converter': 'org.apache.kafka.connect.json.JsonConverter',
        'value.converter': 'org.apache.kafka.connect.json.JsonConverter',
        'key.converter.schemas.enable': 'false',
        'value.converter.schemas.enable': 'false',
        'internal.key.converter': 'org.apache.kafka.connect.json.JsonConverter',
        'internal.value.converter': 'org.apache.kafka.connect.json.JsonConverter',
        'internal.key.converter.schemas.enable': 'false',
        'internal.value.converter.schemas.enable': 'false',        
        'offset.flush.interval.ms': '10000',
        'config.storage.topic': get_configs_topic(),
        'offset.storage.topic': get_offsets_topic(),
        'status.storage.topic': get_status_topic(),
    }
    # Give the worker config to the base layer and let it deploy
    # to Kubernetes when possible.
    set_worker_config(worker_configs)
    set_flag('kafka-connect-influxdb.configured')
    set_flag('kafka-connect-base.install')


@when('influxdb.available',
      'config.set.kcql',
      'config.set.database',
      'leadership.is_leader',
      'kafka-connect.running')
@when_not('kafka-connect-influxdb.running')
def start_kc_influxdb():
    # Get all config for the InfluxDB connector.
    influxdb = endpoint_from_flag('influxdb.available')
    ensure_db_exists(conf.get('database'), influxdb.hostname(), influxdb.port())
    connector_configs = {
        'connector.class': 'com.datamountaineer.streamreactor.connect.influx.InfluxSinkConnector',
        'tasks.max': str(conf.get('max-tasks')),
        'connect.influx.url': 'http://' + influxdb.hostname() + ':' + influxdb.port(),
        'connect.influx.db': conf.get('database'),
        'connect.influx.username': influxdb.username(),
        'connect.influx.password': influxdb.password(),
        'connect.influx.kcql': conf.get('kcql'),
        'topics': conf.get("topics").replace(" ", ","),
    }
    # Ask the base layer to send the config to the Kafka connect REST API.
    response = register_connector(connector_configs, INFLUXDB_CONNECTOR_NAME)
    if response and (response.status_code == 200 or response.status_code == 201):
        status.active('ready')
        clear_flag('kafka-connect-influxdb.stopped')
        set_flag('kafka-connect-influxdb.running')
    else:
        log('Could not register/update connector Response: ' + str(response))
        status.blocked('Could not register/update connector, retrying next hook.')


@when('kafka-connect-influxdb.running',
      'leadership.is_leader')
@when_not('influxdb.available',
          'kafka-connect-influxdb.stopped')
def stop_influxdb_connect():
    # The InfluxDB relation has been removed while the connector is active,
    # ask the base layer to stop the connector.
    response = unregister_connector(INFLUXDB_CONNECTOR_NAME)
    if response and (response.status_code == 204 or response.status_code == 404):
        set_flag('kafka-connect-influxdb.stopped')
        clear_flag('kafka-connect-influxdb.running')


@when('kafka-connect-influxdb.running')
@when_not('kafka-connect.running')
def stop_running():
    clear_flag('kafka-connect-influxdb.running')


def ensure_db_exists(database, influxdb_host, influxdb_port):
    # Create the db if possible.
    data = {
        'q': 'CREATE DATABASE {}'.format(database),
    }
    url = "http://{}:{}".format(influxdb_host, influxdb_port)
    try:
        resp = requests.post(url, data=data)
        resp.raise_for_status()
        if resp.status_code == 200:
            return True
    except requests.exceptions.RequestException as e:
        log(e)
        status.blocked('Error creating Influxdb database.')
    return False
