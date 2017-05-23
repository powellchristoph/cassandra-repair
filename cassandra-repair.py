#!/usr/bin/env python3

import argparse
import json
import logging
import os
import redis
import subprocess
import time
import yaml

from collections import OrderedDict()
from datetime import timedelta
from enum import Enum
from sys import argv, exit


class RepairManager():
    def __init__(self, config_file):
        self._logger = logging.getLogger(__name__)
        self._read_config(config_file)

        try:
            host, port = self._redis_host.split(':')
            self._redis = redis.StrictRedis(host=host, port=port, db=0)
        except:
            self._logger.critical("Unable to connect to redis")
            exit(1)

        self._prep_redis()
        self._check_for_nodetool()
        self._keyspace_map = self._get_keyspace_info()
        self._failures = []
        self._start_time = None
        self._cluster_pause = 1

    def _prep_redis(self):
        self._redis.set('REPAIR_STATUS', 'running')
        self._redis.delete('REPAIR_START_TIME')
        self._redis.delete('REPAIR_CURRENT_JOB')
        self._redis.delete('REPAIR_FAILED_JOBS')
        self._redis.delete('REPAIR_TOTAL_TIME')

    def _read_config(self, config_file):
        """
        Load the configuration file.
        """
        self._logger.debug("Reading config file {}".format(config_file))
        with open(config_file, 'r') as f:
            try:
               config = yaml.load(f)
            except yaml.YAMLError as exc:
                self._logger.critical('Invalid yaml, unable to load config file.')
                self._logger.critical(exc)
                exit(1)

        if not config:
            config = {}

        self._nodetool = config.get('nodetool_path', '/usr/bin/nodetool')
        self._hostlist = config.get('hosts', ['127.0.0.1'])
        self._retries = config.get('retries', 3)
        self._default_timeout = config.get('timeout', 3600)
        self._blacklist = config.get('blacklist', [])
        self._cqlsh_ip = config.get('connect', self._hostlist[0])
        self._redis_host = config.get('redis', 'localhost:6379')

    def _decode(self, value):
        """
        In python3, subprocess returns byte objects instead of nice strings.
        """
        if isinstance(value, list):
            return [s.decode('utf-8') for s in value]
        else:
            return value.decode('utf-8')

    def _get_keyspace_info(self):
        self._logger.info("Gathering cassandra keyspace information.")
        info = OrderedDict()
        keyspaces = self._get_keyspaces()
        for k in keyspaces:
            info[k] = self._get_columnfamilies(k)
        return info

    def _get_keyspaces(self):
        try:
            cmd = ['cqlsh', self._cqlsh_ip, '-e', 'DESC KEYSPACES']
            output = subprocess.check_output(cmd).strip().split()
        except subprocess.CalledProcessError as err:
            self._logger.critical("Unable to connect to cassandra at {}".format(self._cqlsh_ip))
            self._redis.set("REPAIR_STATUS", "error")
            exit(1)

        formatted_output = self._decode(output)
        formatted_output.remove('system') # Do not repair the system keyspace
        self._logger.debug("Found {} keyspaces: {}".format(len(formatted_output), formatted_output))

        if self._blacklist:
            self._logger.warning("Blacklisting {} keyspaces, they will not be repaired.".format(self._blacklist))
            for keyspace in self._blacklist:
                formatted_output.remove(keyspace)

        return sorted(formatted_output)
    
    def _get_columnfamilies(self, keyspace):
        cmd = ['cqlsh', self._cqlsh_ip, '-e', "select columnfamily_name from system.schema_columnfamilies WHERE keyspace_name='{}';".format(keyspace)]
        output = self._decode(subprocess.check_output(cmd)).strip().split('\n')
        formatted_output = map(str.strip, output[2:-2])
        return formatted_output
    
    def _check_for_nodetool(self):
        ''' Ensure nodetool command exists. '''
        if not os.path.isfile(self._nodetool):
            self._logger.critical("Unable to find nodetool command at %s" % nodetool)
            exit(1)

    def _add_failure(self, job):
        f = "{}/{}.{}".format(job.host, job.keyspace, job.cf)
        self._failures.append(f)
        self._redis.set("REPAIR_FAILED_JOBS", json.dumps(self._failures))

    def repair_all(self):
        self._logger.info("Starting a full repair.")
        self._logger.debug("Repairing {} keyspaces.".format(self._keyspace_map.keys()))

        self._start_time = time.time()
        self._redis.set('REPAIR_START_TIME', self._start_time)

        for keyspace, columnfamilies in self._keyspace_map.items():
            for cf in columnfamilies:
                for host in self._hostlist:

                    # Run the repair
                    self._redis.set("REPAIR_CURRENT_JOB", "{}/{}.{}".format(host, keyspace, cf))
                    job = RepairJob(host, keyspace, cf, self._default_timeout, self._retries)
                    result = job.run()
                    self._redis.delete("REPAIR_CURRENT_JOB")

                    if result.status is RepairJobStatus.SUCCESS:
                        self._logger.info("{} succeeded for {}.{} with {} failures".format(host, keyspace, cf, result.failures))
                    elif result.status is RepairJobStatus.FAILED:
                        self._add_failure(job)
                    elif result.status is RepairJobStatus.TIMEOUT:
                        self._add_failure(job)
                        self._logger.debug("{} timed out for {}.{}".format(host, keyspace, cf))
                    else:
                        self._logger.critical("Unknown job result on {} for {}.{}: {}".format(host, keyspace, cf, result))

                    time.sleep(self._cluster_pause) # Give the cluster a few seconds to settle down between repairs.

        total_time = time.time() - self._start_time
        self._redis.set("REPAIR_TOTAL_TIME", total_time)

        if self._failures:
            self._redis.set("REPAIR_STATUS", "error")
            self._logger.error("Repair completed with {} failures in {} seconds.".format(len(self._failures), str(timedelta(seconds=total_time))))
            for f in self._failures:
                self._logger.error("Failed: {}".format(f))
        else:
            self._redis.set("REPAIR_STATUS", "complete")
            self._redis.set("REPAIR_LAST_SUCCESSFUL_RUN", time.time())
            self._logger.info("Repair completed in {} seconds.".format(str(timedelta(seconds=total_time))))


class RepairJobStatus(Enum):
        SUCCESS = 0
        FAILED = 1
        TIMEOUT = 2


class RepairJobResult():
    def __init__(self, status, elapsed_time):
        self.status = status
        self.elapsed_time = elapsed_time
        self.failures = 0

    def __str__(self):
        return "{} in {} sec with {} failures.".format(self.status, self.elapsed_time, self.failures)


class RepairJob():
    def __init__(self, host, keyspace, columnfamily, timeout=3600, retries=3):
        self.host = host
        self.keyspace = keyspace
        self.cf = columnfamily
        self._timeout = timeout
        self._retries = retries
        self._FNULL = open(os.devnull, 'w')
        self._attempts = 0
        self._start_time = None
        self._failure_pause = 1
        self.status = None
        self.total_time = 0

    def _elapsed_time(self):
        return time.time() - self._start_time

    def _update_time(self):
        self.total_time += self._elapsed_time()

    def run(self):
        cmd = ['nodetool', '-h', self.host, 'repair', '-pr', self.keyspace, self.cf]
        while self._attempts <= self._retries:
            logging.debug("{}/{}.{} starting attempt {} with {} sec timeout".format(self.host, self.keyspace, self.cf, self._attempts + 1, self._timeout))
            try:
                self._start_time = time.time()
                subprocess.check_call(cmd, stdout=self._FNULL, stderr=subprocess.STDOUT, timeout=self._timeout)
                self._update_time()
                logging.debug("{}/{}.{} completed in {} sec attempt {}".format(self.host, self.keyspace, self.cf, self._elapsed_time(), self._attempts + 1))
                return RepairJobResult(RepairJobStatus.SUCCESS, self.total_time)

            except subprocess.TimeoutExpired as err:
                # NOTE: Timeout exceptions are not retried. This might change in the future.
                self._update_time()
                logging.error("{}/{}.{} timeout in {} sec attempt {}".format(self.host, self.keyspace, self.cf, self._elapsed_time(), self._attempts + 1))
                return RepairJobResult(RepairJobStatus.TIMEOUT, self.total_time)

            except subprocess.CalledProcessError as err:
                logging.warning("{}/{}.{} failed in {} sec attempt {}".format(self.host, self.keyspace, self.cf, self._elapsed_time(), self._attempts + 1))
                self._attempts += 1
                self._update_time()
                time.sleep(self._failure_pause)

        logging.error("{}/{}.{} failed in {} sec, retries exhausted.".format(self.host, self.keyspace, self.cf, self.total_time))
        return RepairJobResult(RepairJobStatus.FAILED, self.total_time)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Cassandra Repair Utility')
    parser.add_argument('--config', dest='config_file', default='config.yaml')
    args = parser.parse_args()

    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)

    repair_manager = RepairManager(args.config_file)
    repair_manager.repair_all()
