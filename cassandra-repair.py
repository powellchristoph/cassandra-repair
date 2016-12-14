#!/opt/cassandra-repair/.venv/bin/python

import argparse
import os
import subprocess
import time
import yaml

from datetime import timedelta
from socket import gethostname
from sys import argv, exit

failures = []
total_time = 0.0

FNULL = open(os.devnull, 'w')

def read_config(config_file):
    ''' Load the configuration file. '''
    with open(config_file, 'r') as f:
        try:
           config = yaml.load(f)
        except yaml.YAMLError as exc:
            print 'ERROR: Invalid yaml, unable to load config file.'
            print exc
            exit(1)

    options = {}
    options['nodetool'] = config.get('nodetool_path', '/usr/bin/nodetool')
    options['hostlist'] = config.get('hosts')
    options['keyspaces'] = config.get('keyspaces')
    options['retries'] = config.get('retries', '5')
    options['blacklist'] = config.get('blacklist', [])
    options['ip'] = config.get('ip', '127.0.0.1')
    return options

def get_keyspaces(ip):
    cmd = ['cqlsh', ip, '-e', 'DESC KEYSPACES']
    return subprocess.check_output(cmd).strip().split()

def get_columnfamilies(ip, keyspace):
    cmd = ['cqlsh', ip, '-e', "select columnfamily_name from system.schema_columnfamilies WHERE keyspace_name='%s';" % keyspace]
    output = subprocess.check_output(cmd).strip().split('\n')
    return map(str.strip, output[2:-2])

def check_for_nodetool(nodetool):
    ''' Ensure nodetool command exists. '''
    if not os.path.isfile(nodetool):
        print "ERROR: Unable to find nodetool command at %s" % nodetool
        exit(1)

def run_repair(host, keyspace, cf, retries=3):
    global total_time
    attempts = 0
    cmd = ['nodetool', '-h', host, 'repair', '-pr', keyspace, cf]

    while attempts < retries:
        try:
            start_time = time.time()
            subprocess.check_call(cmd, stdout=FNULL, stderr=subprocess.STDOUT)
            success = True
            break
        except subprocess.CalledProcessError as err:
            attempts += 1
            success = False
            elapsed_time = time.time() - start_time
            total_time += elapsed_time
            print "%s: Repair attempt %s on %s FAILED after %s sec." % (host, str(attempts), keyspace, str(elapsed_time))
            time.sleep(5)

    if success:
        elapsed_time = time.time() - start_time
        total_time += elapsed_time
        print "%s completed in %s sec" % (host, str(elapsed_time))
    else:
        # TODO: Do something here eventually. Alert/notify.
        failures.append("%s: %s.%s" % (host, keyspace, cf))
        print "Repair attempts exhausted. Repair failed for %s.%s" % (host, keyspace)



if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Cassandra Repair Utility')
    parser.add_argument('--config', dest='config_file', default='config.yaml')
    args = parser.parse_args()

    options = read_config(args.config_file)

    check_for_nodetool(options['nodetool'])

    keyspaces = sorted(get_keyspaces(options['ip']))

    print "#### Starting repair ####"

    for keyspace in keyspaces:
        columnfamilies = get_columnfamilies(options['ip'], keyspace)
        if keyspace == 'system':
            # Do not repair system
            continue
        elif keyspace in options['blacklist']:
            print "%s blacklisted, skipping..." % keyspace
            continue
        else:
            for cf in columnfamilies:
                print "\nRepairing %s.%s..." % (keyspace, cf)
                for host in options['hostlist']:
                    run_repair(host, keyspace, cf, options['retries'])

    print "\n\n###############################"
    print "Repair total time: %s" % str(timedelta(seconds=total_time))
    print "There were {} failures".format(len(failures))
    print "\n".join(failures)
