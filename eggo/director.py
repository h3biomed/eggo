# Licensed to Big Data Genomics (BDG) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The BDG licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import itertools
import os
import sys
import time
from tempfile import mkdtemp
from datetime import datetime

import boto.ec2
import boto.cloudformation
from boto.ec2.networkinterface import (
    NetworkInterfaceCollection, NetworkInterfaceSpecification)
from fabric.api import local, env, run, execute, prefix, put, open_shell

from eggo.config import eggo_config


AWS_ACCESS_KEY_ID = eggo_config.get('aws', 'aws_access_key_id')
AWS_SECRET_ACCESS_KEY = eggo_config.get('aws', 'aws_secret_access_key')
EC2_KEY_PAIR = eggo_config.get('aws', 'ec2_key_pair')
EC2_PRIVATE_KEY_FILE = eggo_config.get('aws', 'ec2_private_key_file')

REGION = eggo_config.get('director', 'region')
LAUNCHER_INSTANCE_TYPE = eggo_config.get('director', 'launcher_instance_type')
LAUNCHER_AMI = eggo_config.get('director', 'launcher_ami')
CLUSTER_AMI = eggo_config.get('director', 'cluster_ami')
NUM_WORKERS = eggo_config.get('director', 'num_workers')
STACK_NAME = eggo_config.get('director', 'stack_name')
CLOUDFORMATION_TEMPLATE = eggo_config.get('director', 'cloudformation_template')
DIRECTOR_CONF_TEMPLATE = eggo_config.get('director', 'director_conf_template')

def provision():
    start_time = datetime.now()

    # create cloud formation stack (VPC etc)
    cf_conn = create_cf_connection()
    create_stack(cf_conn, STACK_NAME)

    # create launcher instance
    conn = create_ec2_connection()
    launcher_instance = create_launcher_instance(conn, cf_conn)

    # run bootstrap on launcher
    execute(run_director_bootstrap, hosts=[launcher_instance.ip_address])

    end_time = datetime.now()
    print "Cluster has started. Took {t} minutes.".format(
        t=(end_time - start_time).seconds / 60
    )

def list():
    conn = create_ec2_connection()
    print 'Launcher', get_launcher_instance(conn).ip_address
    print 'Manager', get_manager_instance(conn).ip_address
    print 'Gateway', get_gateway_instance(conn).ip_address
    print 'Master', get_master_instance(conn).ip_address
    for instance in get_worker_instances(conn):
        print 'Worker', instance.ip_address

def get_gateway_host():
    conn = create_ec2_connection()
    return get_gateway_instance(conn).ip_address

def get_worker_hosts():
    conn = create_ec2_connection()
    return [i.ip_address for i in get_worker_instances(conn)]

def login():
    conn = create_ec2_connection()
    hosts = get_gateway_instance(conn).ip_address
    execute(open_shell, hosts=hosts)

def web_proxy(instance_name, port):
    conn = create_ec2_connection()
    instance = get_instances(conn, 'group', instance_name)[0]
    local('ssh -i {private_key} -o UserKnownHostsFile=/dev/null '
          '-o StrictHostKeyChecking=no -L {port}:{cm_private_ip}:{port} '
          'ec2-user@{cm_public_ip}'.format(
        private_key=EC2_PRIVATE_KEY_FILE,
        port=port,
        cm_private_ip=instance.private_ip_address,
        cm_public_ip=instance.ip_address))

def cm_web_proxy():
    web_proxy('manager', 7180)

def hue_web_proxy():
    web_proxy('master', 8888)

def yarn_web_proxy():
    web_proxy('master', 8088)

def teardown():
    # terminate Hadoop cluster (prompts for confirmation)
    conn = create_ec2_connection()
    execute(run_director_terminate, hosts=[get_launcher_instance(conn).ip_address])

    # terminate launcher instance
    terminate_launcher_instance(conn)

    # delete stack
    cf_conn = create_cf_connection()
    delete_stack(cf_conn, STACK_NAME)

def create_cf_connection():
    return boto.cloudformation.connect_to_region(REGION)

def create_ec2_connection():
    return boto.ec2.connect_to_region(REGION)

def create_stack(cf_conn, name):
    try:
        if len(cf_conn.describe_stacks(name)) > 0:
            print "Stack '{n}' already exists. Reusing.".format(n=name)
            return
    except:
        # stack does not exist
        pass
    print "Creating stack with name '{n}'.".format(n=name)
    with open(CLOUDFORMATION_TEMPLATE, 'r') as template_file:
        template_body=template_file.read()
    cf_conn.create_stack(name, template_body=template_body,
                      parameters=[('KeyPairName',EC2_KEY_PAIR)],
                      tags={'owner':EC2_KEY_PAIR})
    wait_for_stack_status(cf_conn, name, 'CREATE_COMPLETE')

def create_launcher_instance(conn, cf_conn):
    launcher_instances = get_instances(conn, 'group', 'launcher')
    if len(launcher_instances) > 0:
        print "Launcher instance ({instance}) already exists. Reusing.".format(
            instance=launcher_instances[0].ip_address)
        return launcher_instances[0]
    print "Creating launcher instance."
    # see http://stackoverflow.com/questions/19029588/how-to-auto-assign-public-ip-to-ec2-instance-with-boto
    interface = NetworkInterfaceSpecification(subnet_id=get_subnet_id(cf_conn),
                                              groups=[get_security_group_id(cf_conn)],
                                              associate_public_ip_address=True)
    interfaces = NetworkInterfaceCollection(interface)

    reservation = conn.run_instances(
        LAUNCHER_AMI,
        key_name=EC2_KEY_PAIR,
        instance_type=LAUNCHER_INSTANCE_TYPE,
        network_interfaces=interfaces)
    instance = reservation.instances[0]
    instance.add_tag('owner', EC2_KEY_PAIR)
    instance.add_tag('group', 'launcher')
    wait_for_instance_state(conn, instance)
    execute(install_director, hosts=[instance.ip_address])
    return instance

def install_director():
    # install Cloudera Director client
    run('sudo wget http://archive.cloudera.com/director/redhat/6/x86_64/director/cloudera'
        '-director.repo -O /etc/yum.repos.d/cloudera-director.repo')
    run('sudo yum -y install cloudera-director-client')

    # copy the private key to the launcher
    put(EC2_PRIVATE_KEY_FILE, 'id.pem')
    run('chmod 600 id.pem')

def run_director_bootstrap():
    # replace variables in conf template and copy to launcher
    cf_conn = create_cf_connection()
    with open(DIRECTOR_CONF_TEMPLATE, 'r') as director_conf_template:
        accessKeyId = AWS_ACCESS_KEY_ID
        secretAccessKey = AWS_SECRET_ACCESS_KEY
        region = REGION
        keyName = EC2_KEY_PAIR
        subnetId = get_subnet_id(cf_conn)
        securityGroupsIds = get_security_group_id(cf_conn)
        image = CLUSTER_AMI
        num_workers = NUM_WORKERS
        director_conf=director_conf_template.read() % locals()
    tmp_dir = mkdtemp(prefix='tmp_eggo_')
    tmp_file = '{0}/aws.conf'.format(tmp_dir)
    with open(tmp_file, 'w') as director_conf_file:
        director_conf_file.write(director_conf)
    put(tmp_file, 'aws.conf')

    # bootstrap the Hadoop cluster
    run('cloudera-director bootstrap aws.conf')

def run_director_terminate():
    run('cloudera-director terminate aws.conf')

def terminate_launcher_instance(conn):
    launcher_instance = get_launcher_instance(conn)
    launcher_instance.terminate()
    wait_for_instance_state(conn, launcher_instance, 'terminated')

def delete_stack(cf_conn, name):
    print "Deleting stack with name '{n}'.".format(n=name)
    cf_conn.delete_stack(name)
    wait_for_stack_status(cf_conn, name, 'DELETE_COMPLETE')

def get_stack_resource_id(cf_conn, logical_resource_id):
    for resource in cf_conn.describe_stack_resources(STACK_NAME):
        if resource.logical_resource_id == logical_resource_id:
            return resource.physical_resource_id
    return None

def get_subnet_id(cf_conn):
    return get_stack_resource_id(cf_conn, 'DMZSubnet')

def get_security_group_id(cf_conn):
    return get_stack_resource_id(cf_conn, 'ClusterSG')

def get_instances(conn, tag_key, tag_value):
    reservations = conn.get_all_reservations(
        filters={'tag:' + tag_key: tag_value})
    instances = itertools.chain.from_iterable(r.instances for r in reservations)
    return [i for i in instances if i.state not in ["shutting-down", "terminated"]]

def get_launcher_instance(conn):
    return get_instances(conn, 'group', 'launcher')[0]

def get_manager_instance(conn):
    return get_instances(conn, 'group', 'manager')[0]

def get_worker_instances(conn):
    return get_instances(conn, 'group', 'worker')

def get_gateway_instance(conn):
    return get_instances(conn, 'group', 'gateway')[0]

def get_master_instance(conn):
    return get_instances(conn, 'group', 'master')[0]

def wait_for_stack_status(cf_conn, name, stack_status):
    sys.stdout.write("Waiting for stack to enter '{s}' state.".format(s=stack_status))
    sys.stdout.flush()

    start_time = datetime.now()
    num_attempts = 0

    while True:
        time.sleep(5 * num_attempts)  # seconds

        try:
            stacks = cf_conn.describe_stacks(name)
        except:
            # stack does not exist
            break

        stack = stacks[0]
        if stack.stack_status == stack_status:
            break

        num_attempts += 1

        sys.stdout.write(".")
        sys.stdout.flush()

    sys.stdout.write("\n")

    end_time = datetime.now()
    print "Stack is now in '{s}' state. Waited {t} seconds.".format(
        s=stack_status,
        t=(end_time - start_time).seconds
    )

def wait_for_instance_state(conn, instance, state='running'):
    sys.stdout.write("Waiting for instance to enter '{s}' state.".format(s=state))
    sys.stdout.flush()

    start_time = datetime.now()
    num_attempts = 0

    while True:
        time.sleep(5 * num_attempts)  # seconds

        instance.update()

        statuses = conn.get_all_instance_status(instance.id)

        if len(statuses) > 0:
            status = statuses[0]
            if instance.state == state and \
                    status.system_status.status == 'ok' and \
                    status.instance_status.status == 'ok':
                break

        num_attempts += 1

        sys.stdout.write(".")
        sys.stdout.flush()

    sys.stdout.write("\n")

    end_time = datetime.now()
    print "Instance is now in '{s}' state. Waited {t} seconds.".format(
        s=state,
        t=(end_time - start_time).seconds
    )

