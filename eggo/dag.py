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

"""Luigi tasks etc for implementing ADAM ETL jobs."""

import os
import sys
import json
from time import sleep
from shutil import rmtree
from tempfile import mkdtemp
from subprocess import Popen

from luigi import Task, Config
from luigi.s3 import S3Target, S3FlagTarget, S3Client
from luigi.hdfs import HdfsClient, HdfsTarget
from luigi.hadoop import JobTask, HadoopJobRunner
from luigi.parameter import Parameter

CGHUB_PUBLIC_KEY = 'https://cghub.ucsc.edu/software/downloads/cghub_public.key'

from eggo.config import (
    validate_config, EGGO_S3_BUCKET_URL, EGGO_S3N_BUCKET_URL, EGGO_S3_RAW_URL,
    EGGO_S3N_RAW_URL, EGGO_S3_TMP_URL)
from eggo.util import random_id, build_s3_filename


def raw_data_s3_url(dataset_name):
    return os.path.join(EGGO_S3_RAW_URL, dataset_name) + '/'


def raw_data_s3n_url(dataset_name):
    return os.path.join(EGGO_S3N_RAW_URL, dataset_name) + '/'


def target_s3_url(dataset_name, format='bdg', edition='basic'):
    return os.path.join(EGGO_S3_BUCKET_URL, dataset_name, format, edition) + '/'


def target_s3n_url(dataset_name, format='bdg', edition='basic'):
    return os.path.join(EGGO_S3N_BUCKET_URL, dataset_name, format, edition) + '/'


def dataset_s3n_url(dataset_name):
    return os.path.join(EGGO_S3N_BUCKET_URL, dataset_name) + '/'


class JsonFileParameter(Parameter):

    def parse(self, p):
        with open(p, 'r') as ip:
            json_data = json.load(ip)
        validate_config(json_data)
        return json_data


class ToastConfig(Config):
    config = JsonFileParameter()


def _cghub_download(url, tmp_dir, cghub_key=None, n_threads=8):
    """Download from CGHub to TMP_DIR.

    Requires GeneTorrent. Download client `gtdownload` must be
    on PATH. Use public key if none provided and CGHUB_KEY
    environment variable not set. Returns analysis subdirectory.
    """
    # 1. Check env for CGHub key and substitute public if necessary
    if cghub_key is None:
        cghub_key = os.environ.get('CGHUB_KEY') or CGHUB_PUBLIC_KEY

    # 2. Parse url for analysis ID and filename
    dummy, analysis_id, filename = url.split('/')

    # 3. Download with gtdownload
    cmd = 'gtdownload -c {keypath} -p {prefix} --max-children {threads} -v {analysis_id}'
    p = Popen(cmd.format(keypath=cghub_key, prefix=tmp_dir, threads=n_threads,
                         analysis_id=analysis_id), shell=True)
    p.wait()

    return os.path.join(tmp_dir, analysis_id)


def _http_download(url, tmp_dir):
    """Download URL via HTTP

    Requires curl
    """
    dnload_cmd = 'pushd {tmp_dir} && curl -L -O {source} && popd'
    p = Popen(dnload_cmd.format(tmp_dir=tmp_dir, source=url),
              shell=True)
    p.wait()


def _dnload_to_local_upload_to_s3(source, destination, compression):
    # source: (string) URL suitable for curl
    # destination: (string) full S3 path of destination file name
    # compression: (bool) whether file needs to be decompressed
    try:
        EPHEMERAL_MOUNT = os.environ.get('EPHEMERAL_MOUNT', '/mnt')
        tmp_dir = mkdtemp(prefix='tmp_eggo_', dir=EPHEMERAL_MOUNT)

        # 1. dnload file
        if source.startswith('http'):
            _http_download(source, tmp_dir)
        elif source.startswith('cghub'):
            tmp_dir = _cghub_download(source, tmp_dir)
        else:
            raise ValueError('source must be http(s) or cghub url')

        # 2. decompress if necessary
        if compression:
            compression_type = os.path.splitext(source)[-1]
            if compression_type == '.gz':
                decompr_cmd = ('pushd {tmp_dir} && gunzip *.gz && popd')
            else:
                raise ValueError("Unknown compression type: {0}".format(
                    compression_type))
            p = Popen(decompr_cmd.format(tmp_dir=tmp_dir), shell=True)
            p.wait()

        # 3. upload to tmp S3 location
        tmp_s3_path = os.path.join(EGGO_S3_TMP_URL, random_id())
        upload_cmd = 'pushd {tmp_dir} && aws s3 cp ./* {s3_path} && popd'
        p = Popen(upload_cmd.format(tmp_dir=tmp_dir, s3_path=tmp_s3_path),
                  shell=True)
        p.wait()

        # 4. rename to final target location
        rename_cmd = 'aws s3 mv {tmp_path} {final_path}'
        p = Popen(rename_cmd.format(tmp_path=tmp_s3_path,
                                    final_path=destination),
                  shell=True)
        p.wait()
    except:
        raise
    finally:
        rmtree(tmp_dir)


def create_SUCCESS_file(s3_path):
    s3client = S3Client(os.environ['AWS_ACCESS_KEY_ID'],
                        os.environ['AWS_SECRET_ACCESS_KEY'])
    s3client.put_string('', os.path.join(s3_path, '_SUCCESS'))


class DownloadFileToS3Task(Task):
    """Download a file, decompress, and move to S3."""

    source = Parameter()  # string: URL suitable for curl
    target = Parameter()  # string: full S3 path of destination file name
    compression = Parameter()  # bool: whether file needs to be decompressed

    def run(self):
        _dnload_to_local_upload_to_s3(
            self.source, self.target, self.compression)

    def output(self):
        return S3Target(path=self.target)


class DownloadDatasetTask(Task):
    # downloads the files serially in the scheduler

    destination = Parameter()  # full S3 prefix to put data

    def requires(self):
        for source in ToastConfig().config['sources']:
            dest_name = build_s3_filename(source['url'],
                                          decompress=source['compression'])
            yield DownloadFileToS3Task(
                source=source['url'],
                target=os.path.join(self.destination, dest_name),
                compression=source['compression'])

    def run(self):
        create_SUCCESS_file(self.destination)

    def output(self):
        return S3FlagTarget(self.destination)


class PrepareHadoopDownloadTask(Task):
    hdfs_path = Parameter()

    def run(self):
        try:
            EPHEMERAL_MOUNT = os.environ.get('EPHEMERAL_MOUNT', '/mnt')
            tmp_dir = mkdtemp(prefix='tmp_eggo_', dir=EPHEMERAL_MOUNT)

            # build the remote command for each source
            tmp_command_file = '{0}/command_file'.format(tmp_dir)
            with open(tmp_command_file, 'w') as command_file:
                for source in ToastConfig().config['sources']:
                    command_file.write('{source}\n'.format(
                        source=json.dumps(source)))

            # 3. Copy command file to Hadoop filesystem
            hdfs_client = HdfsClient()
            hdfs_client.put(tmp_command_file, self.hdfs_path)
        except:
            raise
        finally:
            rmtree(tmp_dir)

    def output(self):
        return HdfsTarget(path=self.hdfs_path)


class DownloadDatasetHadoopTask(JobTask):
    destination = Parameter()  # full S3 prefix to put data
    tmp_dir = Parameter(default=random_id(prefix='tmp_eggo_cmds'))

    def requires(self):
        # TODO: get a proper temp file here
        tmp_hdfs_path = '/tmp/{tmp_dir}'.format(tmp_dir=self.tmp_dir)
        return PrepareHadoopDownloadTask(hdfs_path=tmp_hdfs_path)

    def job_runner(self):
        addl_conf = {'mapred.map.tasks.speculative.execution': 'false',
                     'mapred.task.timeout': 12000000}
        return HadoopJobRunner(streaming_jar=os.environ['STREAMING_JAR'],
                               jobconfs=addl_conf,
                               input_format='org.apache.hadoop.mapred.lib.NLineInputFormat',
                               output_format='org.apache.hadoop.mapred.lib.NullOutputFormat',
                               end_job_with_atomic_move_dir=False)

    def mapper(self, line):
        source = json.loads('\t'.join(line.split('\t')[1:]))
        dest_name = build_s3_filename(source['url'],
                                      decompress=source['compression'])
        dest_url = os.path.join(self.destination, dest_name)
        s3client = S3Client(os.environ['AWS_ACCESS_KEY_ID'],
                            os.environ['AWS_SECRET_ACCESS_KEY'])
        if not s3client.exists(dest_url):
            _dnload_to_local_upload_to_s3(
                source['url'], dest_url, source['compression'])

        yield (source['url'], 1)  # dummy output

    def output(self):
        return S3FlagTarget(self.destination.replace('s3:', 's3n:'))


class DeleteDatasetTask(Task):

    def run(self):
        hadoop_home = os.environ.get('HADOOP_HOME', '/root/ephemeral-hdfs')
        delete_raw_cmd = '{hadoop_home}/bin/hadoop fs -rm -r {raw} {target}'.format(
            hadoop_home=hadoop_home, raw=raw_data_s3n_url(ToastConfig().config['name']),
            target=dataset_s3n_url(ToastConfig().config['name']))
        p = Popen(delete_raw_cmd, shell=True)
        p.wait()


class ADAMBasicTask(Task):

    adam_command = Parameter()
    allowed_file_formats = Parameter()
    edition = 'basic'

    def requires(self):
        return DownloadDatasetHadoopTask(destination=raw_data_s3_url(ToastConfig().config['name']))

    def run(self):
        format = ToastConfig().config['sources'][0]['format'].lower()
        if format not in self.allowed_file_formats:
            raise ValueError("Format '{0}' not in allowed formats {1}.".format(
                format, self.allowed_file_formats))

        # 1. Copy the data from S3 to Hadoop's default filesystem
        tmp_hadoop_path = '/tmp/{rand_id}.{format}'.format(rand_id=random_id(),
                                                           format=format)
        distcp_cmd = '{hadoop_home}/bin/hadoop distcp {source} {target}'.format(
            hadoop_home=os.environ['HADOOP_HOME'],
            source=raw_data_s3n_url(ToastConfig().config['name']), target=tmp_hadoop_path)
        p = Popen(distcp_cmd, shell=True)
        p.wait()

        # 2. Run the adam-submit job
        adam_cmd = ('{adam_home}/bin/adam-submit --master {spark_master_url} {adam_command}'
                    '    {source} {target}').format(
                        adam_home=os.environ['ADAM_HOME'],
                        spark_master_url=os.environ['SPARK_MASTER_URL'],
                        adam_command=self.adam_command, source=tmp_hadoop_path,
                        target=target_s3n_url(ToastConfig().config['name'],
                                              edition=self.edition))
        p = Popen(adam_cmd, shell=True)
        p.wait()

    def output(self):
        return S3FlagTarget(
            target_s3_url(ToastConfig().config['name'], edition=self.edition))


class ADAMFlattenTask(Task):

    adam_command = Parameter()
    allowed_file_formats = Parameter()
    source_edition = 'basic'
    edition = 'flat'

    def requires(self):
        return ADAMBasicTask(adam_command=self.adam_command,
                             allowed_file_formats=self.allowed_file_formats)

    def run(self):
        adam_cmd = ('{adam_home}/bin/adam-submit --master {spark_master_url} flatten'
                    '    {source} {target}').format(
                        adam_home=os.environ['ADAM_HOME'],
                        spark_master_url=os.environ['SPARK_MASTER_URL'],
                        source=target_s3n_url(ToastConfig().config['name'],
                                              edition=self.source_edition),
                        target=target_s3n_url(ToastConfig().config['name'],
                                              edition=self.edition))
        p = Popen(adam_cmd, shell=True)
        p.wait()

    def output(self):
        return S3FlagTarget(
            target_s3_url(ToastConfig().config['name'], edition=self.edition))


class ToastTask(Task):

    def output(self):
        return S3FlagTarget(
            target_s3_url(ToastConfig().config['name'], edition=self.edition))


class VCF2ADAMTask(Task):

    def requires(self):
        basic = ADAMBasicTask(adam_command='vcf2adam',
                              allowed_file_formats=['vcf'])
        flat = ADAMFlattenTask(adam_command='vcf2adam',
                               allowed_file_formats=['vcf'])
        dependencies = [basic]
        for edition in ToastConfig().config['editions']:
            if edition == 'basic':
                pass # included by default
            elif edition == 'flat':
                dependencies.append(flat)
        return dependencies

    def run(self):
        pass

    def output(self):
        pass


class BAM2ADAMTask(Task, ToastConfig):

    def requires(self):
        basic = ADAMBasicTask(adam_command='transform',
                              allowed_file_formats=['sam', 'bam'])
        flat = ADAMFlattenTask(adam_command='transform',
                               allowed_file_formats=['sam', 'bam'])
        dependencies = [basic]
        for edition in self.config['editions']:
            if edition == 'basic':
                pass # included by default
            elif edition == 'flat':
                dependencies.append(flat)
        return dependencies

