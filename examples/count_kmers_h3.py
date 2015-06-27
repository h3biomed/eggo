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

"""Luigi workflow to convert bams to ADAM and count kmers"""

import luigi
from luigi import Parameter
from luigi.s3 import S3Target, S3PathTask

from eggo.dag import ADAMBasicTask


class ADAMTransformTask(ADAMBasicTask):

    source_uri = Parameter()

    @property
    def adam_command(self):
        return 'transform {source} {target}'.format(
            source=self.input().path,
            target=self.output().path)

    def requires(self):
        return S3PathTask(path=self.source_uri)

    def output(self):
        return S3Target(self.source_uri.replace('/mapsplice/', '/adam/').replace('.bam', '.adam'))


class CountKmersTask(ADAMBasicTask):

    source_uri = Parameter()
    kmer_length = Parameter(21)

    @property
    def adam_command(self):
        return 'count_kmers {source} {target} {kmer_length}'.format(
            source=self.input().path,
            target=self.output().path,
            kmer_length=self.kmer_length)

    def requires(self):
        return ADAMTransformTask(source_uri=self.source_uri)

    def output(self):
        return S3Target(self.source_uri.replace('/level_2/mapsplice/', '/level_3/kmer/').replace('.bam', '.kmer'))


class BAM2FastQTask(ADAMBasicTask):

    source_uri = Parameter()

    @property
    def adam_command(self):
        return 'adam2fastq {source} {target_1} {target_2}'.format(
            source=self.input().path,
            target_1=self.output()[0].path,
            target_2=self.output()[1].path)

    def requires(self):
        return ADAMTransformTask(source_uri=self.source_uri)

    def output(self):
        return [S3Target(self.source_uri.replace('level_2/mapsplice/', 'level_1/fq/').replace('.adam', '_1.fq.gz')),
                S3Target(self.source_uri.replace('level_2/mapsplice/', 'level_1/fq/').replace('.adam', '_2.fq.gz'))
                ]


if __name__ == '__main__':
    from boto import connect_s3
    s3 = connect_s3()
    bucket = s3.get_bucket('h3bioinf-data')
    bam_keys = [k for k in bucket.list(prefix='GE0189/rnaseq_v2/level_2/mapsplice/') if k.name.endswith('.bam')]
    for key in bam_keys:
        luigi.build([CountKmersTask(source_uri='s3n://h3bioinf-data/' + key.name),
                     BAM2FastQTask(source_uri='s3n://h3bioinf-data/' + key.name)],
                     scheduler_host='ec2-54-159-36-206.compute-1.amazonaws.com')
