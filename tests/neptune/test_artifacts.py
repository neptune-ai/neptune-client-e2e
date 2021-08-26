#
# Copyright (c) 2021, Neptune Labs Sp. z o.o.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import os
import glob
import random
import tempfile
from pathlib import Path
from datetime import datetime, timezone

import boto3
import pytest
from faker import Faker

from neptune import new as neptune
from neptune.new.exceptions import MissingFieldException
from neptune.new import Run
from neptune.new.internal.artifacts.types import ArtifactFileData
from tests.base import BaseE2ETest

fake = Faker()


@pytest.fixture()
def bucket():
    bucket_name = os.environ.get('BUCKET_NAME')
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)

    yield bucket_name, s3

    bucket.objects.all().delete()


class TestArtifacts(BaseE2ETest):
    def test_local_creation(self, run: Run):
        a, b = self.gen_key(), self.gen_key()

        with tempfile.TemporaryDirectory() as tmp:
            with open(f'{tmp}/{fake.file_name()}', 'w') as handler:
                handler.write(fake.paragraph(nb_sentences=5))

            run[a].track_files(tmp)
            run[b].track_files(f'file://{tmp}')

            run.sync()

        assert run[a].fetch_hash() == run[b].fetch_hash()
        assert run[a].fetch_files_list() == run[b].fetch_files_list()

    def test_assignment(self, run: Run):
        a, b = self.gen_key(), self.gen_key()
        filename = fake.file_name()

        with tempfile.TemporaryDirectory() as tmp:
            os.chdir(tmp)

            with open(filename, 'w') as handler:
                handler.write(fake.paragraph(nb_sentences=5))

            run[a].track_files(filename)
            run[b] = run[a].fetch()

            run.sync()

        assert run[a].fetch_hash() == run[b].fetch_hash()
        assert run[a].fetch_files_list() == run[b].fetch_files_list()

    def test_local_download(self, run: Run):
        a, b = self.gen_key(), self.gen_key()
        filename, filepath = fake.file_name(), fake.file_path(depth=3).lstrip('/')

        with tempfile.TemporaryDirectory() as tmp:
            os.chdir(tmp)

            with open(filename, 'w') as handler:
                handler.write(fake.paragraph(nb_sentences=5))

            os.makedirs(Path(filepath).parent, exist_ok=True)
            with open(filepath, 'w') as handler:
                handler.write(fake.paragraph(nb_sentences=5))

            # Relative path
            run[a].track_files(filename)
            # Absolute path
            run[b].track_files(tmp)

            run.sync()

            with tempfile.TemporaryDirectory() as another_tmp:
                os.chdir(another_tmp)

                run[a].download('artifacts/')
                run[b].download('artifacts/')

                assert os.path.exists(f'artifacts/{filename}')
                assert os.path.exists(f'artifacts/{filepath}')

    def test_s3_creation(self, run: Run, bucket):
        a, b = self.gen_key(), self.gen_key()
        filename = fake.file_name()

        bucket_name, s3 = bucket

        with tempfile.TemporaryDirectory() as tmp:
            os.chdir(tmp)

            with open(filename, 'w') as handler:
                handler.write(fake.paragraph(nb_sentences=5))

            s3.meta.client.upload_file(filename, bucket_name, filename)

        run[a].track_files(f's3://{bucket_name}/{filename}')
        run[b].track_files(f's3://{bucket_name}/')

        run.sync()

        assert run[a].fetch_hash() == run[b].fetch_hash()
        assert run[a].fetch_files_list() == run[b].fetch_files_list()

    def test_s3_download(self, run: Run, bucket):
        a = self.gen_key()
        filename, filepath = fake.file_name(), fake.file_path(depth=3).lstrip('/')

        bucket_name, s3 = bucket

        with tempfile.TemporaryDirectory() as tmp:
            os.chdir(tmp)

            with open(filename, 'w') as handler:
                handler.write(fake.paragraph(nb_sentences=5))

            os.makedirs(Path(filepath).parent, exist_ok=True)
            with open(filepath, 'w') as handler:
                handler.write(fake.paragraph(nb_sentences=5))

            s3.meta.client.upload_file(filename, bucket_name, filename)
            s3.meta.client.upload_file(filepath, bucket_name, filepath)

        run[a].track_files(f's3://{bucket_name}/')

        run.sync()

        with tempfile.TemporaryDirectory() as tmp:
            run[a].download(tmp)

            assert os.path.exists(f'{tmp}/{filename}')

    def test_s3_existing(self, run: Run, bucket):
        a, b = self.gen_key(), self.gen_key()
        filename, filepath = fake.file_name(), fake.file_path(depth=3).lstrip('/')

        bucket_name, s3 = bucket

        with tempfile.TemporaryDirectory() as tmp:
            os.chdir(tmp)

            with open(filename, 'w') as handler:
                handler.write(fake.paragraph(nb_sentences=5))

            os.makedirs(Path(filepath).parent, exist_ok=True)
            with open(filepath, 'w') as handler:
                handler.write(fake.paragraph(nb_sentences=5))

            s3.meta.client.upload_file(filename, bucket_name, filename)
            s3.meta.client.upload_file(filepath, bucket_name, filepath)

        run[a].track_files(f's3://{bucket_name}/')
        run[b].track_files(f's3://{bucket_name}/{filename}')
        run.sync()

        # Track to existing
        run[b].track_files(f's3://{bucket_name}/{filepath}', destination=str(Path(filepath).parent))
        run.sync()

        assert run[a].fetch_hash() == run[b].fetch_hash()
        assert run[a].fetch_files_list() == run[b].fetch_files_list()

    def test_local_existing(self, run: Run):
        a, b = self.gen_key(), self.gen_key()
        filename, filepath = fake.file_name(), fake.file_path(depth=3).lstrip('/')

        with tempfile.TemporaryDirectory() as tmp:
            os.chdir(tmp)

            with open(filename, 'w') as handler:
                handler.write(fake.paragraph(nb_sentences=5))

            os.makedirs(Path(filepath).parent, exist_ok=True)
            with open(filepath, 'w') as handler:
                handler.write(fake.paragraph(nb_sentences=5))

            run[a].track_files('.')
            run[b].track_files(f'file://{tmp}/{filename}')
            run.sync()

            # Track to existing
            run[b].track_files(filepath, destination=str(Path(filepath).parent))
            run.sync()

        assert run[a].fetch_hash() == run[b].fetch_hash()
        assert run[a].fetch_files_list() == run[b].fetch_files_list()
