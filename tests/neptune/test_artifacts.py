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
import tempfile
import time
from pathlib import Path

from faker import Faker

from neptune.new import Run
from tests.base import BaseE2ETest
from tests.utils import with_check_if_file_appears, preserve_cwd

fake = Faker()


class TestArtifacts(BaseE2ETest):
    def test_local_creation(self, run: Run):
        first, second = self.gen_key(), self.gen_key()
        filename = fake.file_name()

        with tempfile.TemporaryDirectory() as tmp:
            with preserve_cwd(tmp):
                with open(filename, 'w', encoding='utf-8') as handler:
                    handler.write(fake.paragraph(nb_sentences=5))

                run[first].track_files('.')
                run[second].track_files(f'file://{tmp}')

                run.sync()

        assert run[first].fetch_hash() == run[second].fetch_hash()
        assert run[first].fetch_files_list() == run[second].fetch_files_list()

    def test_assignment(self, run: Run):
        first, second = self.gen_key(), self.gen_key()
        filename = fake.file_name()

        with tempfile.TemporaryDirectory() as tmp:
            with preserve_cwd(tmp):

                with open(filename, 'w', encoding='utf-8') as handler:
                    handler.write(fake.paragraph(nb_sentences=5))

                run[first].track_files(filename)
                run.wait()
                run[second] = run[first].fetch()
                run.sync()

        assert run[first].fetch_hash() == run[second].fetch_hash()
        assert run[first].fetch_files_list() == run[second].fetch_files_list()

    def test_local_download(self, run: Run):
        first, second = self.gen_key(), self.gen_key()
        filename, filepath = fake.file_name(), fake.file_path(depth=3).lstrip('/')

        with tempfile.TemporaryDirectory() as tmp:
            with preserve_cwd(tmp):

                with open(filename, 'w', encoding='utf-8') as handler:
                    handler.write(fake.paragraph(nb_sentences=5))

                os.makedirs(Path(filepath).parent, exist_ok=True)
                with open(filepath, 'w', encoding='utf-8') as handler:
                    handler.write(fake.paragraph(nb_sentences=5))

                # Relative path
                run[first].track_files(filename)
                # Absolute path
                run[second].track_files(tmp)

                run.sync()

                with tempfile.TemporaryDirectory() as another_tmp:
                    with preserve_cwd(another_tmp):
                        with with_check_if_file_appears(f'artifacts/{filename}'):
                            run[first].download('artifacts/')

                        with with_check_if_file_appears(filepath):
                            run[second].download()

    def test_s3_creation(self, run: Run, bucket):
        first, second = self.gen_key(), self.gen_key()
        filename = fake.file_name()

        bucket_name, s3_client = bucket

        with tempfile.TemporaryDirectory() as tmp:
            with preserve_cwd(tmp):

                with open(filename, 'w', encoding='utf-8') as handler:
                    handler.write(fake.paragraph(nb_sentences=5))

                s3_client.meta.client.upload_file(filename, bucket_name, filename)

        run[first].track_files(f's3://{bucket_name}/{filename}')
        run[second].track_files(f's3://{bucket_name}/')

        run.sync()

        assert run[first].fetch_hash() == run[second].fetch_hash()
        assert run[first].fetch_files_list() == run[second].fetch_files_list()

    def test_s3_download(self, run: Run, bucket):
        first = self.gen_key()
        filename, filepath = fake.file_name(), fake.file_path(depth=3).lstrip('/')

        bucket_name, s3_client = bucket

        with tempfile.TemporaryDirectory() as tmp:
            with preserve_cwd(tmp):

                with open(filename, 'w', encoding='utf-8') as handler:
                    handler.write(fake.paragraph(nb_sentences=5))

                os.makedirs(Path(filepath).parent, exist_ok=True)
                with open(filepath, 'w', encoding='utf-8') as handler:
                    handler.write(fake.paragraph(nb_sentences=5))

                s3_client.meta.client.upload_file(filename, bucket_name, filename)
                s3_client.meta.client.upload_file(filepath, bucket_name, filepath)

        run[first].track_files(f's3://{bucket_name}/')

        run.sync()

        with tempfile.TemporaryDirectory() as tmp:
            with with_check_if_file_appears(f'{tmp}/{filename}'):
                run[first].download(tmp)

        with tempfile.TemporaryDirectory() as tmp:
            with preserve_cwd(tmp):
                with with_check_if_file_appears(filename):
                    run[first].download()

    def test_s3_existing(self, run: Run, bucket):
        first, second = self.gen_key(), self.gen_key()
        filename, filepath = fake.file_name(), fake.file_path(depth=3).lstrip('/')

        bucket_name, s3_client = bucket

        with tempfile.TemporaryDirectory() as tmp:
            with preserve_cwd(tmp):

                with open(filename, 'w', encoding='utf-8') as handler:
                    handler.write(fake.paragraph(nb_sentences=5))

                os.makedirs(Path(filepath).parent, exist_ok=True)
                with open(filepath, 'w', encoding='utf-8') as handler:
                    handler.write(fake.paragraph(nb_sentences=5))

                s3_client.meta.client.upload_file(filename, bucket_name, filename)
                s3_client.meta.client.upload_file(filepath, bucket_name, filepath)

        # Track all files - "a" and "b" to first artifact
        run[first].track_files(f's3://{bucket_name}/')

        # Track only the "a" file to second artifact
        run[second].track_files(f's3://{bucket_name}/{filename}')
        run.sync()

        # Add "b" file to existing second artifact
        # so it should be now identical as first
        run[second].track_files(f's3://{bucket_name}/{filepath}', destination=str(Path(filepath).parent))
        run.sync()

        assert run[first].fetch_hash() == run[second].fetch_hash()
        assert run[first].fetch_files_list() == run[second].fetch_files_list()

    def test_local_existing(self, run: Run):
        first, second = self.gen_key(), self.gen_key()
        filename, filepath = fake.file_name(), fake.file_path(depth=3).lstrip('/')

        with tempfile.TemporaryDirectory() as tmp:
            with preserve_cwd(tmp):
                with open(filename, 'w', encoding='utf-8') as handler:
                    handler.write(fake.paragraph(nb_sentences=5))

                os.makedirs(Path(filepath).parent, exist_ok=True)
                with open(filepath, 'w', encoding='utf-8') as handler:
                    handler.write(fake.paragraph(nb_sentences=5))

                # Track all files - "a" and "b" to first artifact
                run[first].track_files('.')

                # Track only the "a" file to second artifact
                run[second].track_files(f'file://{tmp}/{filename}')
                run.sync()

                # Add "b" file to existing second artifact
                # so it should be now identical as first
                run[second].track_files(filepath, destination=str(Path(filepath).parent))
                run.sync()

        assert run[first].fetch_hash() == run[second].fetch_hash()
        assert run[first].fetch_files_list() == run[second].fetch_files_list()

    def test_hash_cache(self, run: Run):
        key = self.gen_key()
        filename = fake.file_name()

        with tempfile.TemporaryDirectory() as tmp:
            with preserve_cwd(tmp):
                # create 2GB file
                with open(filename, 'wb') as handler:
                    handler.write(b'\0' * 2 * 2 ** 30)

                # track it
                start = time.time()
                run[key].track_files('.', wait=True)
                initial_duration = time.time() - start

                # and track it again
                start = time.time()
                run[key].track_files('.', wait=True)
                retry_duration = time.time() - start

                assert retry_duration * 2 < initial_duration, "Tracking again should be significantly faster"

                # append additional byte to file
                with open(filename, 'ab') as handler:
                    handler.write(b'\0')

                # and track updated file
                start = time.time()
                run[key].track_files('.', wait=True)
                updated_duration = time.time() - start

                assert retry_duration * 2 < updated_duration, "Tracking updated file should take more time - no cache"
