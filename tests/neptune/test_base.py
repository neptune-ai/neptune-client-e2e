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
import random
import tempfile
from datetime import datetime, timezone
from zipfile import ZipFile

import pytest
from faker import Faker

from tests.base import BaseE2ETest
from tests.utils import preserve_cwd

fake = Faker()


class TestAtoms(BaseE2ETest):
    @pytest.mark.parametrize("value", [random.randint(0, 100), random.random(), fake.boolean(), fake.word()])
    def test_simple_assign_and_fetch(self, run, value):
        key = self.gen_key()

        run[key] = value
        run.sync()
        assert run[key].fetch() == value

    def test_simple_assign_datetime(self, run):
        key = self.gen_key()
        now = datetime.now()

        run[key] = now
        run.sync()

        # expect truncate to milliseconds and add UTC timezone
        expected_now = now.astimezone(timezone.utc).replace(microsecond=int(now.microsecond / 1000) * 1000)
        assert run[key].fetch() == expected_now

    def test_fetch_non_existing_key(self, run):
        key = self.gen_key()
        with pytest.raises(AttributeError):
            run[key].fetch()

    def test_delete_atom(self, run):
        key = self.gen_key()
        value = fake.name()

        run[key] = value
        run.sync()

        assert run[key].fetch() == value

        del run[key]
        with pytest.raises(AttributeError):
            run[key].fetch()

class TestNamespace(BaseE2ETest):
    def test_reassigning(self, run):
        namespace = self.gen_key()
        key = f"{fake.unique.word()}/{fake.unique.word()}"
        value = fake.name()

        # Assign a namespace
        run[namespace] = {
            f"{key}": value
        }
        run.sync()

        assert run[f"{namespace}/{key}"].fetch() == value

        # Direct reassign internal value
        value = fake.name()
        run[f"{namespace}/{key}"] = value
        run.sync()

        assert run[f"{namespace}/{key}"].fetch() == value

        # Reassigning by namespace
        value = fake.name()
        run[namespace] = {
            f"{key}": value
        }
        run.sync()

        assert run[f"{namespace}/{key}"].fetch() == value

    def test_distinct_types(self, run):
        namespace = self.gen_key()
        key = f"{fake.unique.word()}/{fake.unique.word()}"
        value = random.randint(0, 100)

        run[namespace] = {
            f"{key}": value
        }
        run.sync()

        assert run[f"{namespace}/{key}"].fetch() == value

        new_value = fake.name()

        with pytest.raises(ValueError):
            run[namespace] = {
                f"{key}": new_value
            }
            run.sync()

    def test_delete_namespace(self, run):
        namespace = fake.unique.word()
        key1 = fake.unique.word()
        key2 = fake.unique.word()
        value1 = fake.name()
        value2 = fake.name()

        run[namespace][key1] = value1
        run[namespace][key2] = value2
        run.sync()

        assert run[namespace][key1].fetch() == value1
        assert run[namespace][key2].fetch() == value2

        del run[namespace]
        with pytest.raises(AttributeError):
            run[namespace][key1].fetch()
        with pytest.raises(AttributeError):
            run[namespace][key2].fetch()


class TestStringSet:
    neptune_tags_path = 'sys/tags'

    def test_do_not_accept_non_tag_path(self, run):
        random_path = 'some/path'
        run[random_path].add(fake.unique.word())
        run.sync()

        with pytest.raises(AttributeError):
            # backends accepts `'sys/tags'` only
            run[random_path].fetch()

    def test_add_and_remove_tags(self, run):
        remaining_tag1 = fake.unique.word()
        remaining_tag2 = fake.unique.word()
        to_remove_tag1 = fake.unique.word()
        to_remove_tag2 = fake.unique.word()

        run[self.neptune_tags_path].add(remaining_tag1)
        run[self.neptune_tags_path].add([to_remove_tag1, remaining_tag2])
        run[self.neptune_tags_path].remove(to_remove_tag1)
        run[self.neptune_tags_path].remove(to_remove_tag2)  # remove non existing tag
        run.sync()

        assert run[self.neptune_tags_path].fetch() == {remaining_tag1, remaining_tag2}


class TestFiles(BaseE2ETest):
    def test_file(self, run):
        key = self.gen_key()
        filename = fake.file_name()
        downloaded_filename = fake.file_name()

        with tempfile.TemporaryDirectory() as tmp:
            with preserve_cwd(tmp):
                # create 10MB file
                with open(filename, "wb") as file:
                    file.write(b"\0" * 10 * 2 ** 20)
                run[key].upload(filename)

                run.sync()
                run[key].download(downloaded_filename)

                assert os.path.getsize(downloaded_filename) == 10 * 2 ** 20
                with open(downloaded_filename, "rb") as file:
                    content = file.read()
                    assert len(content) == 10 * 2 ** 20
                    assert content == b"\0" * 10 * 2 ** 20

    def test_fileset(self, run):
        key = self.gen_key()
        filename1 = fake.file_name()
        filename2 = fake.file_name()

        with tempfile.TemporaryDirectory() as tmp:
            with preserve_cwd(tmp):
                # create two 10MB files
                with open(filename1, "wb") as file1, open(filename2, "wb") as file2:
                    file1.write(b"\0" * 10 * 2 ** 20)
                    file2.write(b"\0" * 10 * 2 ** 20)

                # when one file as fileset uploaded
                run[key].upload_files([filename1])

                # then check if will be downloaded
                run.sync()
                run[key].download("downloaded1.zip")

                with ZipFile("downloaded1.zip") as zipped:
                    assert set(zipped.namelist()) == {filename1, "/"}
                    with zipped.open(filename1, "r") as file1:
                        content1 = file1.read()
                        assert len(content1) == 10 * 2 ** 20
                        assert content1 == b"\0" * 10 * 2 ** 20

                # when second file as fileset uploaded
                run[key].upload_files([filename2])

                # then check if both will be downloaded
                run.sync()
                run[key].download("downloaded2.zip")

                with ZipFile("downloaded2.zip") as zipped:
                    assert set(zipped.namelist()) == {filename1, filename2, "/"}
                    with zipped.open(filename1, "r") as file1,\
                            zipped.open(filename2, "r") as file2:
                        content1 = file1.read()
                        content2 = file2.read()
                        assert len(content1) == len(content2) == 10 * 2 ** 20
                        assert content1 == content2 == b"\0" * 10 * 2 ** 20

                # when first file is removed
                run[key].delete_files([filename1])

                # then check if second will be downloaded
                run.sync()
                run[key].download("downloaded3.zip")

                with ZipFile("downloaded3.zip") as zipped:
                    assert set(zipped.namelist()) == {filename2, "/"}
                    with zipped.open(filename2, "r") as file2:
                        content2 = file2.read()
                        assert len(content2) == 10 * 2 ** 20
                        assert content2 == b"\0" * 10 * 2 ** 20
