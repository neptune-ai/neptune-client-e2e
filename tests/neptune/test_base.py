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
import random
from datetime import datetime, timezone

import pytest
from faker import Faker
import neptune.new as neptune

from tests.base import BaseE2ETest

fake = Faker()


# class TestAtoms(BaseE2ETest):
#     @pytest.mark.parametrize("value", [random.randint(0, 100), random.random(), fake.boolean(), fake.word()])
#     def test_simple_assign_and_fetch(self, run, value):
#         key = self.gen_key()
#
#         run[key] = value
#         run.sync()
#         assert run[key].fetch() == value
#
#     def test_simple_assign_datetime(self, run):
#         key = self.gen_key()
#         now = datetime.now()
#
#         run[key] = now
#         run.sync()
#
#         # expect truncate to milliseconds and add UTC timezone
#         expected_now = now.astimezone(timezone.utc).replace(microsecond=int(now.microsecond / 1000) * 1000)
#         assert run[key].fetch() == expected_now
#
#     def test_fetch_non_existing_key(self, run):
#         key = self.gen_key()
#         with pytest.raises(AttributeError):
#             run[key].fetch()
#
#     def test_delete_atom(self, run):
#         key = self.gen_key()
#         value = fake.name()
#
#         run[key] = value
#         run.sync()
#
#         assert run[key].fetch() == value
#
#         del run[key]
#         with pytest.raises(AttributeError):
#             run[key].fetch()
#
#
# class TestNamespace(BaseE2ETest):
#     def test_reassigning(self, run):
#         namespace = self.gen_key()
#         key = f"{fake.unique.word()}/{fake.unique.word()}"
#         value = fake.name()
#
#         # Assign a namespace
#         run[namespace] = {
#             f"{key}": value
#         }
#         run.sync()
#
#         assert run[f"{namespace}/{key}"].fetch() == value
#
#         # Direct reassign internal value
#         value = fake.name()
#         run[f"{namespace}/{key}"] = value
#         run.sync()
#
#         assert run[f"{namespace}/{key}"].fetch() == value
#
#         # Reassigning by namespace
#         value = fake.name()
#         run[namespace] = {
#             f"{key}": value
#         }
#         run.sync()
#
#         assert run[f"{namespace}/{key}"].fetch() == value
#
#     def test_distinct_types(self, run):
#         namespace = self.gen_key()
#         key = f"{fake.unique.word()}/{fake.unique.word()}"
#         value = random.randint(0, 100)
#
#         run[namespace] = {
#             f"{key}": value
#         }
#         run.sync()
#
#         assert run[f"{namespace}/{key}"].fetch() == value
#
#         new_value = fake.name()
#
#         with pytest.raises(ValueError):
#             run[namespace] = {
#                 f"{key}": new_value
#             }
#             run.sync()
#
#     def test_delete_namespace(self, run):
#         namespace = fake.unique.word()
#         key1 = fake.unique.word()
#         key2 = fake.unique.word()
#         value1 = fake.name()
#         value2 = fake.name()
#
#         run[namespace][key1] = value1
#         run[namespace][key2] = value2
#         run.sync()
#
#         assert run[namespace][key1].fetch() == value1
#         assert run[namespace][key2].fetch() == value2
#
#         del run[namespace]
#         with pytest.raises(AttributeError):
#             run[namespace][key1].fetch()
#         with pytest.raises(AttributeError):
#             run[namespace][key2].fetch()


class TestMultipleRuns:
    def test_one_run_multiple_times(self):
        number_of_reinitialized = 5
        namespace = fake.unique.word()

        main_run = neptune.init()
        main_run[f'{namespace}/0'] = main_run._id
        main_run.sync()

        restored_runs = [neptune.init(run=main_run._short_id) for _ in range(number_of_reinitialized)]

        random.shuffle(restored_runs)
        for index, run in enumerate(restored_runs):
            run[f'{namespace}/{index + 1}'] = run._id

        random.shuffle(restored_runs)
        for run in restored_runs:
            run.sync()

        elements = main_run[namespace].fetch()
        main_run.stop()

        assert len(elements) == number_of_reinitialized + 1
