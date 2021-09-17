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
import concurrent.futures
from datetime import datetime, timezone

import pytest
from faker import Faker
import neptune.new as neptune

from tests.base import BaseE2ETest

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


class TestMultipleRuns:
    def test_multiple_runs_single(self, run: neptune.Run):
        number_of_reinitialized = 5
        namespace = fake.unique.word()

        reinitialized_runs = [neptune.init(run=run._short_id) for _ in range(number_of_reinitialized)]

        run[f'{namespace}/{fake.unique.word()}'] = fake.color()
        run.sync()

        random.shuffle(reinitialized_runs)
        for index, run in enumerate(reinitialized_runs):
            run[f'{namespace}/{fake.unique.word()}'] = fake.color()

        random.shuffle(reinitialized_runs)
        for run in reinitialized_runs:
            run.sync()

        assert len(run[namespace].fetch()) == number_of_reinitialized + 1

    @staticmethod
    def _store_in_run(run_short_id: str, destination: str):
        reinitialized_run = neptune.init(run=run_short_id)
        reinitialized_run[destination] = fake.color()
        reinitialized_run.sync()

    def test_multiple_runs_thread(self, run: neptune.Run):
        number_of_reinitialized = 10
        namespace = fake.unique.word()

        run[f'{namespace}/{fake.unique.word()}'] = fake.color()

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(TestMultipleRuns._store_in_run, run._short_id, f'{namespace}/{fake.unique.word()}')
                for _ in range(number_of_reinitialized)
            ]
            for future in concurrent.futures.as_completed(futures):
                _ = future.result()

        run.sync()

        assert len(run[namespace].fetch()) == number_of_reinitialized + 1

    def test_multiple_runs_processes(self, run: neptune.Run):
        number_of_reinitialized = 10
        namespace = fake.unique.word()

        run[f'{namespace}/{fake.unique.word()}'] = fake.color()

        with concurrent.futures.ProcessPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(TestMultipleRuns._store_in_run, run._short_id, f'{namespace}/{fake.unique.word()}')
                for _ in range(number_of_reinitialized)
            ]
            for future in concurrent.futures.as_completed(futures):
                _ = future.result()

        run.sync()

        assert len(run[namespace].fetch()) == number_of_reinitialized + 1
