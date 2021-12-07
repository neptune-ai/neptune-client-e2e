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
import pytest
from faker import Faker

import neptune.new as neptune
from neptune.new.run import Run
from neptune.new.project import Project

from tests.base import BaseE2ETest

fake = Faker()


class TestCopying(BaseE2ETest):
    @pytest.mark.parametrize('container', ['run'], indirect=True)
    def test_copy_project_attribute_to_run(self, container: Run):
        project = neptune.init_project()
        values = [self.gen_key() for _ in range(10)]
        src, destination = self.gen_key(), self.gen_key()

        project[src].log(values)
        container[destination] = project[src].fetch()

        project[src].log("One Extra")

        assert list(project[src].fetch_values()['value'].values) == values + ["One Extra"]
        assert list(container[destination].fetch_values()['value'].values) == values

    @pytest.mark.parametrize('container', ['project'], indirect=True)
    def test_copy_run_attribute_to_project(self, container: Project):
        project = neptune.init_project()
        values = [self.gen_key() for _ in range(10)]
        src, destination = self.gen_key(), self.gen_key()

        project[src].log(values)
        container[destination] = project[src].fetch()

        project[src].log("One Extra")

        assert list(project[src].fetch_values()['value'].values) == values + ["One Extra"]
        assert list(container[destination].fetch_values()['value'].values) == values
