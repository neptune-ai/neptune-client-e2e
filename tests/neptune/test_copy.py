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
import pytest
from faker import Faker

import neptune.new as neptune
from neptune.new.run import Run
from neptune.new.project import Project

from tests.base import BaseE2ETest

fake = Faker()


class TestCopying(BaseE2ETest):
    @pytest.mark.parametrize('container', ['run'], indirect=True)
    @pytest.mark.parametrize("value", [random.randint(0, 100), random.random(), fake.boolean(), fake.word()])
    def test_copy_project_attr_to_run(self, container: Run, value):
        project = neptune.init_project()
        src, destination, destination2 = self.gen_key(), self.gen_key(), self.gen_key()

        project[src] = value
        container[destination] = project[src]

        project[src].log("One Extra")

        assert project[src] == value
        assert container[destination] == value
        assert container[destination2] == value

    @pytest.mark.parametrize('container', ['project'], indirect=True)
    @pytest.mark.parametrize("value", [random.randint(0, 100), random.random(), fake.boolean(), fake.word()])
    def test_copy_run_attr_to_project(self, container: Project, value):
        project = neptune.init_project()
        src, destination, destination2 = self.gen_key(), self.gen_key(), self.gen_key()

        container[src] = value
        project[destination] = container[src]
        project[destination2] = project[destination]

        project[src].log("One Extra")

        assert container[src] == value
        assert project[destination] == value
        assert project[destination2] == value
