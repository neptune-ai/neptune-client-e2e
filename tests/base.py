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
__all__ = [
    'BaseE2ETest',
]

import uuid

from faker import Faker

from neptune.new.attribute_container import AttributeContainer

fake = Faker()


class BaseE2ETest:
    def cleanup(self, container: AttributeContainer):
        if container.exists(self.__class__.__name__):
            container.pop(self.__class__.__name__)

    def gen_key(self):
        return f'{self.__class__.__name__}/{uuid.uuid4()}'
