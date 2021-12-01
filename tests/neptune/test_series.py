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
import tempfile

from PIL import Image
from faker import Faker

from tests.base import BaseE2ETest
from tests.utils import generate_image, image_to_png, preserve_cwd

fake = Faker()


class TestSeries(BaseE2ETest):
    def test_log_numbers(self, run):
        key = self.gen_key()
        values = [random.random() for _ in range(50)]

        run[key].log(values[0])
        run[key].log(values[1:])
        run.sync()

        assert run[key].fetch_last() == values[-1]

        fetched_values = run[key].fetch_values()
        assert list(fetched_values['value']) == values

    def test_log_strings(self, run):
        key = self.gen_key()
        values = [fake.word() for _ in range(50)]

        run[key].log(values[0])
        run[key].log(values[1:])
        run.sync()

        assert run[key].fetch_last() == values[-1]

        fetched_values = run[key].fetch_values()
        assert list(fetched_values['value']) == values

    def test_log_images(self, run):
        key = self.gen_key()
        # images with size between 200KB - 12MB
        images = list(generate_image(size=2 ** n) for n in range(8, 12))

        run[key].log(images[0])
        run[key].log(images[1:])
        run.sync()

        with tempfile.TemporaryDirectory() as tmp:
            with preserve_cwd(tmp):
                run[key].download_last('last')
                run[key].download('all')

                with Image.open("last/3.png") as img:
                    assert img == image_to_png(image=images[-1])

                for i in range(4):
                    with Image.open(f"all/{i}.png") as img:
                        assert img == image_to_png(image=images[i])
