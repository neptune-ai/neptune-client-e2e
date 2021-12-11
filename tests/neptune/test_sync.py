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
import json
import re
from pathlib import Path

import neptune.new as neptune
from click.testing import CliRunner
from faker import Faker
from neptune.new.sync import sync

from tests.base import BaseE2ETest
from tests.utils import DISABLE_SYSLOG_KWARGS, tmp_context

fake = Faker()
runner = CliRunner()


class TestSync(BaseE2ETest):
    SYNCHRONIZED_SYSID_RE = r"\w+/[\w-]+/([\w-]+)"

    def test_sync(self):
        custom_run_id = "-".join((fake.word() for _ in range(3)))

        with tmp_context() as tmp:
            # with test values
            key = self.gen_key()
            original_value = fake.word()
            updated_value = fake.word()

            # init run
            run = neptune.init(
                custom_run_id=custom_run_id,
                **DISABLE_SYSLOG_KWARGS,
            )

            # assign original value
            run[key] = original_value
            run.sync()

            # stop run
            run.stop()

            # pylint: disable=protected-access
            queue_dir = list(Path(f"./.neptune/async/{run._id}/").glob("exec-*"))[0]
            with open(queue_dir / "last_put_version") as last_put_version_f:
                last_put_version = int(last_put_version_f.read())
            with open(queue_dir / "data-1.log", "a") as queue_f:
                queue_f.write(
                    json.dumps({
                        "obj": {
                            "type": "AssignString",
                            "path": key.split("/"),
                            "value": updated_value
                        },
                        "version": last_put_version + 1
                    })
                )
            with open(queue_dir / "last_put_version", "w") as last_put_version_f:
                last_put_version_f.write(str(last_put_version + 1))

            # other exp should see only original value from server
            exp2 = neptune.init(
                custom_run_id=custom_run_id,
                **DISABLE_SYSLOG_KWARGS,
            )
            assert exp2[key].fetch() == original_value

            # run neptune sync
            result = runner.invoke(sync, ["--path", tmp])
            assert result.exit_code == 0

            # other exp should see updated value from server
            with tmp_context():
                exp3 = neptune.init(
                    custom_run_id=custom_run_id,
                    **DISABLE_SYSLOG_KWARGS,
                )
                assert exp3[key].fetch() == updated_value

    def test_offline_sync(self):
        with tmp_context() as tmp:
            run = neptune.init(
                mode="offline",
                **DISABLE_SYSLOG_KWARGS,
            )
            key = self.gen_key()
            val = fake.word()
            run[key] = val

            run.stop()

            result = runner.invoke(sync, ["--path", tmp])
            assert result.exit_code == 0

            # offline mode doesn't support custom_run_id, we'll have to parse sync output to determine short_id
            sys_id_found = re.search(self.SYNCHRONIZED_SYSID_RE, result.stdout)
            assert len(sys_id_found.groups()) == 1
            sys_id = sys_id_found.group(1)

            run2 = neptune.init(run=sys_id)
            assert run2[key].fetch() == val
