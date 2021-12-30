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
from faker import Faker
from datetime import datetime
import pytest

from neptune.management import (
    get_project_list,
    get_project_member_list,
    get_workspace_member_list,
    add_project_member,
    create_project,
    delete_project,
    remove_project_member,
    MemberRole,
    ProjectVisibility
)
from neptune.management.internal.utils import normalize_project_name
from tests.base import BaseE2ETest
from tests.conftest import Environment

fake = Faker()


@pytest.mark.management
class TestManagement(BaseE2ETest):
    def test_standard_scenario(self, environment: Environment):
        project_slug = fake.slug()
        project_name: str = f"temp-{datetime.now().strftime('%Y%m%d')}-{project_slug}"
        project_key: str = project_slug.replace("-", "")[:3].upper()
        project_identifier: str = normalize_project_name(name=project_name, workspace=environment.workspace)

        assert project_identifier not in get_project_list(api_token=environment.admin_token)
        assert environment.user in get_workspace_member_list(
            name=environment.workspace,
            api_token=environment.admin_token
        )
        assert get_workspace_member_list(
            name=environment.workspace,
            api_token=environment.admin_token
        ).get(environment.user) == 'member'

        created_project_identifier = create_project(
            name=project_name,
            key=project_key,
            visibility=ProjectVisibility.PRIVATE,
            workspace=environment.workspace,
            api_token=environment.admin_token
        )

        assert created_project_identifier == project_identifier
        assert created_project_identifier in get_project_list(api_token=environment.admin_token)
        assert environment.user not in get_project_member_list(
            name=created_project_identifier,
            api_token=environment.admin_token
        )

        add_project_member(
            name=created_project_identifier,
            username=environment.user,
            # pylint: disable=no-member
            role=MemberRole.CONTRIBUTOR,
            api_token=environment.admin_token
        )

        assert environment.user in get_project_member_list(
            name=created_project_identifier,
            api_token=environment.admin_token
        )
        assert get_project_member_list(
            name=created_project_identifier,
            api_token=environment.admin_token
        ).get(environment.user) == 'contributor'
        assert created_project_identifier in get_project_list(api_token=environment.user_token)

        remove_project_member(
            name=created_project_identifier,
            username=environment.user,
            api_token=environment.admin_token
        )

        assert environment.user not in get_project_member_list(
            name=created_project_identifier,
            api_token=environment.admin_token
        )

        delete_project(
            name=created_project_identifier,
            api_token=environment.admin_token
        )

        assert project_identifier not in get_project_list(api_token=environment.admin_token)
