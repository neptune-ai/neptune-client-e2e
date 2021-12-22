# `neptune-client` e2e tests

See [tested library](https://github.com/neptune-ai/neptune-client).

Some tests/test groups are tagged with categories:
* `integrations` - tests of client with integrations (pytorch_lightning, fastai etc.)
* `s3` - artifact tests using s3 storage
* `management` - tests of management API

You can opt to execute (or omit) certain groups using `pytest`'s [marker-command](https://docs.pytest.org/en/6.2.x/example/markers.html):
* `pytest -m "integrations"` - run only integrations tests
* `pytest -m "not integrations and not management"` - omit integrations and management API tests
