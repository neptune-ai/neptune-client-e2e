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

import neptune_pytorch_lightning
import pytorch_lightning as pl
import torch
from neptune.new.integrations.pytorch_lightning import NeptuneLogger
from torch.nn import functional as F
from torch.utils.data import DataLoader
from torchvision import transforms
from torchvision.datasets import MNIST

from tests.integrations.common import does_series_converge

PARAMS = {
    'max_epochs': 3,
    'learning_rate': 0.005,
    'batch_size': 32,
}


class LitModel(pl.LightningModule):
    def __init__(self):
        super().__init__()
        self.lin1 = torch.nn.Linear(28 * 28, 10)

    def forward(self, x):
        # pylint: disable=arguments-differ
        return torch.relu(self.lin1(x.view(x.size(0), -1)))  # pylint: disable=no-member

    def training_step(self, batch, batch_idx):
        # pylint: disable=arguments-differ
        x, y = batch
        y_hat = self(x)
        loss = F.cross_entropy(y_hat, y)
        self.log('train_loss', loss)
        return loss

    def configure_optimizers(self):
        return torch.optim.Adam(self.parameters(), lr=PARAMS['learning_rate'])


class TestPytorchLightning:
    def test_learn(self):
        # given (Subject)
        neptune_logger = NeptuneLogger(
            close_after_fit=False,
        )
        trainer = pl.Trainer(max_epochs=PARAMS['max_epochs'],
                             logger=neptune_logger)

        # and
        model = LitModel()
        train_loader = DataLoader(MNIST(os.getcwd(), download=True, transform=transforms.ToTensor()),
                                  batch_size=PARAMS['batch_size'])

        # when
        trainer.fit(model, train_loader)
        run = neptune_logger.run

        # then
        # correct integration version is logged
        logged_version = run['source_code/integrations/neptune-pytorch-lightning'].fetch()
        assert logged_version == neptune_pytorch_lightning.__version__

        # epoch are logged in steps [1, 1, ...., 2, 2, ..., 3, 3 ...]
        logged_epochs = list(run['metrics/epoch'].fetch_values()['value'])
        assert sorted(logged_epochs) == logged_epochs
        assert set(logged_epochs) == {0, 1, 2}

        # does train_loss converge?
        training_loss = list(run['metrics/train_loss'].fetch_values()['value'])
        # assert len(logged_epochs) == len(training_loss)
        assert does_series_converge(training_loss)
