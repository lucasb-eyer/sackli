# Copyright 2025 Google LLC
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

from collections import Counter
import os
from pathlib import Path

import apache_beam as beam
from apache_beam.testing import test_pipeline
from apache_beam.testing import util
import pytest

from sackli.beam import sacklio
import sackli


_OUTPUT_PATHS = (
    'output@2.bagz',
    'output@2.data.bagz',
    'output@*.bagz',
    'output.extra@2.bagz',
    'output@2.bag',
    'output.bag@0',
)


@pytest.mark.parametrize('file_path', _OUTPUT_PATHS)
def test_write_to_bagz(tmp_path: Path, file_path: str) -> None:
  name = os.path.join(tmp_path, file_path)
  records = [b'a', b'b', b'c', b'd']

  with test_pipeline.TestPipeline() as p:
    _ = p | beam.Create(records) | sacklio.WriteToSackli(name)

  reader = sackli.Reader(name[:-2] if name.endswith('@0') else name)
  assert Counter(reader.read()) == Counter(records)


@pytest.mark.parametrize(
    'file_path',
    ('output.txt', 'output@2', 'output@2.bagzext'),
    ids=('no_extension', 'no_sharding', 'bad_extension'),
)
def test_write_to_sackli_invalid_paths(file_path: str) -> None:
  with pytest.raises(ValueError, match=f'File path "{file_path}" must be in the form'):
    sacklio.WriteToSackli(file_path)


@pytest.mark.parametrize('file_path', _OUTPUT_PATHS)
def test_write_and_read(tmp_path: Path, file_path: str) -> None:
  name = os.path.join(tmp_path, file_path)
  records = [b'a', b'b', b'c', b'd']

  with test_pipeline.TestPipeline() as p:
    _ = p | beam.Create(records) | sacklio.WriteToSackli(name)

  with test_pipeline.TestPipeline() as p:
    read_records = p | sacklio.ReadFromSackli(
        name[:-2] if name.endswith('@0') else name
    )
    util.assert_that(read_records, util.equal_to(records))
