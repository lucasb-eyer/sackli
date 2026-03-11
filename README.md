# Säckli

This is a friendly fork of [bagz](https://github.com/google-deepmind/bagz).

Additions so far:
- Merge some PRs such as [S3 support PR by @KefanXIAO](https://github.com/google-deepmind/bagz/pull/5) and [compile fixes](https://github.com/google-deepmind/bagz/pull/4).
- Add `access_pattern` and `cache_policy` reader hints:
    - On POSIX filesystems, this can add `mmap` hints or use `pread`-based no-cache reads to optimize for random access and larger-than-RAM data.
    - On Linux, support `O_DIRECT` for even better reading of random access and larger-than-RAM data.
    - On macOS, support `F_NOCACHE`, `MAP_NOCACHE`, and `madvise`-based cache hints on Apple silicon.
- Make it compatible to Python versions past 3.13.
- Make it compatible with free-threading (nogil) Python.
- Add macOS support and wheels.
- Add CI, stress-tests and automatic wheel releases to PyPI for Linux x86_64 and macOS arm64.

Versioning of this fork is detached from the original bagz library at the point it was forked (v0.2.0).

## Overview

*Säckli* is a format for storing a sequence of byte-array records.
It supports per-record compression and fast index-based lookup.
All indexing is zero based.

## Installation

The recommended installation on Linux and Mac is via the pre-built wheels on PyPI:

```sh
uv pip install sackli
```

If you want to build locally to work on this, just `uv pip install .`.
However, building can be slow because of GCS and S3 support;
to skip both of these dependencies for much faster builds, you can do:

```sh
CMAKE_ARGS="-DSACKLI_ENABLE_GCS=OFF -DSACKLI_ENABLE_S3=OFF" uv pip install .
```

## Python API

### Python Reader

Reader for reading a single or sharded Säckli file-set.

```python
from collections.abc import Sequence, Iterable

import sackli
import numpy as np

# Säckli Readers support random access. The order of elements within a Säckli
# file is the order in which they are written. Records are returned as `bytes`
# objects.
data = sackli.Reader('/path/to/data.bagz')

# Säckli Readers can be configured like this - here we require that the file was
# written with separate limits.
data_separate_limits = sackli.Reader('/path/to/data.bagz', sackli.Reader.Options(
    limits_placement=sackli.LimitsPlacement.SEPARATE,
))

# Säckli Readers are Sequences and support slicing, iterating, etc.
assert isinstance(data, Sequence)

# Säckli Readers have a length.
assert len(data) > 10

# Can access record by row-index.
fifth_value: bytes = data[5]

# Can slice.
data_from_5: sackli.Reader = data[5:]

# Slices are still Readers.
assert isinstance(data_from_5, sackli.Reader)

assert data_from_5[0] == fifth_value

# Can access records by multiple row-indices.
fourth, second, tenth = data.read_indices([4, 2, 10])
assert fourth == data[4]
assert second == data[2]
assert tenth == data[10]

# Can iterate records.
for record in data:
  do_something_else(record)

# Can read all records. This eager version can be faster than iteration.
all_records = data.read()

# Can iterate sub-range of records.
for record in data[4:9]:
  do_something_else(record)

# Can read a sub-range of records. This eager form can be faster than
# iteration.
sub_range = data[4:9].read()

# Can use an infinite iterator as source of indices. (Reads ahead in parallel.)
def my_generator(size: int) -> Iterable[int]:
  rng = np.random.default_rng(42)
  while True:
    yield rng.integers(size).item()

data_iter: Iterable[bytes] = data.read_indices_iter(my_generator(len(data)))
for i in range(10):
  random_item: bytes = next(data_iter)
```

#### Python Reader - `Index` and `MultiIndex`

You can use `Index` to find the first index of a record and `MultiIndex` to find
all instances of an item.

```python
keys = sackli.Reader('/path/to/keys.bag')
# Get the index of the first occurrence of key.
index = sackli.Index(keys)
key_index: int = index[b'example_key']

# Get all occurrences of key.
multi_index = sackli.MultiIndex(keys)
all_indices: list[int] = multi_index[b'example_key']
```

### Python Writer

For writing a single Säckli file.

Example:

```python
import sackli

# Compression is selected based on the file extension:
# `.bagz` will use Zstandard compression with default settings.
# `.bag` will use no compression.
with sackli.Writer('/path/to/data.bagz') as writer:
  for d in generate_records():
    writer.write(d)

# Adjust compression level explicitly.
# Note this will no longer use the extension to detemine whether to compress.
with sackli.Writer(
    '/path/to/data.bagz',
    sackli.Writer.Options(
        compression=sackli.CompressionZstd(level=3)
    ),
) as writer:
  for d in generate_records():
    writer.write(d)
```

## Options

### Reader Options

`sackli.Reader.Options` has these optional arguments.

*   `compression`: Can be one of:
    *   `sackli.CompressionAutoDetect()`: Default - Uses extension whether to
        compress. (`.bagz` - Compressed (ZStandard), `.bag` - Uncompressed)
    *   `sackli.CompressionNone()`: Records are not decompressed.
    *   `sackli.CompressionZstd()`: Records are decompressed using Zstandard.
*   `limits_placement`: Can be one of:
    *   `sackli.LimitsPlacement.TAIL`: Default- Reads limits from a tail of file.
    *   `sackli.LimitsPlacement.SEPARATE`: Reads limits from a separate file.
*   `limits_storage`: Can be one of:
    *   `sackli.LimitsStorage.ON_DISK`: Default - Reads limits from disk for each
        read.
    *   `sackli.LimitsStorage.IN_MEMORY`: Reads all limits from disk in one go.
*   `access_pattern`: Can be one of:
    *   `sackli.AccessPattern.SYSTEM`: Default - no specific hint to the OS.
    *   `sackli.AccessPattern.RANDOM`: Hints that you read entries in random order.
    *   `sackli.AccessPattern.SEQUENTIAL`: Hints that you read entries roughly sequentially.
*   `cache_policy`: Can be one of:
    *   `sackli.CachePolicy.SYSTEM`: Default - no specific hint to the OS.
    *   `sackli.CachePolicy.DROP_AFTER_READ`: Reads data in such a way that the OS
        is unlikely to hold any of it in cache. For POSIX filesystems, this means
        using OS-specific no-cache hints: Linux uses `pread` with
        `posix_fadvise`, while macOS uses `MAP_NOCACHE` plus `madvise` for
        mmap-backed reads and `F_NOCACHE` for streaming reads.
        This is more efficient when you read more data than your RAM before doing
        any repeats (ie when an epoch is larger than RAM).
    *   `sackli.CachePolicy.DIRECT_IO`: Uses `O_DIRECT` on Linux and `F_NOCACHE`
        on macOS to read records. This is the most aggressive os-cache avoidance
        option and can be best for random reads on huge data with rare re-reads.
        Linux uses `STATX_DIOALIGN` if supported, otherwise probes from a conservative
        page-aligned starting point derived from file/filesystem metadata.
        For the unaligned tail, it does a one-time standard read at init.
*   `max_parallelism`: Default number of threads when reading many records.
*   `sharding_layout`: Can be one of:
    *   `sackli.ShardingLayout.CONCATENATED`: Default - See [Sharding](#sharding)
    *   `sackli.ShardingLayout.INTERLEAVED`: See [Sharding](#sharding)

`access_pattern` and `cache_policy` are currently interpreted only for local
POSIX files and influence OS-level behaviour on page cache and cache lines.

For tail-formatted files, non-default POSIX record-cache policies open a second
POSIX read handle to the same file so limits metadata reads keep the default
cache policy.

### Writer Options

`sackli.Writer.Options` has these optional arguments.

*   `compression`: Can be one of:
    *   `sackli.CompressionAutoDetect()`: Default - Uses extension whether to
        compress. (`.bagz` - Compressed (Zstandard), `.bag` - Uncompressed)
    *   `sackli.CompressionNone()`: Records are not compressed.
    *   `sackli.CompressionZstd(level = 3)`: Records are compressed using
        Zstandard the level of the compression can be specified.
*   `limits_placement`: Can be one of:
    *   `sackli.LimitsPlacement.TAIL`: Default - Writes limits to a tail of file.
    *   `sackli.LimitsPlacement.SEPARATE`: Writes limits to a separate file.

## Sharding

An ordered collection of Säckli-formatted files ("shards") may be opened together
and indexed via a single `global-index`. The `global-index` is mapped to a
`shard` and an index within that shard (`shard-index`) in one of two ways:

1.  **Concatenated** (default). Indexing is equivalent to the records in each
    Säckli-formatted shard being concatenated into a single sequence of records.

    Example:

    When opening four Säckli-formatted files with sizes `[8, 4, 0, 5]`, the
    `global-index` with range `[0, 17)` (shown as the table entries) maps to
    `shard` and `shard-index` like this:

    ```
                   | shard-index
    shard          |  0  1  2  3  4  5  6  7
    -------------- | -----------------------
    00000-of-00004 |  0  1  2  3  4  5  6  7
    00001-of-00004 |  8  9 10 11
    00002-of-00004 |
    00003-of-00004 | 12 13 14 15 16
    ```

    Mappings

    `global-index` | `shard`          | `shard-index`
    -------------: | :--------------: | -------------
    `0`            | `00000-of-00004` | `0`
    `1`            | `00000-of-00004` | `1`
    `2`            | `00000-of-00004` | `2`
    ...            | ...              | ...
    `8`            | `00001-of-00004` | `0`
    `9`            | `00001-of-00004` | `1`
    ...            | ...              | ...
    `15`           | `00003-of-00004` | `3`
    `16`           | `00003-of-00004` | `4`

2.  **Interleaved** where the global-index is interleaved in a round-robin
    manner across all the shards.

    Example:

    When opening three Säckli-formatted files with sizes `[6, 6, 5]`, the
    `global-index` with range `[0, 17)` (shown as the table entries) maps to
    `shard` and `shard-index` like this:

    ```
                   |  shard-index
    shard          |  0  1  2  3  4  5
    -------------- | -----------------
    00000-of-00003 |  0  3  6  9 12 15
    00001-of-00003 |  1  4  7 10 13 16
    00002-of-00003 |  2  5  8 11 14
    ```

    Mappings

    `global-index` | `shard`          | `shard-index`
    -------------: | :--------------: | -------------
    `0`            | `00000-of-00003` | `0`
    `1`            | `00001-of-00003` | `0`
    `2`            | `00002-of-00003` | `0`
    ...            | ...              | ...
    `6`            | `00000-of-00003` | `2`
    `7`            | `00001-of-00003` | `2`
    `8`            | `00002-of-00003` | `2`
    ...            | ...              | ...
    `15`           | `00000-of-00003` | `5`
    `16`           | `00001-of-00003` | `5`

## Apache Beam Support

Säckli also provides Apache Beam connectors for reading and writing Säckli files in
Beam pipelines.

Ensure you have Apache Beam installed.
```sh
uv pip install apache_beam
```

### Säckli Source

```python
import apache_beam as beam
from sackli.beam import sacklio
import tensorflow as tf

with beam.Pipeline() as pipeline:
  examples = (
      pipeline
      | 'ReadData' >> sacklio.ReadFromSackli('/path/to/your/data@*.bagz')
      | 'Decode' >> beam.Map(tf.train.Example.FromString)
  )
  # Continue your pipeline.
```

#### Säckli Sink

```python
from sackli.beam import sacklio
import tensorflow as tf

def create_tf_example(data):
  # Replace with your actual feature creation logic.
  feature = {
      'data': tf.train.Feature(bytes_list=tf.train.BytesList(value=[data])),
  }
  return tf.train.Example(features=tf.train.Features(feature=feature))

with beam.Pipeline() as pipeline:
  data = [b'record1', b'record2', b'record3']

  examples = (
      pipeline
      | 'CreateData' >> beam.Create(data)
      | 'Encode' >> beam.Map(lambda x: create_tf_example(x).SerializeToString())
      | 'WriteData' >> sacklio.WriteToSackli('/path/to/output/data@*.bagz')
  )
```

## Cloud Storage

Säckli supports POSIX file-systems, Google Cloud Storage (GCS), and Amazon S3.
These can be enabled or disabled at compile-time, but the PyPI-deployed wheels
have support for both built-in.

### GCS authentication

These examples assume you have the gcloud CLI installed.

```sh
gcloud config set project your-project-name
gcloud auth application-default login
```

### S3 authentication

Authentication uses the standard AWS credential chain (environment variables,
`~/.aws/credentials`, IAM roles, etc.).

```sh
aws configure
```

### Paths

Use the `gs:` and `s3:` file-system prefixes in paths.

```python
import pathlib
import sackli

# This may freeze if you have not configured the GCS project.
gcs_reader = sackli.Reader('gs://your-bucket-name/your-file.bagz')
s3_reader = sackli.Reader('s3://your-bucket-name/your-file.bagz')

# Path supports a leading slash to work well with pathlib.
gcs_bucket = pathlib.Path('/gs://your-bucket-name')
gcs_reader = sackli.Reader(gcs_bucket / 'your-file.bagz')

s3_bucket = pathlib.Path('/s3://your-bucket-name')
s3_reader = sackli.Reader(s3_bucket / 'your-file.bagz')
```

## Säckli/Bagz file format

For now, Säckli still preserves exactly the Bagz file format.
However, this is not guaranteed to remain the case.

The Bagz file format has two parts: the `records` section and the `limits`
section.

*   The `records` section consists of the concatenation of all (possibly
    compressed) records. (There are no additional bytes inside or between
    records, and records are not aligned in any way.)
*   The `limits` section is a dense array of the end-offsets of each record in
    order, encoded in little-endian 64-bit unsigned integers.

These can be stored as *tail-limits* in one file, where the `limits` section is
appended to the `records` section, or as *separate-limits*, where they are
stored in separate files.

### Tail-limits example

Given a Bagz-formatted file with the following 3 uncompressed records:

Records  |
-------- |
`abcdef` |
`123`    |
`catcat` |

The raw bytes of the Bagz-formatted file corresponding to the records above:

```
0x61 a 0x62 b 0x63 c 0x64 d 0x65 e 0x66 f
0x31 1 0x32 2 0x33 3
0x63 c 0x61 a 0x74 t 0x63 c 0x61 a 0x74 t
0x06   0x00   0x00   0x00   0x00   0x00   0x00   0x00  # 6 byte offset
0x09   0x00   0x00   0x00   0x00   0x00   0x00   0x00  # 9 byte offset
0x0f   0x00   0x00   0x00   0x00   0x00   0x00   0x00  # 15 byte offset
```

The last 8 bytes represent the end-offset of the last record. This is also the
start of the `limits` section. Therefore reading the last 8 bytes will directly
tell you the offset of the `records`/`limits` boundary.
