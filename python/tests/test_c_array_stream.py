# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import pytest
from nanoarrow._utils import NanoarrowException
from nanoarrow.c_array_stream import CArrayStream

import nanoarrow as na


def test_c_array_stream_from_c_array_stream():
    # Wrapping an existing stream is a no-op
    array_stream = CArrayStream.from_c_arrays([], na.c_schema(na.int32()))
    stream_from_stream = na.c_array_stream(array_stream)
    assert stream_from_stream is array_stream

    # With requested_schema should go through capsule
    array_stream = CArrayStream.from_c_arrays([], na.c_schema(na.int32()))
    with pytest.raises(NotImplementedError):
        na.c_array_stream(array_stream, na.int64())


def test_c_array_stream_from_capsule_protocol():
    # Use wrapper object to ensure this is the path taken in the constructor
    class CArrayStreamWrapper:
        def __init__(self, obj):
            self.obj = obj

        def __arrow_c_stream__(self, *args, **kwargs):
            return self.obj.__arrow_c_stream__(*args, **kwargs)

    array_stream = CArrayStream.from_c_arrays([], na.c_schema(na.int32()))
    array_stream_wrapper = CArrayStreamWrapper(array_stream)
    from_protocol = na.c_array_stream(array_stream_wrapper)
    assert array_stream.is_valid() is False
    assert from_protocol.get_schema().format == "i"


def test_c_array_stream_from_old_pyarrow():
    # Simulate a pyarrow RecordBatchReader with no __arrow_c_stream__
    class MockLegacyPyarrowRecordBatchReader:
        def __init__(self, obj):
            self.obj = obj

        def _export_to_c(self, *args):
            return self.obj._export_to_c(*args)

    MockLegacyPyarrowRecordBatchReader.__module__ = "pyarrow.lib"

    pa = pytest.importorskip("pyarrow")
    reader = pa.RecordBatchReader.from_batches(pa.schema([]), [])
    mock_reader = MockLegacyPyarrowRecordBatchReader(reader)

    array_stream = na.c_array_stream(mock_reader)
    assert array_stream.get_schema().format == "+s"


def test_c_array_stream_from_bare_capsule():
    array_stream = CArrayStream.from_c_arrays([], na.c_schema(na.int32()))

    # Check from bare capsule without supplying a schema
    capsule = array_stream.__arrow_c_stream__()
    from_capsule = na.c_array_stream(capsule)
    assert from_capsule.get_schema().format == "i"

    array_stream = CArrayStream.from_c_arrays([], na.c_schema(na.int32()))
    capsule = array_stream.__arrow_c_stream__()

    with pytest.raises(TypeError, match="Can't import c_array_stream"):
        na.c_array_stream(capsule, na.int32())


def test_c_array_stream_from_c_array_fallback():
    # Check that arrays are valid input
    c_array = na.c_array([1, 2, 3], na.int32())
    array_stream = na.c_array_stream(c_array)
    assert array_stream.get_schema().format == "i"
    arrays = list(array_stream)
    assert len(arrays) == 1
    assert arrays[0].buffers == c_array.buffers

    # Check fallback with schema
    array_stream = na.c_array_stream([1, 2, 3], na.int32())
    assert array_stream.get_schema().format == "i"
    arrays = list(array_stream)
    assert len(arrays) == 1


def test_c_array_stream_error():
    msg = "Can't resolve ArrayBuilder"
    with pytest.raises(TypeError, match=msg):
        na.c_array_stream(None)


def test_array_stream_from_arrays_schema():
    schema_in = na.c_schema(na.int32())

    stream = CArrayStream.from_c_arrays([], schema_in)
    assert schema_in.is_valid()
    assert list(stream) == []
    assert stream.get_schema().format == "i"

    # Check move of schema
    CArrayStream.from_c_arrays([], schema_in, move=True)
    assert schema_in.is_valid() is False
    assert stream.get_schema().format == "i"


def test_array_stream_from_arrays():
    schema_in = na.c_schema(na.int32())
    array_in = na.c_array([1, 2, 3], na.int32())
    array_in_buffers = array_in.buffers

    stream = CArrayStream.from_c_arrays([array_in], schema_in)
    assert array_in.is_valid()
    arrays = list(stream)
    assert len(arrays) == 1
    assert arrays[0].buffers == array_in_buffers

    # Check move of array
    stream = CArrayStream.from_c_arrays([array_in], schema_in, move=True)
    assert array_in.is_valid() is False
    arrays = list(stream)
    assert len(arrays) == 1
    assert arrays[0].buffers == array_in_buffers


def test_array_stream_from_arrays_validate():
    schema_in = na.c_schema(na.null())
    array_in = na.c_array([1, 2, 3], na.int32())

    # Check that we can skip validation and proceed without error
    stream = CArrayStream.from_c_arrays([array_in], schema_in, validate=False)
    arrays = list(stream)
    assert len(arrays) == 1
    assert arrays[0].n_buffers == 2

    # ...but that validation does happen by default
    with pytest.raises(ValueError, match="Expected schema"):
        CArrayStream.from_c_arrays([array_in], schema_in)


def test_array_stream_from_iterable_is_lazy():
    produced = []

    def batches():
        for i in range(3):
            produced.append(i)
            yield na.c_array([i], na.int32())

    stream = CArrayStream.from_iterable(batches(), na.c_schema(na.int32()))
    assert stream.get_schema().format == "i"
    assert produced == []

    assert stream.get_next().length == 1
    assert produced == [0]

    assert [array.length for array in stream] == [1, 1]
    assert produced == [0, 1, 2]

    # The end of the stream is sticky
    with pytest.raises(StopIteration):
        stream.get_next()


def test_array_stream_from_iterable_converts_items():
    schema_in = na.c_schema(na.int32())

    stream = CArrayStream.from_iterable([[1, 2, 3], [4]], schema_in)
    assert [array.length for array in stream] == [3, 1]

    # Arrays are used without a copy
    array_in = na.c_array([1, 2, 3], na.int32())
    stream = CArrayStream.from_iterable([array_in], schema_in)
    assert stream.get_next().buffers == array_in.buffers


def test_array_stream_from_iterable_empty():
    stream = CArrayStream.from_iterable([], na.c_schema(na.int32()))
    assert list(stream) == []
    assert stream.get_schema().format == "i"


def test_array_stream_from_iterable_copies_schema():
    schema_in = na.c_schema(na.int32())
    stream = CArrayStream.from_iterable([], schema_in)
    assert schema_in.is_valid()
    assert stream.get_schema()._addr() != schema_in._addr()


def test_array_stream_from_iterable_error():
    def batches():
        yield [1, 2, 3]
        raise ValueError("source failed")

    stream = CArrayStream.from_iterable(batches(), na.c_schema(na.int32()))
    assert stream.get_next().length == 3

    with pytest.raises(NanoarrowException, match="ValueError: source failed"):
        stream.get_next()

    # The error is terminal
    with pytest.raises(NanoarrowException, match="ValueError: source failed"):
        stream.get_next()


def test_array_stream_from_iterable_validate():
    schema_in = na.c_schema(na.null())
    array_in = na.c_array([1, 2, 3], na.int32())

    stream = CArrayStream.from_iterable([array_in], schema_in, validate=False)
    assert stream.get_next().n_buffers == 2

    stream = CArrayStream.from_iterable([array_in], schema_in)
    with pytest.raises(NanoarrowException, match="ValueError: Expected schema"):
        stream.get_next()


def test_array_stream_from_iterable_not_iterable():
    with pytest.raises(TypeError):
        CArrayStream.from_iterable(None, na.c_schema(na.int32()))


def test_array_stream_from_iterable_reentrant():
    stream = None

    def batches():
        yield list(stream)

    stream = CArrayStream.from_iterable(batches(), na.c_schema(na.int32()))
    with pytest.raises(NanoarrowException, match="Re-entrant call"):
        stream.get_next()


def test_array_stream_from_iterable_releases_iterator():
    closed = []

    def batches():
        try:
            yield [1]
            yield [2]
        finally:
            closed.append(True)

    stream = CArrayStream.from_iterable(batches(), na.c_schema(na.int32()))
    stream.get_next()
    assert closed == []

    stream.release()
    assert closed == [True]


def test_array_stream_from_iterable_export():
    pa = pytest.importorskip("pyarrow")

    schema = pa.schema([pa.field("x", pa.int32())])

    def batches():
        yield pa.record_batch([pa.array([1, 2], pa.int32())], schema=schema)
        yield pa.record_batch([pa.array([3], pa.int32())], schema=schema)
        raise ValueError("source failed")

    stream = CArrayStream.from_iterable(batches(), na.c_schema(schema))
    reader = pa.RecordBatchReader.from_stream(stream)
    assert reader.read_next_batch().num_rows == 2
    assert reader.read_next_batch().num_rows == 1
    # EIO from the stream becomes an OSError in pyarrow
    with pytest.raises(OSError, match="ValueError: source failed"):
        reader.read_next_batch()
