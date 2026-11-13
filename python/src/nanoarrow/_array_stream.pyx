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

# cython: language_level = 3


from libc.errno cimport EINVAL, EIO
from libc.stdint cimport uintptr_t, int64_t
from libc.stdio cimport snprintf
from cpython.pycapsule cimport PyCapsule_GetPointer
from cpython.ref cimport Py_INCREF, Py_DECREF

from nanoarrow_c cimport (
    ArrowArray,
    ArrowArrayStream,
    ArrowArrayStreamGetNext,
    ArrowArrayStreamGetSchema,
    ArrowArrayStreamMove,
    ArrowBasicArrayStreamInit,
    ArrowBasicArrayStreamSetArray,
    ArrowBasicArrayStreamValidate,
    ArrowBufferAppendInt64,
    ArrowError,
    ArrowResolveChunk64,
    ArrowSchema,
    ArrowSchemaDeepCopy,
    ArrowType,
    NANOARROW_OK,
)

from nanoarrow cimport _types
from nanoarrow._array cimport CArray
from nanoarrow._buffer cimport CBuffer
from nanoarrow._schema cimport CSchema, assert_type_equal
from nanoarrow._utils cimport (
    alloc_c_array_stream,
    c_array_shallow_copy,
    Error
)

from typing import Iterable, List, Tuple

from nanoarrow import _repr_utils


cdef class _IterableArrayStreamPrivate:
    """State for an ArrowArrayStream that pulls arrays lazily from a Python iterator

    An instance is the ``private_data`` of the stream, which holds one reference to it
    until the stream is released. After the iterator is exhausted or raises, the stream
    drops it and keeps returning the same result.
    """
    cdef object _iterator
    cdef CSchema _schema
    cdef bint _validate
    cdef bint _finished
    cdef bint _evaluating
    cdef int _error_code
    cdef ArrowError _error

    def __cinit__(self, object iterator, CSchema schema, bint validate):
        self._iterator = iterator
        self._schema = schema
        self._validate = validate
        self._finished = False
        self._evaluating = False
        self._error_code = NANOARROW_OK
        self._error.message[0] = 0

    cdef int _set_error(self, int code, object message) noexcept:
        cdef bytes message_bytes = str(message).encode("utf-8", "replace")
        snprintf(
            self._error.message,
            sizeof(self._error.message),
            "%s",
            <const char*>message_bytes,
        )
        self._error_code = code
        self._iterator = None
        return code

    cdef CArray _next_c_array(self):
        from nanoarrow.c_array import c_array

        item = next(self._iterator)

        # Arrays (and array-likes) are used as-is because CArray doesn't support
        # requested_schema; anything else (e.g., a list of values) is built with
        # the schema
        if hasattr(item, "__arrow_c_array__"):
            array = c_array(item)
        else:
            array = c_array(item, self._schema)

        if self._validate:
            assert_type_equal(array.schema, self._schema, False)
            array.view()

        return array

    cdef int get_next(self, ArrowArray* out) noexcept:
        cdef CArray array

        if self._finished:
            return NANOARROW_OK
        if self._error_code != NANOARROW_OK:
            return self._error_code
        if self._evaluating:
            return self._set_error(
                EINVAL,
                "Re-entrant call to get_next() on an array stream from an iterable",
            )

        self._evaluating = True
        try:
            array = self._next_c_array()
            c_array_shallow_copy(array._base, array._ptr, out)
        except StopIteration:
            self._finished = True
            self._iterator = None
        except BaseException as e:
            # Errors (and interrupts) can't cross the C callback: report them as a
            # terminal stream error that keeps the original type name
            return self._set_error(EIO, f"{type(e).__name__}: {e}")
        finally:
            self._evaluating = False

        return NANOARROW_OK


cdef int _iterable_array_stream_get_schema(
    ArrowArrayStream* stream, ArrowSchema* out
) noexcept nogil:
    with gil:
        return ArrowSchemaDeepCopy(
            (<_IterableArrayStreamPrivate>stream.private_data)._schema._ptr, out
        )


cdef int _iterable_array_stream_get_next(
    ArrowArrayStream* stream, ArrowArray* out
) noexcept nogil:
    out.release = NULL
    with gil:
        return (<_IterableArrayStreamPrivate>stream.private_data).get_next(out)


cdef const char* _iterable_array_stream_get_last_error(
    ArrowArrayStream* stream
) noexcept nogil:
    with gil:
        # The message buffer lives as long as the stream holds its private data
        return (<_IterableArrayStreamPrivate>stream.private_data)._error.message


cdef void _iterable_array_stream_release(ArrowArrayStream* stream) noexcept nogil:
    with gil:
        Py_DECREF(<object>stream.private_data)

    stream.private_data = NULL
    stream.release = NULL


cdef class CArrayStream:
    """Low-level ArrowArrayStream wrapper

    This object is a literal wrapper around an ArrowArrayStream. It provides methods that
    that wrap the underlying C callbacks and handles the C Data interface lifecycle
    (i.e., initialized ArrowArrayStream structures are always released).

    See `nanoarrow.c_array_stream()` for construction and usage examples.
    """
    cdef object _base
    cdef ArrowArrayStream* _ptr
    cdef object _cached_schema

    def __cinit__(self, object base, uintptr_t addr):
        self._base = base
        self._ptr = <ArrowArrayStream*>addr
        self._cached_schema = None

    @staticmethod
    def allocate() -> CArrayStream:
        """Allocate a released ArrowArrayStream"""
        cdef ArrowArrayStream* c_array_stream_out
        base = alloc_c_array_stream(&c_array_stream_out)
        return CArrayStream(base, <uintptr_t>c_array_stream_out)

    @staticmethod
    def from_c_arrays(arrays: List[CArray], CSchema schema, move=False, validate=True) -> CArrayStream:
        """Create an ArrowArrayStream from an existing set of arrays

        Given a previously resolved list of arrays, create an ArrowArrayStream
        representation of the sequence of chunks.

        Parameters
        ----------
        arrays : List[CArray]
            A list of arrays to use as batches.
        schema : CSchema
            The schema that will be returned. Must be type equal with the schema
            of each array (this is checked if validate is ``True``)
        move : bool, optional
            If True, transfer ownership from each array instead of creating a
            shallow copy. This is only safe if the caller knows the origin of the
            arrays and knows that they will not be accessed after this stream has been
            created.
        validate : bool, optional
            If True, enforce type equality between the provided schema and the schema
            of each array.
        """
        cdef ArrowArrayStream* c_array_stream_out
        base = alloc_c_array_stream(&c_array_stream_out)

        # Don't create more copies than we have to (but make sure
        # one exists for validation if requested)
        cdef CSchema out_schema = schema
        if validate and not move:
            validate_schema = schema
            out_schema = schema.__deepcopy__()
        elif validate:
            validate_schema = schema.__deepcopy__()
            out_schema = schema
        elif not move:
            out_schema = schema.__deepcopy__()

        cdef int code = ArrowBasicArrayStreamInit(c_array_stream_out, out_schema._ptr, len(arrays))
        Error.raise_error_not_ok("ArrowBasicArrayStreamInit()", code)

        cdef ArrowArray tmp
        cdef CArray array
        for i in range(len(arrays)):
            array = arrays[i]

            if validate:
                assert_type_equal(array.schema, validate_schema, False)

            if not move:
                c_array_shallow_copy(array._base, array._ptr, &tmp)
                ArrowBasicArrayStreamSetArray(c_array_stream_out, i, &tmp)
            else:
                ArrowBasicArrayStreamSetArray(c_array_stream_out, i, array._ptr)

        cdef Error error = Error()
        if validate:
            code = ArrowBasicArrayStreamValidate(c_array_stream_out, &error.c_error)
            error.raise_message_not_ok("ArrowBasicArrayStreamValidate()", code)

        return CArrayStream(base, <uintptr_t>c_array_stream_out)

    @staticmethod
    def from_iterable(obj, CSchema schema, validate=True) -> CArrayStream:
        """Create an ArrowArrayStream that pulls arrays lazily from an iterable

        Each call to the stream's ``get_next()`` takes the next item from ``obj``
        and converts it with :func:`c_array`, so batches can be produced on demand
        instead of being resolved up front as with :meth:`from_c_arrays`. Neither
        creating the stream nor requesting its schema consumes the iterable.

        An exception raised while producing or converting an item (including
        ``KeyboardInterrupt``) can't propagate through the C stream interface.
        It ends the stream with an error whose message starts with the exception's
        type name, and every later ``get_next()`` call returns the same error.
        The stream drops its reference to the iterator once it is exhausted or
        raises, and the iterator must not use the stream while producing an item.

        Parameters
        ----------
        obj : iterable
            An iterable of arrays or array-likes. Items that implement the Arrow
            PyCapsule interface are used as-is; any other item is converted with
            ``c_array(item, schema)``.
        schema : CSchema
            The schema that will be returned. Must be type equal with the schema
            of each array (this is checked if validate is ``True``)
        validate : bool, optional
            If True, enforce type equality between the provided schema and the schema
            of each array, and validate each array's buffers.

        Examples
        --------
        >>> import nanoarrow as na
        >>> from nanoarrow.c_array_stream import CArrayStream
        >>> def batches():
        ...     yield [1, 2, 3]
        ...     yield [4, 5]
        >>> stream = CArrayStream.from_iterable(batches(), na.c_schema(na.int32()))
        >>> [array.length for array in stream]
        [3, 2]
        """
        iterator = iter(obj)
        private_data = _IterableArrayStreamPrivate(
            iterator, schema.__deepcopy__(), validate
        )

        cdef ArrowArrayStream* c_array_stream_out
        base = alloc_c_array_stream(&c_array_stream_out)

        Py_INCREF(private_data)
        c_array_stream_out.private_data = <void*>private_data
        c_array_stream_out.get_schema = &_iterable_array_stream_get_schema
        c_array_stream_out.get_next = &_iterable_array_stream_get_next
        c_array_stream_out.get_last_error = &_iterable_array_stream_get_last_error
        c_array_stream_out.release = &_iterable_array_stream_release

        return CArrayStream(base, <uintptr_t>c_array_stream_out)

    def release(self):
        """Explicitly call the release callback of this stream"""
        if self.is_valid():
            self._ptr.release(self._ptr)

    @staticmethod
    def _import_from_c_capsule(stream_capsule) -> CArrayStream:
        """Import from a ArrowArrayStream PyCapsule.

        Parameters
        ----------
        stream_capsule : PyCapsule
            A valid PyCapsule with name 'arrow_array_stream' containing an
            ArrowArrayStream pointer.
        """
        return CArrayStream(
            stream_capsule,
            <uintptr_t>PyCapsule_GetPointer(stream_capsule, 'arrow_array_stream')
        )

    def __arrow_c_stream__(self, requested_schema=None):
        """
        Export the stream as an Arrow C stream PyCapsule.

        Parameters
        ----------
        requested_schema : PyCapsule | None
            A PyCapsule containing a C ArrowSchema representation of a requested
            schema. Not supported.

        Returns
        -------
        PyCapsule
        """
        self._assert_valid()

        if requested_schema is not None:
            raise NotImplementedError("requested_schema")

        cdef:
            ArrowArrayStream* c_array_stream_out

        array_stream_capsule = alloc_c_array_stream(&c_array_stream_out)
        ArrowArrayStreamMove(self._ptr, c_array_stream_out)
        return array_stream_capsule

    def _addr(self) -> int:
        """test to see if this causes a ci fail"""
        return <uintptr_t>self._ptr

    def is_valid(self) -> bool:
        """Check for a non-null and non-released underlying ArrowArrayStream"""
        return self._ptr != NULL and self._ptr.release != NULL

    def _assert_valid(self):
        if self._ptr == NULL:
            raise RuntimeError("array stream pointer is NULL")
        if self._ptr.release == NULL:
            raise RuntimeError("array stream is released")

    def _get_schema(self, CSchema schema):
        self._assert_valid()
        cdef Error error = Error()
        cdef int code = ArrowArrayStreamGetSchema(self._ptr, schema._ptr, &error.c_error)
        error.raise_message_not_ok("ArrowArrayStream::get_schema()", code)

    def _get_cached_schema(self):
        if self._cached_schema is None:
            self._cached_schema = CSchema.allocate()
            self._get_schema(self._cached_schema)

        return self._cached_schema

    def get_schema(self) -> CSchema:
        """Get the schema associated with this stream

        Calling this method will always issue a call to the underlying stream's
        get_schema callback.
        """
        out = CSchema.allocate()
        self._get_schema(out)
        return out

    def get_next(self) -> CArray:
        """Get the next Array from this stream

        Raises StopIteration when there are no more arrays in this stream.
        """
        self._assert_valid()

        # We return a reference to the same Python object for each
        # Array that is returned. This is independent of get_schema(),
        # which is guaranteed to call the C object's callback and
        # faithfully pass on the returned value.

        cdef Error error = Error()
        cdef CArray array = CArray.allocate(self._get_cached_schema())
        cdef int code = ArrowArrayStreamGetNext(self._ptr, array._ptr, &error.c_error)
        error.raise_message_not_ok("ArrowArrayStream::get_next()", code)

        if not array.is_valid():
            raise StopIteration()
        else:
            return array

    def __iter__(self):
        return self

    def __next__(self):
        return self.get_next()

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.release()

    def __repr__(self):
        return _repr_utils.array_stream_repr(self)


cdef class CMaterializedArrayStream:
    """Optimized representation of a fully consumed ArrowArrayStream

    This class provides a data structure similar to pyarrow's ChunkedArray
    where each consumed array is a referenced-counted shared array. This
    class wraps the utilities provided by the nanoarrow C library to iterate
    over and facilitate log(n) random access to items in this container.
    """
    cdef CSchema _schema
    cdef CBuffer _array_ends
    cdef list _arrays
    cdef int64_t _total_length

    def __cinit__(self):
        self._arrays = []
        self._total_length = 0
        self._schema = CSchema.allocate()
        self._array_ends = CBuffer.empty()
        cdef int code = ArrowBufferAppendInt64(self._array_ends._ptr, 0)
        Error.raise_error_not_ok("ArrowBufferAppendInt64()", code)

    cdef _finalize(self):
        self._array_ends._set_data_type(<ArrowType>_types.INT64)

    @property
    def schema(self) -> CSchema:
        return self._schema

    def __getitem__(self, k) -> Tuple[CArray, int]:
        cdef int64_t kint
        cdef int array_i
        cdef const int64_t* sorted_offsets = <int64_t*>self._array_ends._ptr.data

        if isinstance(k, slice):
            raise NotImplementedError("index with slice")

        kint = k
        if kint < 0:
            kint += self._total_length
        if kint < 0 or kint >= self._total_length:
            raise IndexError(f"Index {kint} is out of range")

        array_i = ArrowResolveChunk64(kint, sorted_offsets, 0, len(self._arrays))
        kint -= sorted_offsets[array_i]
        return self._arrays[array_i], kint

    def __len__(self) -> int:
        return self._array_ends[len(self._arrays)]

    def __iter__(self) -> Iterable[Tuple[CArray, int]]:
        for c_array in self._arrays:
            for item_i in range(len(c_array)):
                yield c_array, item_i

    def array(self, int64_t i) -> CArray:
        return self._arrays[i]

    @property
    def n_arrays(self) -> int:
        return len(self._arrays)

    @property
    def arrays(self) -> Iterable[CArray]:
        return iter(self._arrays)

    def __arrow_c_stream__(self, requested_schema=None):
        # When an array stream from iterable is supported, that could be used here
        # to avoid unnessary shallow copies.
        stream = CArrayStream.from_c_arrays(
            self._arrays,
            self._schema,
            move=False,
            validate=False
        )

        return stream.__arrow_c_stream__(requested_schema=requested_schema)

    def child(self, int64_t i) -> CMaterializedArrayStream:
        cdef CMaterializedArrayStream out = CMaterializedArrayStream()
        cdef int code

        out._schema = self._schema.child(i)
        out._arrays = [chunk.child(i) for chunk in self._arrays]
        for child_chunk in out._arrays:
            out._total_length += len(child_chunk)
            code = ArrowBufferAppendInt64(out._array_ends._ptr, out._total_length)
            Error.raise_error_not_ok("ArrowBufferAppendInt64()", code)

        out._finalize()
        return out

    @staticmethod
    def from_c_arrays(arrays: Iterable[CArray], CSchema schema, bint validate=True):
        """"Create a materialized array stream from an existing iterable of arrays

        This is slightly more efficient than creating a stream and then consuming it
        because the implementation can avoid a shallow copy of each array.
        """
        cdef CMaterializedArrayStream out = CMaterializedArrayStream()

        for array in arrays:
            if not isinstance(array, CArray):
                raise TypeError(f"Expected CArray but got {type(array).__name__}")

            if len(array) == 0:
                continue

            if validate:
                assert_type_equal(array.schema, schema, False)

            out._total_length += len(array)
            code = ArrowBufferAppendInt64(out._array_ends._ptr, out._total_length)
            Error.raise_error_not_ok("ArrowBufferAppendInt64()", code)
            out._arrays.append(array)

        out._schema = schema
        out._finalize()
        return out

    @staticmethod
    def from_c_array(CArray array) -> CMaterializedArrayStream:
        """"Create a materialized array stream from a single array
        """
        return CMaterializedArrayStream.from_c_arrays(
            [array],
            array.schema,
            validate=False
        )

    @staticmethod
    def from_c_array_stream(CArrayStream stream) -> CMaterializedArrayStream:
        """"Create a materialized array stream from an unmaterialized ArrowArrayStream
        """
        with stream:
            return CMaterializedArrayStream.from_c_arrays(
                stream,
                stream._get_cached_schema(),
                validate=False
            )
