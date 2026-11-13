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

import _cython_3_0_11
import types
from _typeshed import Incomplete
from typing import ClassVar

__reduce_cython__: _cython_3_0_11.cython_function_or_method
__setstate_cython__: _cython_3_0_11.cython_function_or_method
__test__: dict

class CArrayStream:
    @classmethod
    def __init__(cls, *args, **kwargs) -> None:
        """Create and return a new object.  See help(type) for accurate signature."""
    @staticmethod
    def allocate(*args, **kwargs):
        """Allocate a released ArrowArrayStream"""
    @staticmethod
    def from_c_arrays(*args, **kwargs):
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
    @staticmethod
    def from_iterable(*args, **kwargs):
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
    def get_next(self, *args, **kwargs):
        """Get the next Array from this stream

        Raises StopIteration when there are no more arrays in this stream.
        """
    def get_schema(self, *args, **kwargs):
        """Get the schema associated with this stream

        Calling this method will always issue a call to the underlying stream's
        get_schema callback.
        """
    def is_valid(self, *args, **kwargs):
        """Check for a non-null and non-released underlying ArrowArrayStream"""
    def release(self, *args, **kwargs):
        """Explicitly call the release callback of this stream"""
    def __arrow_c_stream__(self, *args, **kwargs):
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
    def __enter__(self): ...
    def __exit__(
        self,
        type: type[BaseException] | None,
        value: BaseException | None,
        traceback: types.TracebackType | None,
    ): ...
    def __iter__(self):
        """Implement iter(self)."""
    def __next__(self): ...
    def __reduce__(self): ...

class CMaterializedArrayStream:
    __pyx_vtable__: ClassVar[PyCapsule] = ...
    arrays: Incomplete
    n_arrays: Incomplete
    schema: Incomplete
    @classmethod
    def __init__(cls, *args, **kwargs) -> None:
        """Create and return a new object.  See help(type) for accurate signature."""
    def array(self, *args, **kwargs): ...
    def child(self, *args, **kwargs): ...
    @staticmethod
    def from_c_array(*args, **kwargs):
        """ "Create a materialized array stream from a single array"""
    @staticmethod
    def from_c_array_stream(*args, **kwargs):
        """ "Create a materialized array stream from an unmaterialized ArrowArrayStream"""
    @staticmethod
    def from_c_arrays(*args, **kwargs):
        """ "Create a materialized array stream from an existing iterable of arrays

        This is slightly more efficient than creating a stream and then consuming it
        because the implementation can avoid a shallow copy of each array.
        """
    def __arrow_c_stream__(self, *args, **kwargs): ...
    def __getitem__(self, index):
        """Return self[key]."""
    def __iter__(self):
        """Implement iter(self)."""
    def __len__(self) -> int:
        """Return len(self)."""
    def __reduce__(self): ...
