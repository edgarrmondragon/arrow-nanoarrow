# Upstream port

It's on branch python-lazy-array-stream in ~/Code/contrib/apache/arrow-nanoarrow, and adds CArrayStream.from_iterable(obj, schema, validate=True):

- It's a lazy stream: creating it or reading its schema doesn't touch the iterator, and each get_next() pulls one item.

- Items with `__arrow_c_array__` are used as they are; anything else goes through `c_array(item, schema)`. With `validate=True`, it checks the type and runs nanoarrow's default buffer validation, the same as `from_c_arrays`.

- It follows #975's rules. An exception (including `KeyboardInterrupt`) ends the stream with an EIO error whose message starts with the exception type, e.g. `ValueError: source failed`. Errors are terminal, a re-entrant call is refused, and the iterator is dropped once it finishes or fails. Releasing the stream closes the generator.

- It includes 10 new tests and a type-stub entry. The full suite passes (294 passed, 4 skipped), and flake8, isort and black are clean. Pre-commit's pinned black crashes on Python 3.15, so I ran it on 3.13.

- A real consumer works: DuckDB ingested 1,000 lazily produced batches through ADBC, and pyarrow's `RecordBatchReader.from_stream` reads it, with source errors arriving as OSError.

## Before opening a PR

- **Foreign threads**. All the callbacks ran on the main thread. They use with gil, so a consumer calling from a thread Python didn't create should work, but nothing tests that yet.

- **Interpreter shutdown**. As with the existing IPC callbacks, releasing a stream after the interpreter has shut down isn't handled.

- **Scope**. I didn't add a high-level na.ArrayStream.from_iterable wrapper. It's worth asking the maintainers on #386 whether they want one, and whether from_iterable is the right name for the Python version of R's function_array_stream.

- **Commit message**. The changelog is generated from conventional commits, so it should be something like feat(python): Add CArrayStream.from_iterable() for lazy array streams, with a line referencing #386.
