"""
A copy of id3c/lib/id3c/json.py and the additional util functions from
id3c/lib/id3c/utils.py of the seattleflu/id3c repo as of commit 911e7d7 and
licensed under the MIT License. The License file included in the repo is
copied below verbatim.

MIT License

Copyright (c) 2018 Brotman Baty Institute

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""
import json
from datetime import datetime
from typing import Iterable
from uuid import UUID


def as_json(value):
    """
    Converts *value* to a JSON string using our custom :class:`JsonEncoder`.
    """
    return json.dumps(value, allow_nan = False, cls = JsonEncoder)


def load_json(value):
    """
    Converts *value* from a JSON string with better error messages.
    Raises an :exc:`augur.io.json.JSONDecodeError` which provides improved error
    messaging, compared to :exc:`json.JSONDecodeError`, when stringified.
    """
    try:
        return json.loads(value)
    except json.JSONDecodeError as e:
        raise JSONDecodeError(e) from e


def dump_ndjson(iterable: Iterable) -> None:
    """
    :func:`print` *iterable* as a set of newline-delimited JSON records.
    """
    for item in iterable:
        print(as_json(item))


def load_ndjson(file: Iterable[str], ignore_empty_lines = True) -> Iterable:
    """
    Load newline-delimited JSON records from *file*. Ignore empty lines
    in the file by default.
    """
    for line in file:
        # Skip empty lines when requested
        if ignore_empty_lines and not line.strip():
            continue

        yield load_json(line)


class JsonEncoder(json.JSONEncoder):
    """
    Encodes Python values into JSON for non-standard objects.
    """

    def __init__(self, *args, **kwargs):
        """
        Disallows the floating point values NaN, Infinity, and -Infinity.
        Python's :class:`json` allows them by default because they work with
        JSON-as-JavaScript, but they don't work with spec-compliant JSON
        parsers.
        """
        kwargs["allow_nan"] = False
        super().__init__(*args, **kwargs)

    def default(self, value):
        """
        Returns *value* as JSON or raises a TypeError.
        Serializes:
        * :class:`~datetime.datetime` using :meth:`~datetime.datetime.isoformat()`
        * :class:`~uuid.UUID` using ``str()``
        """
        if isinstance(value, datetime):
            return value.isoformat()

        elif isinstance(value, UUID):
            return str(value)

        else:
            # Let the base class raise the TypeError
            return super().default(value)


class JSONDecodeError(json.JSONDecodeError):
    """
    Subclass of :class:`json.JSONDecodeError` which contextualizes the
    stringified error message by including a snippet of the JSON source input.
    Typically you won't need to ever reference this class directly.  It will be
    raised by :func:`load_json` and be caught by except blocks which catch the
    standard :class:`json.JSONDecodeError`.
    >>> load_json('{foo: "bar"}')
    Traceback (most recent call last):
        ...
    augur.io.json.JSONDecodeError: Expecting property name enclosed in double quotes: line 1 column 2 (char 1): '{▸▸▸f◂◂◂oo: "bar"}'
    >>> load_json('not json')
    Traceback (most recent call last):
        ...
    augur.io.json.JSONDecodeError: Expecting value: line 1 column 1 (char 0): 'not json'
    >>> load_json("[0, 1, 2, 3, 4, 5")
    Traceback (most recent call last):
        ...
    augur.io.json.JSONDecodeError: Expecting ',' delimiter: line 1 column 18 (char 17): unexpected end of document: '…, 3, 4, 5'
    >>> load_json("[\\n")
    Traceback (most recent call last):
        ...
    augur.io.json.JSONDecodeError: Expecting value: line 2 column 1 (char 2): unexpected end of document: '[\\n'
    >>> load_json("\\n")
    Traceback (most recent call last):
        ...
    augur.io.json.JSONDecodeError: Expecting value: line 2 column 1 (char 1): unexpected end of document: '\\n'
    >>> load_json('')
    Traceback (most recent call last):
        ...
    augur.io.json.JSONDecodeError: Expecting value: line 1 column 1 (char 0): (empty source document)
    """
    CONTEXT_LENGTH = 10

    def __init__(self, exc: json.JSONDecodeError):
        super().__init__(exc.msg, exc.doc, exc.pos)

    def __str__(self):
        error = super().__str__()

        if self.doc:
            if self.pos == 0 and self.msg == "Expecting value":
                # Most likely not a JSON document at all, so show the whole thing.
                context = repr(self.doc)
            elif self.pos > 0 and self.pos == len(self.doc):
                context = "unexpected end of document: " + repr(shorten_left(self.doc, self.CONTEXT_LENGTH, "…"))
            else:
                context = repr(contextualize_char(self.doc, self.pos, self.CONTEXT_LENGTH))
        else:
            context = "(empty source document)"

        return f"{error}: {context}"


def shorten_left(text, length, placeholder):
    """
    A variant of :py:func:`shorten` which shortens from the left end of *text*
    instead of the right.

    >>> shorten_left("foobar", 6, "...")
    'foobar'
    >>> shorten_left("foobarbaz", 6, "...")
    '...baz'
    >>> shorten_left("foobar", 3, "...")
    Traceback (most recent call last):
        ...
    ValueError: maximum length (3) must be greater than length of placeholder (3)
    """
    if length <= len(placeholder):
        raise ValueError(f"maximum length ({length}) must be greater than length of placeholder ({len(placeholder)})")

    if len(text) > length:
        return placeholder + text[-(length - len(placeholder)):]
    else:
        return text


def contextualize_char(text, idx, context = 10):
    """
    Marks the *idx* char in *text* and snips out a surrounding amount of
    *context*.

    Avoids making a copy of *text* before snipping, in case *text* is very
    large.

    >>> contextualize_char('hello world', 0, context = 4)
    '▸▸▸h◂◂◂ello…'
    >>> contextualize_char('hello world', 5, context = 3)
    '…llo▸▸▸ ◂◂◂wor…'
    >>> contextualize_char('hello world', 5, context = 100)
    'hello▸▸▸ ◂◂◂world'
    >>> contextualize_char('hello world', 10)
    'hello worl▸▸▸d◂◂◂'
    >>> contextualize_char('hello world', 2, context = 0)
    '…▸▸▸l◂◂◂…'

    >>> contextualize_char('hello world', 11)
    Traceback (most recent call last):
        ...
    IndexError: string index out of range
    """
    if context < 0:
        raise ValueError("context must be positive")

    start = max(0, idx - context)
    end   = min(len(text), idx + context + 1)
    idx   = min(idx, context)

    start_placeholder = "…" if start > 0         else ""
    end_placeholder   = "…" if end   < len(text) else ""

    return start_placeholder + mark_char(text[start:end], idx) + end_placeholder


def mark_char(text, idx):
    """
    Prominently marks the *idx* char in *text*.

    >>> mark_char('hello world', 0)
    '▸▸▸h◂◂◂ello world'
    >>> mark_char('hello world', 2)
    'he▸▸▸l◂◂◂lo world'
    >>> mark_char('hello world', 10)
    'hello worl▸▸▸d◂◂◂'

    >>> mark_char('hello world', 11)
    Traceback (most recent call last):
        ...
    IndexError: string index out of range

    >>> mark_char('', 0)
    Traceback (most recent call last):
        ...
    IndexError: string index out of range
    """
    return text[0:idx] + '▸▸▸' + text[idx] + '◂◂◂' + text[idx+1:]
