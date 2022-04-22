"""
    Created by prakash at 24/02/22
"""
__author__ = 'Prakash14'

import logging
import re
from json import JSONDecoder as BaseJSONDecoder, JSONDecodeError

from . import py_make_scanner

FLAGS = re.VERBOSE | re.MULTILINE | re.DOTALL

STRINGCHUNK = re.compile(r'(.*?)(["\\\x00-\x1f])', FLAGS)
SINGLE_STRINGCHUNK = re.compile(r"(.*?)(['\\\x00-\x1f])", FLAGS)
W_STRINGCHUNK = re.compile(r'(.*?)([:\\\x00-\x1f])', FLAGS)
NUMBER_RE = re.compile(r'(-?(?:0|[1-9]\d*))(\.\d+)?([eE][-+]?\d+)?', FLAGS)
BACKSLASH = {
    '"': '"', '\\': '\\', '/': '/',
    'b': '\b', 'f': '\f', 'n': '\n', 'r': '\r', 't': '\t',
}
WHITESPACE = re.compile(r'[ \t\n\r]*', FLAGS)
WHITESPACE_STR = ' \t\n\r'


def _decode_uXXXX(s, pos):
    esc = s[pos + 1:pos + 5]
    if len(esc) == 4 and esc[1] not in 'xX':
        try:
            return int(esc, 16)
        except ValueError:
            pass
    msg = "Invalid \\uXXXX escape"
    raise JSONDecodeError(msg, s, pos)


def scanstring(s, end, strict=True, start_with='"', _m=STRINGCHUNK.match):
    """Scan the string s for a JSON string. End is the index of the
    character in s after the quote that started the JSON string.
    Unescapes all valid JSON string escape sequences and raises ValueError
    on attempt to decode an invalid string. If strict is False then literal
    control characters are allowed in the string.

    Returns a tuple of the decoded string and the index of the character in s
    after the end quote."""
    chunks = []
    _append = chunks.append
    begin = end - 1
    _b = BACKSLASH
    is_loop = True
    while is_loop:
        chunk = _m(s, end)
        if chunk is None:
            raise JSONDecodeError("Unterminated string starting at", s, begin)
        end = chunk.end()
        content, terminator = chunk.groups()
        # Content is contains zero or more unescaped string characters
        if content:
            _append(content)
        # Terminator is the end of string, a literal control character,
        # or a backslash denoting that an escape sequence follows
        if (start_with == '"' and terminator == '"') or (start_with == "'" and terminator == "'"):
            is_loop = False
            break
        elif terminator == ":":
            # _nextchar = s[end+1]
            # _end = end+1
            # _collen_ = end
            # is_lost_collen = False
            # while not is_lost_collen:
            #     while _nextchar in WHITESPACE_STR:
            #         _end += 1
            #         _nextchar = s[_end]
            #     if _nextchar in ("'", '"', '[', "{") or ((_nextchar == 'n' and s[_end:_end + 4] == 'null') or
            #                                              (_nextchar == 't' and s[_end:_end + 4] == 'true')) \
            #             and s[_end+4] in ("}", ",", "]"):#\
            #             # or (_nextchar == 'n' and s[_end:_end + 4] == 'null') or \
            #             # (_nextchar == 't' and s[_end:_end + 4] == 'true') or \
            #             # (_nextchar == 'f' and s[_end:_end + 5] == 'false') or NUMBER_RE.match(s, _end) or \
            #             # (_nextchar == 'N' and s[_end:_end + 3] == 'NaN') or \
            #             # (_nextchar == 'I' and s[_end:_end + 8] == 'Infinity') or \
            #             # (_nextchar == '-' and s[_end:_end + 9] == '-Infinity'):
            #         end = _collen_ - 1
            #         is_lost_collen = True
            #     # elif ((_nextchar == 'n' and s[_end:_end + 4] == 'null') or \
            #     #         (_nextchar == 't' and s[_end:_end + 4] == 'true')) and s[_end+5] in ("}", ",", "]"):
            #     else:
            #         chunk = W_STRINGCHUNK.match(s[_end:])
            #         content, terminator = chunk.groups()
            #         _end = _end + chunk.end()
            #         _collen_ = _end
            end -= 1
            break
        elif terminator != '\\':
            if strict:
                # msg = "Invalid control character %r at" % (terminator,)
                msg = "Invalid control character {0!r} at".format(terminator)
                raise JSONDecodeError(msg, s, end)
            else:
                _append(terminator)
                continue
        try:
            esc = s[end]
        except IndexError:
            raise JSONDecodeError("Unterminated string starting at",
                                  s, begin) from None
        # If not a unicode escape sequence, must be in the lookup table
        if esc != 'u':
            try:
                char = _b[esc]
            except KeyError:
                msg = "Invalid \\escape: {0!r}".format(esc)
                raise JSONDecodeError(msg, s, end)
            end += 1
        else:
            uni = _decode_uXXXX(s, end)
            end += 5
            if 0xd800 <= uni <= 0xdbff and s[end:end + 2] == '\\u':
                uni2 = _decode_uXXXX(s, end + 1)
                if 0xdc00 <= uni2 <= 0xdfff:
                    uni = 0x10000 + (((uni - 0xd800) << 10) | (uni2 - 0xdc00))
                    end += 6
            char = chr(uni)
        _append(char)
    return ''.join(chunks), end


def JSONObject(s_and_end, strict, scan_once, object_hook, object_pairs_hook,
               memo=None, _w=WHITESPACE.match, _ws=WHITESPACE_STR):
    s, end = s_and_end
    pairs = []
    pairs_append = pairs.append
    # Backwards compatibility
    if memo is None:
        memo = {}
    memo_get = memo.setdefault
    # Use a slice to prevent IndexError from being raised, the following
    # check will raise a more specific ValueError if the string is empty
    nextchar = s[end:end + 1]
    # Normally we expect nextchar == '"'
    chuck_re = None
    if nextchar != '"':
        if nextchar in _ws:
            end = _w(s, end).end()
            nextchar = s[end:end + 1]
        # Trivial empty object
        if nextchar == '}':
            if object_pairs_hook is not None:
                result = object_pairs_hook(pairs)
                return result, end + 1
            pairs = {}
            if object_hook is not None:
                pairs = object_hook(pairs)
            return pairs, end + 1
        elif nextchar in ('"', "'"):
            end += 1
        # elif nextchar != '"':
        #     raise JSONDecodeError(
        #         "Expecting property name enclosed in double quotes", s, end)
    chuck_re = STRINGCHUNK if nextchar == '"' else SINGLE_STRINGCHUNK if nextchar == "'" else W_STRINGCHUNK
    while True:
        key, end = scanstring(s, end, strict, _m=chuck_re.match)
        key = memo_get(key, key)
        # To skip some function call overhead we optimize the fast paths where
        # the JSON key separator is ": " or just ":".
        if s[end:end + 1] != ':':
            end = _w(s, end).end()
            if s[end:end + 1] != ':':
                raise JSONDecodeError("Expecting ':' delimiter", s, end)
        end += 1

        try:
            if s[end] in _ws:
                end += 1
                if s[end] in _ws:
                    end = _w(s, end + 1).end()
        except IndexError:
            pass

        try:
            value, end = scan_once(s, end)
        except StopIteration as err:
            raise JSONDecodeError("Expecting value", s, err.value) from None
        pairs_append((key, value))
        try:
            nextchar = s[end]
            if nextchar in _ws:
                end = _w(s, end + 1).end()
                nextchar = s[end]
        except IndexError:
            nextchar = ''
        end += 1

        if nextchar == '}':
            break
        elif nextchar != ',':
            raise JSONDecodeError("Expecting ',' delimiter", s, end - 1)
        end = _w(s, end).end()
        nextchar = s[end:end + 1]
        chuck_re = STRINGCHUNK if nextchar == '"' else SINGLE_STRINGCHUNK if nextchar == "'" else W_STRINGCHUNK
        if nextchar in ('"', "'"):
            end += 1
        #     raise JSONDecodeError(
        #         "Expecting property name enclosed in double quotes", s, end - 1)
    if object_pairs_hook is not None:
        result = object_pairs_hook(pairs)
        return result, end
    pairs = dict(pairs)
    if object_hook is not None:
        pairs = object_hook(pairs)
    return pairs, end


def JSONArray(s_and_end, scan_once, _w=WHITESPACE.match, _ws=WHITESPACE_STR):
    s, end = s_and_end
    values = []
    nextchar = s[end:end + 1]
    if nextchar in _ws:
        end = _w(s, end + 1).end()
        nextchar = s[end:end + 1]
    # Look-ahead for trivial empty array
    if nextchar == ']':
        return values, end + 1
    _append = values.append
    while True:
        try:
            value, end = scan_once(s, end)
        except StopIteration as err:
            raise JSONDecodeError("Expecting value", s, err.value) from None
        _append(value)
        nextchar = s[end:end + 1]
        if nextchar in _ws:
            end = _w(s, end + 1).end()
            nextchar = s[end:end + 1]
        end += 1
        if nextchar == ']':
            break
        elif nextchar != ',':
            logging.info("stsfdsfsdffsf ", s[end:])
            logging.info("\n sffdsf \n ", s[:end])
            raise JSONDecodeError("Expecting ',' delimiter", s, end - 1)
        try:
            if s[end] in _ws:
                end += 1
                if s[end] in _ws:
                    end = _w(s, end + 1).end()
        except IndexError:
            pass

    return values, end


class JSONDecoder(BaseJSONDecoder):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.parse_object = JSONObject
        self.parse_array = JSONArray
        self.scan_once = py_make_scanner(self)
