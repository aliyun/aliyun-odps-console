from enum import Enum
from typing import Dict, List, NamedTuple


class State(Enum):
    DEFAULT = 0
    SINGLE_LINE_COMMENT = 1
    MULTI_LINE_COMMENT = 2
    IN_SET = 3
    IN_KEY_VALUE = 4
    STOP = 5


class ParseResult(NamedTuple):
    settings: Dict[str, str]
    remaining_query: str
    errors: List[str]


class SettingParser:
    @staticmethod
    def parse(query: str) -> ParseResult:
        parser = SettingParser()
        return parser.extract_set_statements(query)

    def extract_set_statements(self, s: str) -> ParseResult:
        settings: dict[str, str] = {}
        errors = []
        exclude_ranges = []
        current_state = State.DEFAULT
        lower_s = s.lower()
        i = 0
        current_start_index = -1
        s_length = len(s)

        while i < s_length:
            if current_state == State.DEFAULT:
                if i <= s_length - 2 and s[i : i + 2] == "--":
                    current_state = State.SINGLE_LINE_COMMENT
                    i += 2
                elif i <= s_length - 2 and s[i : i + 2] == "/*":
                    current_state = State.MULTI_LINE_COMMENT
                    i += 2
                elif i <= s_length - 3 and lower_s[i : i + 3] == "set":
                    if i + 3 < s_length and s[i + 3].isspace():
                        current_state = State.IN_SET
                        current_start_index = i
                        i += 4  # Skip 'set' and whitespace
                    else:
                        i += 1
                else:
                    if i < s_length and not s[i].isspace():
                        current_state = State.STOP
                    i += 1

            elif current_state == State.SINGLE_LINE_COMMENT:
                while i < s_length and s[i] != "\n":
                    i += 1
                if i < s_length:
                    i += 1
                current_state = State.DEFAULT

            elif current_state == State.MULTI_LINE_COMMENT:
                while i < s_length:
                    if i + 1 < s_length and s[i : i + 2] == "*/":
                        i += 2
                        current_state = State.DEFAULT
                        break
                    else:
                        i += 1

            elif current_state == State.IN_SET:
                while i < s_length and s[i].isspace():
                    i += 1
                if i < s_length:
                    current_state = State.IN_KEY_VALUE
                else:
                    errors.append("Invalid SET statement: missing key-value after 'set'")
                    current_start_index = -1
                    current_state = State.DEFAULT

            elif current_state == State.IN_KEY_VALUE:
                key_value_start = i
                found_semicolon = False
                while i < s_length:
                    if s[i] == ";" and (i == 0 or s[i - 1] != "\\"):
                        found_semicolon = True
                        i += 1
                        break
                    i += 1
                if found_semicolon:
                    kv = s[key_value_start : i - 1].strip()
                    if self._parse_key_value(kv, settings, errors):
                        exclude_ranges.append((current_start_index, i))
                else:
                    errors.append("Invalid SET statement: missing semicolon")
                current_state = State.DEFAULT
                current_start_index = -1

            elif current_state == State.STOP:
                i = s_length
        # Build remaining query
        remaining = []
        current_pos = 0
        for start, end in sorted(exclude_ranges, key=lambda x: x[0]):
            if current_pos < start:
                remaining.append(s[current_pos:start])
            current_pos = end
        if current_pos < s_length:
            remaining.append(s[current_pos:])

        return ParseResult(settings=settings, remaining_query="".join(remaining), errors=errors)

    def _parse_key_value(self, kv: str, settings: Dict[str, str], errors: List[str]) -> bool:
        eq_idx = kv.find("=")
        if eq_idx == -1:
            errors.append(f"Invalid key-value pair '{kv}': missing '='")
            return False
        key = kv[:eq_idx].strip()
        if not key:
            errors.append(f"Invalid key-value pair '{kv}': empty key")
            return False
        value = kv[eq_idx + 1 :].strip() if eq_idx < len(kv) - 1 else ""
        value = value.replace("\\;", ";")
        settings[key] = value
        return True
