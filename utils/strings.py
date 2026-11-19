from typing import Optional

ESC = "\033"
RESET = ESC + "[0m"

def color(text: str, code: int, bg: bool = False, bold: bool = False) -> str:
    sgr = 48 if bg else 38
    style = 1 if bold else 0
    return f"{ESC}[{style};{sgr};5;{code}m{text}{RESET}"

def repeat(text: str, count: int, separator: str = "") -> str:
    if not isinstance(count, int) or count < 1:
        raise ValueError(f"count must be an int > 0, got {type(count).__name__} with value {count}")
    if not isinstance(separator, str):
        raise ValueError(f"delimiter must be a string, got {type(separator).__name__}")
    return separator.join([text] * count)

def padding(text: str, width: int):
    return f"{text:<{width}}"



def lowercase_text(text: str) -> str:
    return text.lower() if isinstance(text, str) else ''

def uppercase_text(text: str) -> str:
    return text.upper() if isinstance(text, str) else ''

def strip_text(text: str, char_to_remove: Optional[str] = None) -> str:
    return text.strip(char_to_remove) 

def lstrip_text(text: str, char_to_remove: Optional[str] = None) -> str:
    return text.lstrip(char_to_remove) 

def rstrip_text(text: str, char_to_remove: Optional[str] = None) -> str:
    return text.rstrip(char_to_remove) 

def split_text(text: str, separator: Optional[str] = None) -> str:
    if separator is None:
        return text
    return text.split(separator)

def replace(text: str, old: str, new: str) -> str:
    return text.replace(old, new)

def count_char(text: str, char:str) -> int:
    return text.count(char)

def count_letters(text: str) -> int:
    is_letters = [char.isalpha() for char in text]
    return (sum(is_letters))

def find_char_pos(text: str, char: str) -> int:
    return text.find(char)

def get_chars_pattern(text: str) -> tuple[str, dict[int, str]]:
    chars_pattern = ""
    sep_args = {}
    counter = 0
    for char in text:
        if char.isdigit():
            chars_pattern += 'd' #digit
        elif char.isalpha():
            chars_pattern += 'l' #letter
        elif char.isspace():
            chars_pattern += 'w' #whitespace
        else:
            chars_pattern += 's' #other(separator)
            sep_args['s' + str(counter)] = char
            counter += 1
    return chars_pattern, sep_args