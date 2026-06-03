from typing import Optional

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

def find_char(text:str, char:str) -> int:
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