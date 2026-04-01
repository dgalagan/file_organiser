from typing import Optional

def lower_text(text: str) -> str:
    return text.lower()

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

def count_char(text: str, char:str) -> int:
    return text.count(char)

def count_letters(text: str) -> int:
    is_letters = [char.isalpha() for char in text]
    return (sum(is_letters))

def find_char(text:str, char:str) -> int:
    return text.find(char)