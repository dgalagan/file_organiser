from cli.components import Header, MenuLine, Prompt, Warning, Info
from cli.tokens import Separator, Icon
from enum import StrEnum, auto
import os
import pandas as pd
from utils.path import is_parent, is_dir, clean_dir
from utils.text import lowercase_text, strip_text

# Actions
class MenuActions(StrEnum):
    EXIT = auto()
    INTERUPT = auto()
    SKIP = auto()
    SUCCESS = auto()
    FAILED = auto()
    RESTART = auto()

# Destination directory for categorized files
def get_dest_dir() -> str:
    while True:
        print(Header.ELEMENTS["dest_dir"].generate())
        try:
            dest_dir_path = input("➡️  Provide empty directory for organized files: ")
        except KeyboardInterrupt:
            print()
            return ''
        
        if os.path.exists(dest_dir_path) and is_dir(dest_dir_path):
            return dest_dir_path
        else:
            print(Warning.ELEMENTS["invalid_input"].generate())
            continue

def prepare_dest_dir(path: str) -> bool:
    # Interact with user in case directory has files
    while True:
        try:
            permission = input(Prompt.ELEMENTS["clean"].generate(path=path))
        except KeyboardInterrupt:
            print()
            return False
        
        if permission == "y":
            try:
                clean_dir(path)
                return True
            except OSError as e:
                print(f"Failed to clean directory '{path}': {e}")
                continue
        elif permission == "n":
            return False
        else:
            print(Warning.ELEMENTS["invalid_input"].generate())
            continue

# Source directories for file processing
def get_input_data() -> pd.DataFrame: # 1st level
    while True:
        # Render menu
        print("\n".join([Header.ELEMENTS["src_dirs"].generate(), MenuLine.ELEMENTS["exit"].generate(), MenuLine.ELEMENTS["csv_load"].generate(), MenuLine.ELEMENTS["manual_load"].generate()]))
        # Request user input
        try:
            input_option = input(Prompt.ELEMENTS["base"].generate())
            input_option = lowercase_text(strip_text(input_option))
        except KeyboardInterrupt:
            print("Execution interrupted")
            return pd.DataFrame()
        
        # User input handling
        selected_dirs, in_action = upload_dirs(input_option)
        # Loop control parameters check
        match in_action:
            case MenuActions.INTERUPT:
                continue
            case MenuActions.FAILED:
                continue
            case MenuActions.SUCCESS:
                return selected_dirs