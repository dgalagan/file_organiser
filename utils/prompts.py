from cli.components import Colored, Ansi
from cli.tokens import Icon
from enum import StrEnum, auto
import sys

WARNING_ICON = Colored(text=Icon.WARNING, color_code=Ansi.YELLOW)
PROMPT_ICON = Colored(text=Icon.GREATERTHAN, color_code=Ansi.CYAN)

class MenuActions(StrEnum):
    SKIP = auto()
    SUCCESS = auto()
    INTERRUPT = auto()

def invalid_input() -> str:
    return "Invalid input"

def request_move_approval() -> None:
    response = input(f"{WARNING_ICON} Source files will be permanently deleted\n{" " * len(WARNING_ICON)}\\__Proceed? [y/N]: ").strip().lower()
    if response != 'y':
        sys.exit(0)

def request_depth(dir_path: str, depth_range: list[int]) -> tuple[int, StrEnum]: # dependency: build_processing_configs()

    range_str = f"{depth_range[0]}-{depth_range[1]}" if depth_range[0] != depth_range[1] else depth_range[0]

    while True:
        try:
            prompt_msg = f"{PROMPT_ICON}{range_str:^7}"
            offset = len(prompt_msg) - PROMPT_ICON.invisible
            processing_depth = input(f"{prompt_msg}{dir_path}\n{" "*offset}\\__depth: ")
            if processing_depth == "":
                return -1, MenuActions.SKIP
            processing_depth = int(processing_depth)
            if depth_range[0] <= processing_depth <= depth_range[1]:
                return processing_depth, MenuActions.SUCCESS
            print(f"{" "*offset}{invalid_input()}")
            continue
        except ValueError:
            print(f"{" "*offset}{invalid_input()}")
            continue
        except KeyboardInterrupt:
            print()
            return -1, MenuActions.INTERRUPT