from enum import StrEnum, auto
import os
import pandas as pd
from pandas.errors import ParserError, EmptyDataError
from string import Formatter
from typing import Tuple, Optional, Iterable, Iterator

# Glossary
# Depth = maximum processing radius from drive
# depth starting index 0 vs 1 ?

# To improve:
# instead of os.walk(), create recursion based on os.scandir()
# auto alignment of header object between separators
# create function for duplicated code part in csv and manual loop
# convert paths by depth ito 1 list

## String helpers
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

def get_placeholders(text: str) -> set:
    return {
        placehoder
        for _, placehoder, _, _, in Formatter().parse(text)
        if placehoder
    }

## Path helpers
def is_file(path: str) -> bool:
    return os.path.isfile(path)

def is_folder(path:str) -> bool:
    return os.path.isdir(path)

def get_file_extension(path: str) -> str:
    _, file_extension = os.path.splitext(path)
    return file_extension

def has_valid_extension(path: str, expected_ext: str) -> bool:
    if not expected_ext:
        raise ValueError("You must provide an expected extension to validate against.")
    file_ext = get_file_extension(path)
    file_ext = lower_text(strip_text(file_ext))
    return file_ext == expected_ext

def get_file_basename(path: str) -> str:
    file_basename, _ = os.path.splitext(path)
    return file_basename

def get_abs_path(path: str) -> str:
    return os.path.abspath(path)

def get_common_path(paths: Iterable[str]) -> str:
    return os.path.commonpath(paths)

def get_path_length(path: str, path_separator: str = os.sep) -> int:
    path_elements = split_text(path, path_separator)
    path_length = len(path_elements)
    return path_length

def is_parent(path, of_path):
    assert is_folder(path), f"Provided path is not a folder {path}"
    assert is_folder(of_path), f"Provided path is not a folder {of_path}"
    abs_path = get_abs_path(path)
    abs_of_path = get_abs_path(of_path)
    common_path = get_common_path([abs_path, abs_of_path])
    return abs_path == common_path and abs_path != abs_of_path

def get_drive_root(path: str) -> str:
    assert is_folder(path), f"Provided path is not a folder {path}"
    abs_path = get_abs_path(path)
    drive_root, _ = os.path.splitdrive(abs_path)
    return drive_root

def get_path_depth(path: str) -> int:
    assert is_folder(path), f"Provided path is not a folder {path}"
    abs_path = get_abs_path(path)
    normalize_path = strip_text(abs_path, char_to_remove=os.sep)
    return get_path_length(normalize_path)

def get_max_depth_from_path(path: str) -> int:
    assert is_folder(path), f"Provided path is not a folder {path}"
    return max(
        get_path_depth(root_path)
        for root_path, _ , _ in os.walk(path)
    )

def iter_hierarchy_until_depth(path: str, max_depth: int) -> Iterator[tuple[int, str]]:
    for root_path, folders , _ in os.walk(path):
        current_depth = get_path_depth(root_path)
        if current_depth >= max_depth:
            folders[:] = []
        yield current_depth, root_path

## Df helpers

def mark_duplicates(df: pd.DataFrame, column_name: Optional[str] = None) -> pd.Series:    
    return df.duplicated(subset=column_name)

def filter_df(df: pd.DataFrame, condition: str) -> pd.DataFrame:
    return df.loc[condition].copy()

def open_csv(path: str) -> Tuple[pd.DataFrame | None, None | Exception]:
    try:
        return pd.read_csv(path), None
    except (FileNotFoundError, PermissionError, ParserError, EmptyDataError) as e:
        return None, e

def has_required_cols(path_data: pd.DataFrame, required_col: str = "FolderPath") -> bool:
    return required_col in path_data.columns

def is_empty(path_data: pd.DataFrame) -> bool:
    return path_data.empty

def filter_invalids(path_data: pd.DataFrame) -> pd.DataFrame:
    # Validate input df
    if is_empty(path_data):
        return pd.DataFrame()
    path_data["is_valid"] = path_data["FolderPath"].apply(lambda path: True if is_folder(path) else False)
    num_invalid = path_data.shape[0] - path_data["is_valid"].sum()
    if num_invalid:
        print(render_cli_object(cli_objects["infos"], "invalid_entries", path_count=num_invalid))
        path_data = filter_df(path_data, path_data["is_valid"])
    return path_data

def filter_duplicates(path_data: pd.DataFrame) -> pd.DataFrame:
    # Validate input df
    if is_empty(path_data):
        return pd.DataFrame()
    path_data["is_duplicate"] = mark_duplicates(path_data, "FolderPath")
    num_duplicates = path_data["is_duplicate"].sum()
    if num_duplicates:
        print(render_cli_object(cli_objects["infos"], "duplicate_entries", path_count=num_duplicates))
        path_data = filter_df(path_data, ~path_data["is_duplicate"])
    return path_data

def enrich_path_data(path_data: pd.DataFrame) -> pd.DataFrame:
    if is_empty(path_data):
        return pd.DataFrame()
    # Enrich path data
    path_data["FolderPathDepth"] = path_data["FolderPath"].apply(lambda path: get_path_depth(path))
    path_data["BranchMaxDepth"] = path_data["FolderPath"].apply(lambda path: get_max_depth_from_path(path))
    return path_data

def get_paths_by_depth(path_data: pd.DataFrame) -> dict:
    # Normalize and enrich
    path_data = enrich_path_data(filter_duplicates(filter_invalids(path_data)))
    if is_empty(path_data):
        return None, MenuActions.FAILED
    # Define depth and resolve parent-child relationship
    max_depth = path_data["BranchMaxDepth"].max()
    paths_by_depth = {depth: [] for depth in range(1, max_depth + 1)}
    depths = sorted(list(set(path_data["FolderPathDepth"])))
    skip_all = False
    reload = False
    total_paths_added = 0
    for depth in depths:
        temp_data = path_data.loc[path_data["FolderPathDepth"] == depth]
        for idx in temp_data.index:
            folder_path = strip_text(temp_data.loc[idx, "FolderPath"], char_to_remove=os.sep)
            max_depth = temp_data.loc[idx, "BranchMaxDepth"]
            depth_options = [depth for depth in range(depth, max_depth + 1)]
            print(Delimiter.DASH.repeat(80))
            print(render_cli_object(cli_objects["infos"], "processing", path=folder_path))
            print(Icon.DOWNARROW.repeat(3))
            if folder_path not in paths_by_depth[depth]:
                depth_input, in_action = depth_loop(cli_grouped_objects, cli_objects, depth_options)
                match in_action:
                    case MenuActions.SKIP:
                        continue
                    case MenuActions.SKIP_ALL:
                        skip_all = True
                        break
                    case MenuActions.INTERUPT:
                        reload = True
                        break
                    case MenuActions.SUCCESS:
                        paths_added = 0
                        for depth, folder_path in iter_hierarchy_until_depth(folder_path, depth_input):
                            paths_by_depth[depth].append(folder_path)
                            paths_added += 1
                        total_paths_added += paths_added
                        print(Icon.DOWNARROW.repeat(3))
                        print(render_cli_object(cli_objects["infos"], "added", path_count=paths_added))
                        continue
            else:
                print(render_cli_object(cli_objects["infos"], "skipped"))
                # hierarchy resolution, ask whether child should be processed separately, delete all related path from parent search, add new 
        if skip_all or reload:
            break
    if reload:
        return None, MenuActions.RESTART
    print(Delimiter.DASH.repeat(80))
    print(render_cli_object(cli_objects["infos"], "selected", path_count=total_paths_added))
    print(Icon.DOWNARROW.repeat(3))
    
    if total_paths_added == 0:
        return None, MenuActions.FAILED
    
    return paths_by_depth, MenuActions.SUCCESS

# CLI elements
class MenuActions(StrEnum):
    EXIT = auto()
    INTERUPT = auto()
    SKIP = auto()
    SKIP_ALL = auto()
    SUCCESS = auto()
    FAILED = auto()
    RESTART = auto()

class Template:
    SEP_MSG_SEP = "{start}{sep}{msg}{sep}"
    ICON_SEP_MSG = "{start}{icon}{sep}{msg}"
class Token:
    def __init__(self, token: str):
        self.token = token
    def repeat_with_delim(self, count: int, delim: str = "") -> str:
        if not isinstance(count, int) or count < 1:
            raise ValueError(f"count must be an int > 0, got {type(count).__name__} with value {count}")
        if not isinstance(delim, str):
            raise ValueError(f"delimiter must be a string, got {type(delim).__name__}")
        return delim.join([self.token] * count)
    def repeat(self, count: int) -> str:
        return self.token * count
    def __str__(self):
        return self.token
class Delimiter:
    SPACE = Token(" ")
    DASH = Token("-")
    COMMA = Token(",")
    PIPE = Token("|")
    FORWARDSLASH = Token("/")
    BACKSLASH = Token("\\")
class Emoji:
    KEYBOARD = Token('⌨️')
    CHECKMARK = Token('✅')
    CROSSMARK = Token('❌')
    STOPSIGN = Token('🛑')
    WARNINGSIGN = Token('⚠️')
    RIGHTARROW = Token('➡️')
    DOWNARROW = Token("⬇️")
    LEFTWARDARROW = Token('↩️')
    RESTART = Token('🔄')
    INFORMATION = Token('ℹ️')
    BULLSEYE = Token('🎯')
    HOURGLASS = Token('⏳')
    CHEQUEREDFLAG = Token('🏁')
class Icon:
    DOWNARROW = Token("↓")

cli_objects = {  
    "headers": {
        "template": Template.SEP_MSG_SEP,
        "defaults": {
            "start": "\n",
            "sep": Delimiter.DASH,
            "msg": "empty"
        },
        "elements":{
            "main": {"sep": Delimiter.DASH.repeat(25), "msg": "Main"},
            "csv_load": {"sep": Delimiter.DASH.repeat(23), "msg": "CSV load"},
            "manual_load": {"sep": Delimiter.DASH.repeat(22), "msg": "Manual load"},
            "depth": {"sep": Delimiter.DASH.repeat(25), "msg": "Depth"}
        }
    },
    "menu_lines": {
        "template": Template.ICON_SEP_MSG,
        "defaults": {
            "start": "",
            "icon": Emoji.KEYBOARD,
            "sep": Delimiter.SPACE.repeat(2),
            "msg": "empty"
        },
        "elements": {
            "exit": {"icon": Emoji.CROSSMARK, "sep": Delimiter.SPACE, "msg": "Press 'Ctrl+C' to suspend the script"},
            "return_back": {"icon": Emoji.LEFTWARDARROW, "msg": "Press 'Ctrl+C' to go back"},
            "restart": {"icon": Emoji.RESTART, "sep": Delimiter.SPACE, "msg": "Press 'Ctrl+C' to cancel current input and retry"},
            "skip": {"msg": "Type 'skip' to skip current folder path"},
            "skip_all": {"msg": "Type 'skipall' to skip the rest of folder path(s)"},
            "csv_load": {"msg": "Type 'csv' to load folder path(s) from CSV"},
            "manual_load": {"msg": "Type 'manual' to provide folder path(s) directly in CLI"},
            "depth": {"msg": "Select 'depth level' from {depth_range}"}
        }
    },
    "prompts": {
        "template": Template.ICON_SEP_MSG,
        "defaults": {
            "start": "",
            "icon": Emoji.RIGHTARROW,
            "sep": Delimiter.SPACE.repeat(2),
            "msg": "Provide your option: "
        },
        "elements": {
            "csv": {"msg": "Provide link to CSV file: "},
            "manual": {"msg": "Provide one or several folder path(s) separated with {paths_separator}: "},
        }
    },
    "warnings": {
        "template": Template.ICON_SEP_MSG,
        "defaults": {
            "start": "",
            "icon": Emoji.WARNINGSIGN,
            "sep": Delimiter.SPACE.repeat(2),
            "msg": "empty"
        },
        "elements": {
            "invalid_input": {"msg": "Invalid input"}, # General
            "empty_input": {"msg": "No folder path(s) to process"}, # General
            "csv_load_failed": {"msg": "CSV loading failed with the reason - {error}"}, # CSV
            "file_not_found": {"msg": "Provided path '{path}' is not a file"}, # File / Extension
            "extension_not_supported": {"msg": "Provided extension '{ext}' is not supported"}, # File / Extension
            "missing_columns": {"msg": "Required columns {cols} are missing"}, # Columns
        }
    },
    "infos": {
        "template": Template.ICON_SEP_MSG,
        "defaults": {
            "start": "",
            "icon": Emoji.INFORMATION,
            "sep": Delimiter.SPACE.repeat(2),
            "msg": "empty"
        },
        "elements": {
            "exit": {"msg": "Script terminated"}, # General
            "output_ready": {"icon": Emoji.CHEQUEREDFLAG, "msg": "Output ready"},
            "invalid_entries": {"msg": "{path_count} invalid path(s) removed"},
            "duplicate_entries": {"msg": "{path_count} duplicate path(s) removed"},
            "processing": {"icon": Emoji.HOURGLASS, "sep": Delimiter.SPACE, "msg": "[Processing] -----> {path}"},
            "added": {"msg": "[Added] -----> {path_count} folder path(s)"},
            "skipped": {"msg": "[Skipped] -----> as already in scope"},
            "selected":{"icon": Emoji.BULLSEYE.repeat(1), "msg": "[Selected] -----> {path_count} folder path(s)"}
        }
    },
}
cli_grouped_objects = {
    "main_menu": [
        ("headers", "main"),
        ("menu_lines", "exit"),
        ("menu_lines", "csv_load"),
        ("menu_lines", "manual_load")
    ],
    "csv_menu": [
        ("headers", "csv_load"),
        ("menu_lines", "return_back")
    ],
    "manual_menu": [
        ("headers", "manual_load"),
        ("menu_lines", "return_back")
    ],
    "depth_menu": [
        ("menu_lines", "restart"),
        ("menu_lines", "skip_all"),
        ("menu_lines", "skip"),
        ("menu_lines", "depth")
    ]
}

def render_cli_object(cli_object: dict, element_name: str = None, **runtime_args) -> str:
    # Validate CLI object dict
    assert isinstance(cli_object, dict), f"CLI object should be a dictionary, {type(cli_object)} provided instead"
    assert "template" in cli_object, "Template is missing"
    assert "defaults" in cli_object, "Defaults is missing"
    assert "elements" in cli_object, "Elements is missing"
    # Unpack CLI assets
    template = cli_object.get("template", {})
    elements = cli_object.get("elements", {})
    # Merge default and element configurations
    element_config = elements.get(element_name, {})
    default_config = cli_object.get("defaults", {})
    merged_config = {**default_config, **element_config}
    # Validate template placeholders
    template_args = get_placeholders(template)
    missing_configs = template_args - merged_config.keys()
    assert not missing_configs, f"Missing config arguments keys: {missing_configs}"
    # Fill in the template placeholders
    if "msg" in merged_config:
        text_args = get_placeholders(merged_config["msg"])
        if text_args:
            missing = text_args - runtime_args.keys()
            assert not missing, f"Missing arguments for placeholders: {missing}"
            merged_config["msg"] = merged_config["msg"].format(**runtime_args)
    final_element = template.format(**merged_config)
    return final_element

def render_cli_grouped_object(cli_grouped_object: dict, cli_objects: dict, **runtime_args) -> str: # hardcoded "\n"
    rendered_objects = []
    for cli_object_config in cli_grouped_object:
        object_name, element_name = cli_object_config
        cli_object = cli_objects.get(object_name, {})
        rendered_object = render_cli_object(cli_object, element_name, **runtime_args)
        rendered_objects.append(rendered_object)
    return "\n".join(rendered_objects)

def prompt_user(prompt: str)-> Tuple[str | None, MenuActions | None]:
    try:
        user_input = input(prompt)
        return user_input, MenuActions.SUCCESS
    except KeyboardInterrupt:
        return None, MenuActions.INTERUPT

def main_loop(cli_grouped_objects, cli_objects): # 1st level
    input_handler = {
        "csv": csv_loop,
        "manual": manual_loop,
    }
    while True:
        # Request user input
        print(render_cli_grouped_object(cli_grouped_objects["main_menu"], cli_objects))
        user_input, action = prompt_user(render_cli_object(cli_objects["prompts"]))
        match action:
            case MenuActions.SUCCESS:
                user_input = lower_text(strip_text(user_input))
                #  User input handling
                handler = input_handler.get(user_input)
                if not handler:
                    print(render_cli_object(cli_objects["warnings"], "invalid_input"))
                    continue
                loop_func = handler
                paths_by_depth, in_action = loop_func(cli_grouped_objects, cli_objects)
                # Loop control parameters check
                match in_action:
                    case MenuActions.INTERUPT:
                        continue
                    case MenuActions.SUCCESS:
                        print(render_cli_object(cli_objects["infos"], "output_ready"))
                        return paths_by_depth
            case MenuActions.INTERUPT:
                print()
                print(Icon.DOWNARROW.repeat(3))
                print(render_cli_object(cli_objects["infos"], "exit"))
                break

def csv_loop(cli_grouped_objects, cli_objects): # 2nd level    
    while True:
        # Request user input
        print(render_cli_grouped_object(cli_grouped_objects["csv_menu"], cli_objects))
        user_input, action = prompt_user(render_cli_object(cli_objects["prompts"], "csv"))
        match action:
            case MenuActions.SUCCESS:
                user_input = strip_text(user_input)
                # Check whether provided link is file, otherwise continue
                if not is_file(user_input):
                    print(render_cli_object(cli_objects["warnings"], "file_not_found", path=user_input))
                    continue
                # Check whether file extension == 'csv', otherwise continue
                if not has_valid_extension(user_input, '.csv'):
                    print(render_cli_object(cli_objects["warnings"], "extension_not_supported", ext=user_input))
                    continue
                # Open CSV file as dataframe
                raw_data, e = open_csv(user_input)
                if e:
                    print(render_cli_object(cli_objects["warnings"], "csv_load_failed", error=e))
                    continue
                if not has_required_cols(raw_data):
                    print(render_cli_object(cli_objects["warnings"], "missing_columns", cols="FolderPath"))
                    continue
                # Get paths by depths
                paths_by_depth, in_action = get_paths_by_depth(raw_data)
                match in_action:
                    case MenuActions.RESTART:
                        continue
                    case MenuActions.FAILED:
                        print(render_cli_object(cli_objects["warnings"], "empty_input"))
                        continue
                    case MenuActions.SUCCESS:
                        return paths_by_depth, MenuActions.SUCCESS
            case MenuActions.INTERUPT:
                print()
                return None, MenuActions.INTERUPT

def manual_loop(cli_grouped_objects, cli_objects): # 2nd level # hardcoded ","
    # Separator to parse user input
    separator = ","

    while True:
        # Request user input
        print(render_cli_grouped_object(cli_grouped_objects["manual_menu"], cli_objects))
        user_input, action = prompt_user(render_cli_object(cli_objects["prompts"], "manual", paths_separator=separator))
        match action:
            case MenuActions.SUCCESS:
                raw_data = pd.DataFrame({"FolderPath": split_text(strip_text(user_input), separator)})
                # Get paths by depths
                paths_by_depth, in_action = get_paths_by_depth(raw_data)
                match in_action:
                    case MenuActions.RESTART:
                        continue
                    case MenuActions.FAILED:
                        print(render_cli_object(cli_objects["warnings"], "empty_input"))
                        continue
                    case MenuActions.SUCCESS:
                        return paths_by_depth, MenuActions.SUCCESS
            case MenuActions.INTERUPT:
                print()
                return None, MenuActions.INTERUPT

def depth_loop(cli_grouped_objects, cli_objects, depth_options): # 3rd level

    while True:
        # Request user input
        print(render_cli_grouped_object(cli_grouped_objects["depth_menu"], cli_objects, depth_range=depth_options))
        depth_input, action = prompt_user(render_cli_object(cli_objects["prompts"]))
        match action:
            case MenuActions.SUCCESS:
                depth_input = lower_text(strip_text(depth_input))
                if depth_input == "skip":
                    return None, MenuActions.SKIP
                elif depth_input == "skipall":
                    return None, MenuActions.SKIP_ALL

                try:
                    depth_input = int(depth_input)
                    if depth_input in depth_options:
                        return depth_input, MenuActions.SUCCESS
                    else:
                        print(render_cli_object(cli_objects["warnings"], "invalid_input"))
                        continue
                except ValueError:
                    print(render_cli_object(cli_objects["warnings"], "invalid_input"))
            case MenuActions.INTERUPT:
                print()
                return None, MenuActions.INTERUPT

if __name__ == "__main__":
    paths_by_depth = main_loop(cli_grouped_objects, cli_objects)
    if paths_by_depth:
        print(paths_by_depth)