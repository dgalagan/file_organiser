from dataclasses import dataclass
from enum import StrEnum, auto
import os
import pandas as pd
from pandas.errors import ParserError
from string import Formatter
from typing import Tuple, Optional, Iterable, Iterator, Self

# Glossary
# Depth = maximum processing radius downward

# To improve:
# instead of os.walk(), create recursion based on os.scandir()

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
def open_csv(path: str) -> Tuple[pd.DataFrame | None, None | Exception]:
    try:
        return pd.read_csv(path), None
    except (FileNotFoundError, PermissionError, ParserError) as e:
        return None, e

def remove_duplicates(df: pd.DataFrame, column_name: Optional[str] = None) -> pd.DataFrame:

    # Check whether column provided
    if column_name is None:
        return df
    else:
        df_normalized = df.drop_duplicates(
            column_name, 
            inplace=False
            )
    return df_normalized

def filter_df(df: pd.DataFrame, condition: str) -> pd.DataFrame:
    return df.loc[condition].copy()

def remove_subfolders(paths: list[str], to_remove: list[str]) -> list[str]:
    return list(set(paths) - set(to_remove))

# CLI elements
class MenuActions(StrEnum):
    EXIT = auto()
    INTERUPT = auto()
    SKIP = auto()
    SKIP_ALL = auto()
    SUCCESS = auto()
    FAILED = auto()

class Template:
    SEP_MSG_SEP = "{start}{separator}{text}{separator}"
    EMO_SEP_MSG = "{start}{emoji}{separator}{text}"

class Token:
    def __init__(self, token):
        self.token = token
    
    def repeat(self, count: int, delim: str = "") -> str:
        return delim.join([self.token] * count)

class Delimiter:
    SPACE = " "
    DASH = "-"
    COMMA = ","
    PIPE = "|"
    FORWARDSLASH = "/"
    BACKSLASH = "\\"

class Emoji:
    KEYBOARD = Token('⌨️')
    CHECKMARK = Token('✅')
    CROSSMARK = Token('❌')
    STOPSIGN = '🛑'
    WARNINGSIGN = '⚠️'
    RIGHTARROW = '➡️'
    DOWNARROW = "⬇️"
    LEFTWARDARROW = '↩️'
    RESTART = '🔄'
    INFORMATION = 'ℹ️'
    BULLSEYE = Token('🎯')
    HOURGLASS = Token('⏳')

class Icon:
    DOWNARROW = "↓"

DELIMITERS = [" ", "|", "-", ","]
HEADER = "{start}{separator}{text}{separator}"
SHARED = "{start}{emoji}{separator}{text}"

cli_tokens = {
    "emojis": {
        "empty": '🚫',
        "keyboard":'⌨️',
        "check_mark": '✅',
        "cross_mark": '❌',
        "stop_sign": '🛑',
        "warning_sign": '⚠️',
        "right_arrow": '➡️',
        "down_arrow": "⬇️",
        "leftwards_arrow_with_hook": '↩️',
        "restart": '🔄',
        "information":'ℹ️',
        "bullseye": '🎯',
        "hourglass": '⏳'
    },
    "icons": {
        "down_arrow": "↓",
    },
}
cli_objects = {  
    "headers": {
        "template": HEADER,
        "defaults": {
            "start": "\n",
            "separator": DELIMITERS[2] * 5,
            "text": "default"
        },
        "elements":{
            "main": {"text": "Main"},
            "csv_load": {"text": "CSV load"},
            "manual_load": {"text": "Manual load"},
            "depth": {"text": "Depth"}
        }
    },
    "menu_lines": {
        "template": "shared",
        "defaults": {
            "start": "",
            "emoji": ("empty", 1),
            "separator": DELIMITERS[0] * 2,
            "text": "default"
        },
        "elements": {
            "exit": {"emoji": ("cross_mark", 1), "separator": DELIMITERS[0], "text": "Press 'Ctrl+C' to suspend the script"},
            "return_back": {"emoji": ("leftwards_arrow_with_hook", 1), "text": "Press 'Ctrl+C' to go back"},
            "restart": {"emoji": ("restart", 1), "separator": DELIMITERS[0], "text": "Press 'Ctrl+C' to cancel current input and retry"},
            "skip": {"emoji": ("keyboard", 1), "text": "Type 'skip' to skip folder path"},
            "skip_all": {"emoji": ("keyboard", 1), "text": "Type 'skipall' to skip the rest of folder path(s)"},
            "csv_load": {"emoji": ("keyboard", 1), "text": "Type 'csv' to load folder path(s) from CSV"},
            "manual_load": {"emoji": ("keyboard", 1),"text": "Type 'manual' to provide folder path(s) directly in CLI"}
        }
    },
    "prompts": {
        "template": SHARED,
        "defaults": {
            "start": "",
            "emoji": ("right_arrow", 1),
            "separator": DELIMITERS[0] * 2,
            "text": "Provide your option: "
        },
        "elements": {
            "csv": {"text": "Provide link to CSV file: "},
            "manual": {"text": "Provide one or several folder path(s) separated with {paths_separator}: "},
            "depth": {"text": "Provide 'depth' value from the range {depth_range}: "}
        }
    },
    "warnings": {
        "template": SHARED,
        "defaults": {
            "start": "",
            "emoji": ("warning_sign", 1),
            "separator": DELIMITERS[0] * 2,
            "text": "default"
        },
        "elements": {
            "invalid_input": {"text": "Invalid input"}, # General
            "empty_input": {"text": "No folder path(s) to process"}, # General
            "csv_load_failed": {"text": "CSV loading failed with {error}"}, # CSV
            "file_not_found": {"text": "Provided path '{path}' is not a file"}, # File / Extension
            "extension_not_supported": {"text": "Provided extension '{ext}' is not supported"}, # File / Extension
            "missing_columns": {"text": "Required columns {cols} are missing"}, # Columns
        }
    },
    "infos": {
        "template": SHARED,
        "defaults": {
            "start": "",
            "emoji": ("information", 1),
            "separator": DELIMITERS[0] * 2,
            "text": "default"
        },
        "elements": {
            "exit": {"text": "Script terminated"}, # General
            "output_ready": {"emoji": ("bullseye", 3), "text": "Output ready"}, # General
            "valid_paths": {"text": "Valid folder path(s) identified {path_count}"}, # Folder paths
            "duplicated_removed": {"text": "{path_count} duplicate path(s) removed"}, # Folder paths
            "corrupted_removed": {"text": "{path_count} corrupted path(s) removed"}, # Folder paths
            "processing": {"emoji": ("hourglass", 1), "separator": DELIMITERS[0], "text": "[Processing] -----> {path}"}, # Paths selection
            "added": {"text": "[Added] -----> {path_count} folder path(s)"}, # Paths selection
            "skipped": {"text": "[Skipped] -----> as already in scope"}, # Paths selection
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
    ]
}

# CLI execution logic
def verify_cli_token_config(token_config: tuple, tokens: dict) -> dict:
    params = {}
    for index, value in enumerate(token_config):
        if index == 0 and value in tokens:
            params["token"] = tokens[value]
        if index == 1 and isinstance(value, int):
            params["count"] = value
        if index == 2 and len(set(value)) == 1 and list(set(value))[0] in cli_tokens["delimiters"]:
            params["delimiter"] = value
    return params

def render_cli_token(token, count, delimiter=""):
    if not isinstance(token, str):
        raise ValueError(f"token must be a string, got {type(token).__name__}")
    if not isinstance(count, int) or count < 1:
        raise ValueError(f"token_count must be an int > 0, got {type(count).__name__} with value {count}")
    if not isinstance(delimiter, str):
        raise ValueError(f"delimiter must be a string, got {type(delimiter).__name__}")
    tokens = [token] * count
    return delimiter.join(tokens)

def render_cli_object(cli_tokens: dict, cli_object: dict, key: str = None, required_tokens: set = {"emojis", "icons"}, **runtime_args) -> str:
    # Validate CLI object dict
    assert isinstance(cli_object, dict), f"CLI group should be a dictionary, {type(cli_object)} provided instead"
    assert "template" in cli_object, "Template is missing"
    assert "defaults" in cli_object, "Defaults is missing"
    assert "elements" in cli_object, "Elements is missing"
    # Validate CLI assets
    assert isinstance(cli_tokens, dict), f"CLI tokens should be a dictionary, {type(cli_tokens)} provided instead"
    missing_tokens = required_tokens - cli_tokens.keys()
    assert not missing_tokens, f"Missing CLI assets keys: {missing_tokens}"
    # Unpack CLI assets
    template = cli_object.get("template")
    elements = cli_object.get("elements")
    # Merge default and element configurations
    element_config = elements.get(key, {})
    default_config = cli_object.get("defaults", {})
    merged_config = {**default_config, **element_config}
    # Validate template placeholders
    template_args = get_placeholders(template)
    missing_configs = template_args - merged_config.keys()
    assert not missing_configs, f"Missing config arguments keys: {missing_configs}"
    # Fill in the template placeholders
    if "icon" in merged_config:
        icons_config = merged_config["icon"]
        params = verify_cli_token_config(icons_config, cli_tokens["icons"])
        merged_config["icon"] = render_cli_token(**params)
    if "emoji" in merged_config:
        emoji_config = merged_config["emoji"]
        params = verify_cli_token_config(emoji_config, cli_tokens["emojis"])
        merged_config["emoji"] = render_cli_token(**params)
    if "text" in merged_config:
        text_args = get_placeholders(merged_config["text"])
        if text_args:
            missing = text_args - runtime_args.keys()
            assert not missing, f"Missing arguments for placeholders: {missing}"
            merged_config["text"] = merged_config["text"].format(**runtime_args)
    final_element = template.format(**merged_config)
    return final_element


def prompt_user(prompt: str)-> Tuple[str | None, MenuActions | None]:
    try:
        user_input = input(prompt)
        return user_input, MenuActions.SUCCESS
    except KeyboardInterrupt:
        return None, MenuActions.INTERUPT

def main_loop(cli_tokens, cli_objects, cli_grouped_objects): # 1st level
    
    input_handler = {
        "csv": csv_loop,
        "manual": manual_loop,
    }

    while True:
        # Request user input
        main_menu = cli_grouped_objects["main_menu"]
        for menu_line in main_menu:
            object_name, element = menu_line
            print(render_cli_object(cli_tokens, cli_objects[object_name], element))
        user_input, action = prompt_user(render_cli_object(cli_tokens, cli_objects["prompts"]))
        match action:
            case MenuActions.SUCCESS:
                user_input = lower_text(strip_text(user_input))
            case MenuActions.INTERUPT:
                print(render_cli_object(cli_tokens, cli_objects["infos"], "exit"))
                break

        #  User input handling
        handler = input_handler.get(user_input)

        if not handler:
            print(render_cli_object(cli_tokens, cli_objects["warnings"], "invalid_input"))
            continue
        
        loop_func = handler
        folder_scope, in_action = loop_func(cli_tokens, cli_objects, cli_grouped_objects)

        # Loop control parameters check
        match in_action:
            case MenuActions.INTERUPT:
                continue
            case MenuActions.FAILED:
                print(render_cli_object(cli_tokens, cli_objects["warnings"], "empty_input"))
                continue
            case MenuActions.SUCCESS:
                print(render_cli_object(cli_tokens, cli_objects["infos"], "output_ready"))
                return folder_scope

def csv_loop(cli_tokens, cli_objects, cli_grouped_objects): # 2nd level
    
    # Required CSV columns for correct input validation
    required_col1 = "FolderPath"
    test_req1 = "FolderPathTest"
    
    while True:
        # Request user input
        csv_menu = cli_grouped_objects["csv_menu"]
        for menu_line in csv_menu:
            object_name, element = menu_line
            print(render_cli_object(cli_tokens, cli_objects[object_name], element))
        user_input, action = prompt_user(render_cli_object(cli_tokens, cli_objects["prompts"], "csv"))
        match action:
            case MenuActions.SUCCESS:
                user_input = strip_text(user_input)
            case MenuActions.INTERUPT:
                print()
                return None, MenuActions.INTERUPT
        
        # Check whether provided link is file, otherwise continue
        if not is_file(user_input):
            print(render_cli_object(cli_tokens, cli_objects["warnings"], "file_not_found", path=user_input))
            continue
        # Check whether file extension == 'csv', otherwise continue
        file_ext = get_file_extension(user_input)
        file_ext = lower_text(strip_text(file_ext))
        if file_ext != '.csv':
            print(render_cli_object(cli_tokens, cli_objects["warnings"], "extension_not_supported", ext=file_ext))
            continue
        # Open CSV file as dataframe
        csv_data, e = open_csv(user_input)
        if e:
            print(render_cli_object(cli_tokens, cli_objects["warnings"], "csv_load_failed", error=e))
            continue
        # Validate CSV columns
        if required_col1 not in csv_data.columns:
            print(render_cli_object(cli_tokens, cli_objects["warnings"], "missing_columns", cols=required_col1))
            continue
        # Validate CSV data
        csv_data[test_req1] = csv_data[required_col1].apply(lambda x: True if is_folder(x) else False)
        # Normalize CSV data
        normalized_csv_data = remove_duplicates(csv_data, column_name=required_col1)
        duplicates_count = csv_data.shape[0] - normalized_csv_data.shape[0]
        if duplicates_count:
            print(render_cli_object(cli_tokens, cli_objects["infos"], "duplicated_removed", path_count=duplicates_count))
        # Select valid entries
        condition = normalized_csv_data[test_req1] == True
        filtered_csv_data = filter_df(normalized_csv_data, condition)
        if filtered_csv_data.empty:
            print(render_cli_object(cli_tokens, cli_objects["warnings"], "empty_input"))
            continue
        # Define max depth available
        filtered_csv_data["FolderPathDepth"] = filtered_csv_data[required_col1].apply(lambda x: get_path_depth(x))
        filtered_csv_data["BranchMaxDepth"] = filtered_csv_data[required_col1].apply(lambda x: get_max_depth_from_path(x))
        # Define depth and resolve parent-child relationship
        max_depth = filtered_csv_data["BranchMaxDepth"].max()
        paths_by_depth = {depth: [] for depth in range(1, max_depth + 1)}
        depths = sorted(list(set(filtered_csv_data["FolderPathDepth"])))
        skip_all = False
        reload = False
        total_paths_added = 0
        for depth in depths:
            temp_data = filtered_csv_data.loc[filtered_csv_data["FolderPathDepth"] == depth]
            for idx in temp_data.index:
                folder_path = strip_text(temp_data.loc[idx, "FolderPath"], char_to_remove=os.sep)
                max_depth = temp_data.loc[idx, "BranchMaxDepth"]
                depth_options = [depth for depth in range(depth, max_depth + 1)]
                print(DELIMITERS[2] * 80)
                print(render_cli_object(cli_tokens, cli_objects["infos"], "processing", path=folder_path))
                print(render_cli_token(cli_tokens["icons"]["down_arrow"], 3))
                if folder_path not in paths_by_depth[depth]:
                    depth_input, in_action = depth_loop(cli_tokens, cli_objects, cli_grouped_objects, depth_options)
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
                            print(render_cli_token(cli_tokens["icons"]["down_arrow"], 3))
                            print(render_cli_object(cli_tokens, cli_objects["infos"], "added", path_count=paths_added))
                            continue
                else:
                    print(render_cli_object(cli_tokens, cli_objects["infos"], "skipped"))
                    # hierarchy resolution, ask whether child should be processed separately, delete all related path from parent search, add new 
            if skip_all or reload:
                break
        if reload:
            continue
        print(DELIMITERS[2] * 80)
        print(f"Total paths selected {total_paths_added}")
        
        if total_paths_added == 0:
            return paths_by_depth, MenuActions.FAILED
        
        return paths_by_depth, MenuActions.SUCCESS

def manual_loop(cli_tokens, cli_objects, cli_grouped_objects): # 2nd level
    
    # Separator to parse user input
    separator = ","
    required_col1 = "FolderPath"

    while True:
        # Request user input
        manual_menu = menus["manual_menu"]
        for menu_line in manual_menu:
            config_name, context = menu_line
            print(render_cli_element(cli_elements[config_name], context, cli_assets))
        user_input, action = prompt_user(render_cli_element(cli_elements["prompts"], "manual", cli_assets, paths_separator=separator))
        match action:
            case MenuActions.SUCCESS:
                user_input = strip_text(user_input)
            case MenuActions.INTERUPT:
                print()
                return None, MenuActions.INTERUPT
        
        # Parse user input
        folder_paths = split_text(user_input, separator)
        
        # Process folder path(s)
        duplicated_paths = []
        corrupted_paths = []
        valid_paths = []

        for folder_path in folder_paths:
            if is_folder(folder_path):
                if folder_path not in valid_paths:
                    valid_paths.append(folder_path)
                else:
                    duplicated_paths.append(folder_path)
            else:
                corrupted_paths.append(folder_path)

        # Notify user about valid entries
        if not valid_paths:
            print(render_cli_element(cli_elements["warnings"], "invalid_input", cli_assets))
            continue
        print(render_cli_element(cli_elements["infos"], "valid_paths", cli_assets, path_count=len(valid_paths)))
        # Notify user about duplicated entries
        if duplicated_paths:
            print(render_cli_element(cli_elements["infos"], "duplicated_removed", cli_assets, path_count=len(duplicated_paths)))
        # Notify user about corrupted entries
        if corrupted_paths:
            print(render_cli_element(cli_elements["infos"], "corrupted_removed", cli_assets, path_count=len(corrupted_paths)))

        path_data = pd.DataFrame()
        path_data[required_col1] = valid_paths
        path_data["FolderPathDepth"] = path_data[required_col1].apply(lambda x: get_path_depth(x))
        path_data["BranchMaxDepth"] = path_data[required_col1].apply(lambda x: get_max_depth_from_path(x))

        # Define depth and resolve parent-child relationship
        max_depth = path_data["BranchMaxDepth"].max()
        folder_scope = {depth: [] for depth in range(1, max_depth + 1)}
        folder_depths = sorted(list(set(path_data["FolderPathDepth"])))
        skip_all = False
        reload = False
        for folder_depth in folder_depths:
            temp_data = path_data.loc[path_data["FolderPathDepth"] == folder_depth]
            for idx in temp_data.index:
                folder_path = strip_text(temp_data.loc[idx, "FolderPath"], char_to_remove=os.sep)
                max_depth = temp_data.loc[idx, "BranchMaxDepth"]
                depth_options = [depth for depth in range(folder_depth, max_depth + 1)]
                if folder_path not in folder_scope[folder_depth]:
                    print(render_cli_element(cli_elements["infos"], "processing", cli_assets, path=folder_path))
                    depth_input, in_action = depth_loop(menus, cli_elements, cli_assets, depth_options)
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
                            folders_added = 0
                            for depth, folder_path in iter_hierarchy_until_depth(folder_path, depth_input):
                                folder_scope[depth].append(folder_path)
                                folders_added += 1
                            print(f"Folders added {folders_added}")
                            continue
                else:
                    print(render_cli_element(cli_elements["infos"], "hierarchy_clash", cli_assets))
            
            if skip_all or reload:
                break
        if reload:
            continue
        
        total_paths_added = 0
        for depth in folder_scope:
            total_paths_added += len(folder_scope[depth])
        
        if total_paths_added == 0:
            return folder_scope, MenuActions.FAILED
        
        return folder_scope, MenuActions.SUCCESS

def depth_loop(cli_tokens, cli_objects, cli_grouped_objects, depth_options): # 3rd level

    while True:
        # Request user input
        depth_menu = cli_grouped_objects["depth_menu"]
        for menu_line in depth_menu:
            object_name, element = menu_line
            print(render_cli_object(cli_tokens, cli_objects[object_name], element))
        depth_input, action = prompt_user(render_cli_object(cli_tokens, cli_objects["prompts"], "depth", depth_range=depth_options))
        match action:
            case MenuActions.SUCCESS:
                depth_input = lower_text(strip_text(depth_input))
            case MenuActions.INTERUPT:
                print()
                return None, MenuActions.INTERUPT

        if depth_input == "skip":
            return None, MenuActions.SKIP
        elif depth_input == "skipall":
            return None, MenuActions.SKIP_ALL

        try:
            depth_input = int(depth_input)
            if depth_input in depth_options:
                return depth_input, MenuActions.SUCCESS
            else:
                print(render_cli_object(cli_tokens, cli_objects["warnings"], "invalid_input"))
                continue
        except ValueError:
            print(render_cli_object(cli_tokens, cli_objects["warnings"], "invalid_input"))

if __name__ == "__main__":
    # paths_by_depth = main_loop(cli_tokens, cli_objects, cli_grouped_objects)
    print(f"{Emoji.KEYBOARD.repeat(-2, delim="  | ")}")