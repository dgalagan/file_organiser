from enum import StrEnum, auto
import os
import pandas as pd
from pandas.errors import ParserError
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
            "skip": {"msg": "Type 'skip' to skip folder path"},
            "skip_all": {"msg": "Type 'skipall' to skip the rest of folder path(s)"},
            "csv_load": {"msg": "Type 'csv' to load folder path(s) from CSV"},
            "manual_load": {"msg": "Type 'manual' to provide folder path(s) directly in CLI"}
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
            "depth": {"msg": "Provide 'depth' value from the range {depth_range}: "}
        }
    },
    "warnings": {
        "template": Template.ICON_SEP_MSG,
        "defaults": {
            "start": "",
            "icon": Emoji.WARNINGSIGN,
            "sep": Delimiter.SPACE,
            "msg": "empty"
        },
        "elements": {
            "invalid_input": {"msg": "Invalid input"}, # General
            "empty_input": {"msg": "No folder path(s) to process"}, # General
            "csv_load_failed": {"msg": "CSV loading failed with {error}"}, # CSV
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
            "valid_paths": {"msg": "Valid folder path(s) identified {path_count}"},
            "duplicated_removed": {"msg": "{path_count} duplicate path(s) removed"},
            "corrupted_removed": {"msg": "{path_count} corrupted path(s) removed"},
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
    ]
}

def render_cli_object(object: dict, element: str = None, **runtime_args) -> str:
    # Validate CLI object dict
    assert isinstance(object, dict), f"CLI object should be a dictionary, {type(object)} provided instead"
    assert "template" in object, "Template is missing"
    assert "defaults" in object, "Defaults is missing"
    assert "elements" in object, "Elements is missing"
    # Unpack CLI assets
    template = object.get("template")
    elements = object.get("elements")
    # Merge default and element configurations
    element_config = elements.get(element, {})
    default_config = object.get("defaults", {})
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

def render_cli_grouped_object(grouped_object: dict, objects:dict) -> str:
    rendered_objects = []
    for object_config in grouped_object:
        object_name, element = object_config
        rendered_object = render_cli_object(objects[object_name], element)
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
            case MenuActions.INTERUPT:
                print()
                print(render_cli_object(cli_objects["infos"], "exit"))
                break

        #  User input handling
        handler = input_handler.get(user_input)

        if not handler:
            print(render_cli_object(cli_objects["warnings"], "invalid_input"))
            continue
        
        loop_func = handler
        folder_scope, in_action = loop_func(cli_grouped_objects, cli_objects)

        # Loop control parameters check
        match in_action:
            case MenuActions.INTERUPT:
                continue
            case MenuActions.FAILED:
                print(render_cli_object(cli_objects["warnings"], "empty_input"))
                continue
            case MenuActions.SUCCESS:
                print(render_cli_object(cli_objects["infos"], "output_ready"))
                return folder_scope

def csv_loop(cli_grouped_objects, cli_objects): # 2nd level
    
    # Required CSV columns for correct input validation
    required_col1 = "FolderPath"
    test_req1 = "FolderPathTest"
    
    while True:
        # Request user input
        print(render_cli_grouped_object(cli_grouped_objects["csv_menu"], cli_objects))
        user_input, action = prompt_user(render_cli_object(cli_objects["prompts"], "csv"))
        match action:
            case MenuActions.SUCCESS:
                user_input = strip_text(user_input)
            case MenuActions.INTERUPT:
                print()
                return None, MenuActions.INTERUPT
        
        # Check whether provided link is file, otherwise continue
        if not is_file(user_input):
            print(render_cli_object(cli_objects["warnings"], "file_not_found", path=user_input))
            continue
        # Check whether file extension == 'csv', otherwise continue
        file_ext = get_file_extension(user_input)
        file_ext = lower_text(strip_text(file_ext))
        if file_ext != '.csv':
            print(render_cli_object(cli_objects["warnings"], "extension_not_supported", ext=file_ext))
            continue
        # Open CSV file as dataframe
        raw_data, e = open_csv(user_input)
        if e:
            print(render_cli_object(cli_objects["warnings"], "csv_load_failed", error=e))
            continue
        # Validate CSV columns
        if required_col1 not in raw_data.columns:
            print(render_cli_object(cli_objects["warnings"], "missing_columns", cols=required_col1))
            continue
        # ↓↓↓ same in manual
        # Validate CSV data
        raw_data[test_req1] = raw_data[required_col1].apply(lambda x: True if is_folder(x) else False)
        # Normalize CSV data
        normalized_data = remove_duplicates(raw_data, column_name=required_col1)
        duplicates_count = raw_data.shape[0] - normalized_data.shape[0]
        if duplicates_count:
            print(render_cli_object(cli_objects["infos"], "duplicated_removed", path_count=duplicates_count))
        # Select valid entries
        condition = normalized_data[test_req1] == True
        path_data = filter_df(normalized_data, condition)
        if path_data.empty:
            print(render_cli_object(cli_objects["warnings"], "empty_input"))
            continue
        # Enrich path data
        path_data["FolderPathDepth"] = path_data[required_col1].apply(lambda x: get_path_depth(x))
        path_data["BranchMaxDepth"] = path_data[required_col1].apply(lambda x: get_max_depth_from_path(x))
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
            continue
        print(Delimiter.DASH.repeat(80))
        print(render_cli_object(cli_objects["infos"], "selected", path_count=total_paths_added))
        print(Icon.DOWNARROW.repeat(3))
        
        if total_paths_added == 0:
            return paths_by_depth, MenuActions.FAILED
        
        return paths_by_depth, MenuActions.SUCCESS

def manual_loop(cli_grouped_objects, cli_objects): # 2nd level
    
    # Separator to parse user input
    separator = ","
    required_col1 = "FolderPath"
    test_req1 = "FolderPathTest"

    while True:
        # Request user input
        print(render_cli_grouped_object(cli_grouped_objects["manual_menu"], cli_objects))
        user_input, action = prompt_user(render_cli_object(cli_objects["prompts"], "manual", paths_separator=separator))
        match action:
            case MenuActions.SUCCESS:
                user_input = strip_text(user_input)
            case MenuActions.INTERUPT:
                print()
                return None, MenuActions.INTERUPT
        
        # Parse user input
        folder_paths = split_text(user_input, separator)
        raw_data = pd.DataFrame({required_col1: folder_paths})
        # ↓↓↓ same in csv
        # Validate CSV data
        raw_data[test_req1] = raw_data[required_col1].apply(lambda x: True if is_folder(x) else False)
        # Normalize CSV data
        normalized_data = remove_duplicates(raw_data, column_name=required_col1)
        duplicates_count = raw_data.shape[0] - normalized_data.shape[0]
        if duplicates_count:
            print(render_cli_object(cli_objects["infos"], "duplicated_removed", path_count=duplicates_count))
        # Select valid entries
        condition = normalized_data[test_req1] == True
        path_data = filter_df(normalized_data, condition)
        if path_data.empty:
            print(render_cli_object(cli_objects["warnings"], "empty_input"))
            continue
        # Enrich path data
        path_data["FolderPathDepth"] = path_data[required_col1].apply(lambda x: get_path_depth(x))
        path_data["BranchMaxDepth"] = path_data[required_col1].apply(lambda x: get_max_depth_from_path(x))
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
            continue
        print(Delimiter.DASH.repeat(80))
        print(render_cli_object(cli_objects["infos"], "selected", path_count=total_paths_added))
        print(Icon.DOWNARROW.repeat(3))
        
        if total_paths_added == 0:
            return paths_by_depth, MenuActions.FAILED
        
        return paths_by_depth, MenuActions.SUCCESS

def depth_loop(cli_grouped_objects, cli_objects, depth_options): # 3rd level

    while True:
        # Request user input
        print(render_cli_grouped_object(cli_grouped_objects["depth_menu"], cli_objects))
        depth_input, action = prompt_user(render_cli_object(cli_objects["prompts"], "depth", depth_range=depth_options))
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
                print(render_cli_object(cli_objects["warnings"], "invalid_input"))
                continue
        except ValueError:
            print(render_cli_object(cli_objects["warnings"], "invalid_input"))

if __name__ == "__main__":
    paths_by_depth = main_loop(cli_grouped_objects, cli_objects)
    # if paths_by_depth:
    #     print(paths_by_depth)