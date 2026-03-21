from enum import StrEnum, auto
import inspect
import os
import pandas as pd
from pandas.errors import ParserError, EmptyDataError
from string import Formatter
import time
from typing import Optional, Iterable, Iterator

# To improve:
# instead of os.walk(), create recursion based on os.scandir()
# auto alignment of header object between separators
# convert paths by depth ito 1 list

# Custom Errors
class EmptyDataError(Exception):
    pass
class MissingColumnError(Exception):
    pass
# Schema
class PathSchema:
    PATH = "FolderPath"
    IS_INVALID = "isInvalid"
    IS_DUPLICATE = "isDuplicate"
    PATH_DEPTH_FROM_ROOT = "PathDepthFromRoot"
    BRANCH_DEPTH_FROM_ROOT = "BranchDepthFromRoot"
    BRANCH_DEPTH_FROM_PATH = "BranchDepthFromPath"
    REQUIRED = [PATH]
class PathData:
    def __init__(self, path_data: pd.DataFrame, required_cols: list):
        # Type check
        if not isinstance(path_data, pd.DataFrame):
            raise TypeError("path_data must be a pandas DataFrame")
        # Content check
        if self.is_empty(path_data):
            raise EmptyDataError("path_data is empty")
        # Column check
        if not self.has_columns(path_data, required_cols):
            missing = self.missing_columns(path_data, required_cols)
            raise MissingColumnError(f"Required columns {missing} are missing")
        # Attributes
        self.path_data = path_data.copy() # mutates as per applied methods
        self._trace = []
        # Normalize data and store trace log
        self._mark_invalid()
        self._filter_by_column(PathSchema.IS_INVALID, filter_by_flag=False)
        self._mark_duplicates()
        self._filter_by_column(PathSchema.IS_DUPLICATE, filter_by_flag=False)

    @property
    def data(self): # could not be named df only if i rename attr to _df
        return self.path_data
    @property
    def max_depth(self):
        if not self.has_columns(self.path_data, PathSchema.BRANCH_DEPTH_FROM_ROOT):
            raise MissingColumnError(f"{PathSchema.BRANCH_DEPTH_FROM_ROOT} depth has not been calculated")
        return self.path_data[PathSchema.BRANCH_DEPTH_FROM_ROOT].max()

    def _log(self, action, details=None):
        self._trace.append({
            "action": action,
            "details": details or {},
            "rows_after": self.path_data.shape[0],
            "timestamp": time.time()
        })
        
    def _mark_invalid(self):
        self.data[PathSchema.IS_INVALID] = self.data[PathSchema.PATH].apply(is_not_folder)
        num_invalids = self.data[PathSchema.IS_INVALID].sum()
        self._log("mark_invalids", {"count": int(num_invalids)})
        return self

    def _mark_duplicates(self):
        self.path_data[PathSchema.IS_DUPLICATE] = self.path_data.duplicated(subset=PathSchema.PATH)
        num_duplicates = self.path_data[PathSchema.IS_DUPLICATE].sum()
        self._log("mark_duplicates", {"count": int(num_duplicates)})
        return self

    def _filter_by_column(self, col_name, filter_by_flag=True):
        if self.is_empty(self.path_data):
            raise EmptyDataError
        if not self.has_columns(self.path_data, col_name):
            missing = self.missing_columns(self.path_data, col_name)
            raise MissingColumnError(f"Required columns {missing} are missing")
        rows_before = self.path_data.shape[0]
        condition = self.path_data[col_name] if filter_by_flag else ~self.path_data[col_name]
        self.path_data = self.path_data.loc[condition]
        removed = rows_before - self.path_data.shape[0]
        log_action = func_name() + " " + col_name
        self._log(log_action, {"filtered": int(removed)})
        return 

    def calculate_hierarchy_depths(self):
        if self.is_empty(self.path_data):
            raise EmptyDataError("Provided data is empty")
        self.path_data[PathSchema.PATH_DEPTH_FROM_ROOT] = self.path_data[PathSchema.PATH].apply(get_path_depth)
        self.path_data[PathSchema.BRANCH_DEPTH_FROM_ROOT] = self.path_data[PathSchema.PATH].apply(get_branch_depth_from_root)
        self.path_data[PathSchema.BRANCH_DEPTH_FROM_PATH] = self.path_data[PathSchema.BRANCH_DEPTH_FROM_ROOT] - self.path_data[PathSchema.PATH_DEPTH_FROM_ROOT]
        self._log("calculate_depths")
        return self

    def sort_by(self, by_col):
        if self.is_empty(self.path_data):
            raise EmptyDataError("Provided data is empty")
        if not self.has_columns(self.path_data, by_col):
            missing = self.missing_columns(self.path_data, by_col)
            raise MissingColumnError(f"Required columns {missing} are missing")
        self.path_data = self.path_data.sort_values(by=by_col)
        return self

    def get_report(self):
        for line in self._trace:
            print(Delimiter.DASH.repeat(80))
            print(f"{line["action"]} accomplished {line["details"]}")
        return self

    @staticmethod
    def has_columns(path_data: pd.DataFrame, cols: list | str) -> bool:
        if isinstance(cols, str):
            cols = [cols]
        return set(cols).issubset(path_data.columns)
    @staticmethod
    def missing_columns(path_data: pd.DataFrame, cols: list | str) -> set:
        if isinstance(cols, str):
            cols = [cols]        
        return set(cols) - set(path_data.columns)
    @staticmethod
    def is_empty(path_data: pd.DataFrame) -> bool:
        return path_data.empty
# Actions
class MenuActions(StrEnum):
    EXIT = auto()
    INTERUPT = auto()
    SKIP = auto()
    SKIP_ALL = auto()
    SUCCESS = auto()
    FAILED = auto()
    RESTART = auto()
# CLI Elements
class Template:
    SEP_MSG_SEP = "{start}{sep}{msg}{sep}" # add space required
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
    "header": {
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
    "menu_line": {
        "template": Template.ICON_SEP_MSG,
        "defaults": {
            "start": "",
            "icon": Emoji.KEYBOARD,
            "sep": Delimiter.SPACE.repeat(2),
            "msg": "empty"
        },
        "elements": {
            "exit": {"icon": Emoji.CROSSMARK, "sep": Delimiter.SPACE, "msg": "Press 'Ctrl+C' to suspend the script"},
            "cancel": {"icon": Emoji.LEFTWARDARROW, "msg": "Press 'Ctrl+C' to cancel"},
            "restart": {"icon": Emoji.RESTART, "sep": Delimiter.SPACE, "msg": "Press 'Ctrl+C' to cancel current input and retry"},
            "skip": {"msg": "Type 'skip' to skip current folder path"},
            "skip_all": {"msg": "Type 'skipall' to skip the rest of folder path(s)"},
            "csv_load": {"msg": "Type 'csv' to load folder path(s) from CSV"},
            "manual_load": {"msg": "Type 'manual' to provide folder path(s) directly in CLI"},
            "manual_stop": {"msg": "Type 'stop' to finish adding paths"},
            "depth": {"msg": "Select 'depth level' from {depth_range}"}
        }
    },
    "prompt": {
        "template": Template.ICON_SEP_MSG,
        "defaults": {
            "start": "",
            "icon": Emoji.RIGHTARROW,
            "sep": Delimiter.SPACE.repeat(2),
            "msg": "Provide your option: "
        },
        "elements": {
            "csv": {"msg": "Enter link to CSV file: "},
            "manual": {"msg": "Enter folder path: "},
        }
    },
    "warning": {
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
            # "file_not_found": {"msg": "Provided path '{path}' is not a file"}, # File / Extension
            "ext_not_supported": {"msg": "File extension '{ext}' is not supported"}, # File / Extension
            # "missing_columns": {"msg": "Required columns {cols} are missing"}, # Columns
        }
    },
    "info": {
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
            "processing": {"icon": Emoji.HOURGLASS, "sep": Delimiter.SPACE, "msg": "[Processing] -----> {path}"},
            "added": {"msg": "[Added] -----> {path_count} folder path(s)"},
            "skipped": {"msg": "[Skipped] -----> as already in scope"},
            "selected":{"icon": Emoji.BULLSEYE.repeat(1), "msg": "[Selected] -----> {path_count} folder path(s)"}
        }
    },
}
cli_grouped_objects = {
    "main_menu": [
        ("header", "main"),
        ("menu_line", "exit"),
        ("menu_line", "csv_load"),
        ("menu_line", "manual_load")
    ],
    "csv_menu": [
        ("header", "csv_load"),
        ("menu_line", "cancel")
    ],
    "manual_menu": [
        ("header", "manual_load"),
        ("menu_line", "cancel"),
        ("menu_line", "manual_stop")
    ],
    "depth_menu": [
        ("menu_line", "cancel"),
        ("menu_line", "skip_all"),
        ("menu_line", "skip"),
        ("menu_line", "depth")
    ]
}
input_args = {
    "csv": ("csv_menu", "csv"),
    "manual": ("manual_menu", "manual")
}

## Inspect helpers

def func_name():
    return inspect.stack()[1].function

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

def count_char(text:str, char:str) -> int:
    return text.count(char)

def find_char(text:str, char:str) -> int:
    return text.find(char)

def get_placeholders(text: str) -> set:
    return {
        placehoder
        for _, placehoder, _, _, in Formatter().parse(text)
        if placehoder
    }

## Path helpers

def is_file(path: str) -> bool:
    return os.path.isfile(path)

def is_not_file(path: str) -> bool:
    return not os.path.isfile(path)

def is_folder(path:str) -> bool:
    return os.path.isdir(path)

def is_not_folder(path:str) -> bool:
    return not os.path.isdir(path)

def get_file_extension(path: str) -> str:
    if is_not_file(path):
        raise FileNotFoundError(f"No such file: {path}")
    if find_char(path, ".") > 0:
        return os.path.splitext(path)[1]
    else:
        return os.path.splitext(path)[0]

def get_file_basename(path: str) -> str:
    if is_not_file(path):
        raise FileNotFoundError(f"No such file: {path}")
    if find_char(path, ".") > 0:
        return os.path.splitext(path)[0]
    else:
        return ""

def get_abs_path(path: str) -> str:
    return os.path.abspath(path)

def get_common_path(paths: Iterable[str]) -> str:
    return os.path.commonpath(paths)

def is_parent(path: str, of_path: str) -> bool:
    if is_not_folder(path) or is_not_file(of_path):
        raise NotADirectoryError(f"Provided path '{path}' is not a folder")
    abs_path = get_abs_path(path)
    abs_of_path = get_abs_path(of_path)
    common_path = get_common_path([abs_path, abs_of_path])
    return abs_path == common_path and abs_path != abs_of_path

def get_drive_root(path: str) -> str:
    if is_not_folder(path):
        raise NotADirectoryError(f"Provided path '{path}' is not a folder")
    abs_path = get_abs_path(path)
    drive_root, _ = os.path.splitdrive(abs_path)
    return drive_root

def get_path_length(path: str, path_separator: str = os.sep) -> int:
    path_elements = split_text(path, path_separator)
    path_length = len(path_elements)
    return path_length

def get_path_depth(path: str) -> int: # depth starting index 0 vs 1 ?
    if is_not_folder(path):
        raise NotADirectoryError(f"Provided path '{path}' is not a folder")
    abs_path = get_abs_path(path)
    normalize_path = strip_text(abs_path, char_to_remove=os.sep)
    return get_path_length(normalize_path) - 1

def get_branch_depth_from_root(path: str) -> tuple[int, int]:
    if is_not_folder(path):
        raise NotADirectoryError(f"Provided path '{path}' is not a folder")
    return max(
        get_path_depth(root_path)
        for root_path, _ , _ in os.walk(path)
    )

def iter_hierarchy_until(path: str, max_depth: int) -> Iterator[tuple[int, str]]:
    if is_not_folder(path):
        raise NotADirectoryError(f"Provided path '{path}' is not a folder")
    for root_path, folders , _ in os.walk(path):
        current_depth = get_path_depth(root_path)
        if current_depth >= max_depth:
            folders[:] = []
        yield current_depth, root_path

## DF helpers

def open_csv(path: str) -> pd.DataFrame: # hardcoded "file extension"
    if is_file(path):
        file_extension = lower_text(get_file_extension(path))
        if not file_extension == ".csv":
            raise ValueError(f"File extension '{file_extension}' is not supported")
    return pd.read_csv(path)

## CLI helpers

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

## Loops

def main_loop(cli_grouped_objects: dict, cli_objects: dict, input_args: dict): # 1st level
    while True:
        # Render menu
        print(render_cli_grouped_object(cli_grouped_objects["main_menu"], cli_objects))
        # Request user input
        try:
            user_input = input(render_cli_object(cli_objects["prompt"]))
            user_input = lower_text(strip_text(user_input))
        except KeyboardInterrupt:
            print()
            print(Icon.DOWNARROW.repeat(3))
            print(render_cli_object(cli_objects["info"], "exit"))
            break
        # User input handling
        args = input_args.get(user_input)
        menu_name, prompt_name = args
        if not args:
            print(render_cli_object(cli_objects["warning"], "invalid_input"))
            continue
        paths_by_dist_from_root, in_action = input_loop(cli_grouped_objects, cli_objects, menu_name, prompt_name)
        # Loop control parameters check
        match in_action:
            case MenuActions.INTERUPT:
                continue
            case MenuActions.SUCCESS:
                print(render_cli_object(cli_objects["info"], "output_ready"))
                return paths_by_dist_from_root

def input_loop(cli_grouped_objects: dict, cli_objects: dict, menu_name, prompt_name): # 2nd level
    while True:
        # Render menu
        print(render_cli_grouped_object(cli_grouped_objects[menu_name], cli_objects))
        # Open CSV
        if menu_name == "csv_menu":
            # Request user input
            try:
                user_input = input(render_cli_object(cli_objects["prompt"], prompt_name))
                user_input = strip_text(user_input)
            except KeyboardInterrupt:
                return None, MenuActions.INTERUPT
            # Open CSV
            try:
                raw_data = open_csv(user_input)
            except (FileNotFoundError, ValueError, PermissionError, ParserError, EmptyDataError) as e:
                print(render_cli_object(cli_objects["warning"], "csv_load_failed", error=e))
                continue
        elif menu_name == "manual_menu":
            # Request user input
            try:
                folder_paths = []
                while True:
                    user_input = input(render_cli_object(cli_objects["prompt"], prompt_name))
                    user_input = strip_text(user_input)
                    folder_paths.append(user_input)
                    if user_input == "stop":
                        break
            except KeyboardInterrupt:
                return None, MenuActions.INTERUPT
            # Create DF
            raw_data = pd.DataFrame({"FolderPath": folder_paths})
        # Normalize and enrich path data
        try:
            processor = (
                PathData(raw_data, PathSchema.REQUIRED)
                .calculate_hierarchy_depths()
                .sort_by(PathSchema.PATH_DEPTH_FROM_ROOT)
                .get_report()
            )
        except EmptyDataError:
            print(render_cli_object(cli_objects["warning"], "empty_input"))
            continue
        # Get paths by dist from root
        path_data = processor.data
        max_depth = processor.max_depth
        paths_by_dist_from_root = {depth: [] for depth in range(0, max_depth + 1)}
        # Resolve parent-child relationship
        reload = False
        total_paths_added = 0
        for _, row in path_data.iterrows():
            folder_path = strip_text(row[PathSchema.PATH], char_to_remove=os.sep)
            path_depth = row[PathSchema.PATH_DEPTH_FROM_ROOT]
            branch_depth = row[PathSchema.BRANCH_DEPTH_FROM_PATH]
            print(Delimiter.DASH.repeat(80))
            print(render_cli_object(cli_objects["info"], "processing", path=folder_path))
            print(Icon.DOWNARROW.repeat(3))
            if folder_path not in paths_by_dist_from_root[path_depth]:
                depth_input, in_action = depth_loop(cli_grouped_objects, cli_objects, branch_depth)
                match in_action:
                    case MenuActions.SKIP:
                        continue
                    case MenuActions.SKIP_ALL:
                        break
                    case MenuActions.INTERUPT:
                        reload = True
                        break
                    case MenuActions.SUCCESS:
                        paths_added = 0
                        for depth, folder_path in iter_hierarchy_until(folder_path, depth_input):
                            paths_by_dist_from_root[depth].append(folder_path)
                            paths_added += 1
                        total_paths_added += paths_added
                        print(Icon.DOWNARROW.repeat(3))
                        print(render_cli_object(cli_objects["info"], "added", path_count=paths_added))
                        continue
            else:
                print(render_cli_object(cli_objects["info"], "skipped"))
                # hierarchy resolution, ask whether child should be processed separately, delete all related path from parent search, add new 
        if reload:
            return None, MenuActions.RESTART
        print(Delimiter.DASH.repeat(80))
        print(render_cli_object(cli_objects["info"], "selected", path_count=total_paths_added))
        print(Icon.DOWNARROW.repeat(3))
        
        if total_paths_added == 0:
            return None, MenuActions.FAILED
        
        return paths_by_dist_from_root, MenuActions.SUCCESS

def depth_loop(cli_grouped_objects: dict, cli_objects: dict, branch_depth): # 3rd level
    while True:
        depth_options = f"0-{branch_depth}" if branch_depth else "0"
        # Render menu
        print(render_cli_grouped_object(cli_grouped_objects["depth_menu"], cli_objects, depth_range=depth_options))
        # Request user input
        try:
            user_input = input(render_cli_object(cli_objects["prompt"]))
            user_input = lower_text(strip_text(user_input))
        except KeyboardInterrupt:
            return None, MenuActions.INTERUPT
        # Process input
        if user_input == "skip":
            return None, MenuActions.SKIP
        elif user_input == "skipall":
            return None, MenuActions.SKIP_ALL
        else:
            try:
                user_input = int(user_input)
                if user_input <= branch_depth:
                    return user_input, MenuActions.SUCCESS
                else:
                    print(render_cli_object(cli_objects["warning"], "invalid_input"))
                    continue
            except ValueError:
                print(render_cli_object(cli_objects["warning"], "invalid_input"))

if __name__ == "__main__":
    paths_by_dist_from_root = main_loop(cli_grouped_objects, cli_objects, input_args)
    # if paths_by_dist_from_root:
    #     print(paths_by_dist_from_root)