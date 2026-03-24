
from enum import StrEnum, auto
import os
os.environ["DISABLE_PANDERA_IMPORT_WARNING"] = "True"
import pandera as pa
from pandera.typing import Series
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
class PathDataSchema(pa.DataFrameModel):
    FolderPath: Series[str] = pa.Field(coerce=True)
    isInvalid: Series[bool] = pa.Field(default=False)
    isDuplicate: Series[bool] = pa.Field(default=False)
    PathDepth: Series[int] = pa.Field(default=-1)
    BranchDepth: Series[int] = pa.Field(default=-1)
    BranchDepthFromPath: Series[int] = pa.Field(default=-1)
    
    class Config:
        strict = True
        coerce = True

    @classmethod
    def columns(cls):
        return cls.to_schema().columns.keys()
    
    @pa.dataframe_parser
    def sort_by_depth(cls, df: pd.DataFrame) -> pd.DataFrame:
        return df.sort_values(cls.PathDepth, ascending=True)
class PathData:
    def __init__(self, schema: PathDataSchema):
        # Load schema
        self.schema = schema
        # Init empty dataframe
        self.path_df = pd.DataFrame(columns=self.schema.columns())
        # Init policy
        self.exclude_policy = {}
        # Log report storage
        self._trace = []
        # Step tracker
        self._steps_completed = set()
    
    @property
    def data(self): # could not be named df only if i rename attr to _df
        return self.schema.validate(self.path_df)
    @property
    def valid_data(self):
        return self.schema.validate(self.path_df.loc[self._active_mask])
    @property
    def _active_mask(self):
        '''Dynamically builds a mask'''
        mask = pd.Series(True, index=self.path_df.index)
        for col_name, status in self.exclude_policy.items():
            if status:
                mask &= (~self.path_df[col_name])
        return mask

    def _log(self, action, details=None):
        self._trace.append({
            "action": action,
            "details": details or {},
            "rows_after": self.path_df.shape[0],
            "timestamp": time.time()
        })

    def load(self, paths: list):
        if not paths:
            raise EmptyDataError("No data to process")
        if not isinstance(paths, list):
            raise TypeError("paths should be a list")
        # Load paths 
        self.path_df[self.schema.FolderPath] = paths
        # Log trace
        self._steps_completed.add("LOAD")
        # TBA
        return self
    
    def normalize(self, exclude_invalids=True, exclude_duplicates=True):
        if "LOAD" not in self._steps_completed:
            raise RuntimeError("You must call .load() before .normalize()")
        # Mark invalid folders
        (
            self
            ._mark_invalid()
            ._mark_duplicates()
        )
        
        # Envoke exclude policy
        self.exclude_policy[self.schema.isInvalid] = exclude_invalids
        self.exclude_policy[self.schema.isDuplicate] = exclude_duplicates
        # Write a step
        self._steps_completed.add("NORMALIZED")
        return self

    def add_hierarchy_depth_data(self):
        if "NORMALIZED" not in self._steps_completed:
            raise RuntimeError("You must call .normalize() before to add_hierarchy_depth_data()")
        if not self._active_mask.any():
            raise EmptyDataError
        (
            self
            ._add_path_depth()
            ._add_branch_depth()
            ._add_branch_depth_from_path()
        )
        self._steps_completed.add("DEPTH_ADDED")

        return self
    
    def _mark_invalid(self):
        self.path_df[self.schema.isInvalid] = self.path_df[self.schema.FolderPath].apply(is_not_folder)
        num_invalids = self.path_df[self.schema.isInvalid].sum()
        self._log("mark_invalids", {"count": int(num_invalids)})
        return self

    def _mark_duplicates(self):
        self.path_df[self.schema.isDuplicate] = self.path_df.duplicated(subset=self.schema.FolderPath)
        num_duplicates = self.path_df[self.schema.isDuplicate].sum()
        self._log("mark_duplicates", {"count": int(num_duplicates)})
        return self

    def _add_path_depth(self):
        self.path_df.loc[self._active_mask, self.schema.PathDepth] = self.path_df.loc[self._active_mask, self.schema.FolderPath].apply(get_path_depth)
        self._log("add_path_depth")
        return self

    def _add_branch_depth(self):
        self.path_df.loc[self._active_mask, self.schema.BranchDepth] = self.path_df.loc[self._active_mask, self.schema.FolderPath].apply(get_branch_depth_from_root)
        self._log("add_branch_depth")
        return self

    def _add_branch_depth_from_path(self):
        self.path_df.loc[self._active_mask, self.schema.BranchDepthFromPath] = self.path_df.loc[self._active_mask, self.schema.BranchDepth] - self.path_df.loc[self._active_mask, self.schema.PathDepth]
        self._log("add_branch_depth_from_path")
        return self

    def get_report(self):
        for line in self._trace:
            print(Delimiter.DASH.repeat(80))
            print(f"{line["action"]} {line["details"]} accomplished at {line["timestamp"]}")
        return self
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
            "manual_additional": {"msg": "Enter another folder path: "},
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
            "path_data_processing_failed": {"msg": "Path data processing failed with the reason - {error}"}, # Path data
            "ext_not_supported": {"msg": "File extension '{ext}' is not supported"}, # File / Extension
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
    for root_path, folders , files in os.walk(path):
        current_depth = get_path_depth(root_path)
        if current_depth >= max_depth:
            folders[:] = []
        yield current_depth, root_path, files

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
        if not args:
            print(render_cli_object(cli_objects["warning"], "invalid_input"))
            continue
        paths_by_dist_from_root, in_action = input_loop(cli_grouped_objects, cli_objects, *args)
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
        folder_paths = []
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
                if PathDataSchema.FolderPath in raw_data:
                    folder_paths = raw_data[PathDataSchema.FolderPath].to_list()
            except (FileNotFoundError, ValueError, PermissionError, ParserError, EmptyDataError) as e:
                print(render_cli_object(cli_objects["warning"], "csv_load_failed", error=e))
                continue
        elif menu_name == "manual_menu":
            # Request user input
            try:
                while True:
                    if len(folder_paths) == 0:
                        user_input = input(render_cli_object(cli_objects["prompt"], prompt_name))
                    else:
                        user_input = input(render_cli_object(cli_objects["prompt"], "manual_additional"))
                    user_input = strip_text(user_input)
                    if user_input == "stop":
                        break
                    folder_paths.append(user_input)
            except KeyboardInterrupt:
                return None, MenuActions.INTERUPT
        # Load, normalize and enrich path data
        try:
            processor = (
                PathData(PathDataSchema)
                .load(folder_paths)
                .normalize()
                .add_hierarchy_depth_data()
            )
        except (EmptyDataError, RuntimeError) as e:
            print(render_cli_object(cli_objects["warning"], "path_data_processing_failed", error=e))
            continue
        # Get paths by dist from root
        path_data = processor.valid_data
        print(path_data)
        max_depth = path_data[PathDataSchema.BranchDepth].max()
        folders_by_dist_from_root = {depth: [] for depth in range(0, max_depth + 1)}
        files_by_dist_from_root = {depth: [] for depth in range(0, max_depth + 1)}
        # Resolve parent-child relationship
        reload = False
        total_folders_added = 0
        total_files_added = 0
        for _, row in path_data.iterrows():
            # folder_path = strip_text(row[PathDataSchema.FolderPath], char_to_remove=os.sep)
            folder_path = row[PathDataSchema.FolderPath]
            path_depth = row[PathDataSchema.PathDepth]
            branch_depth_from_path = row[PathDataSchema.BranchDepthFromPath]
            print(Delimiter.DASH.repeat(80))
            print(render_cli_object(cli_objects["info"], "processing", path=folder_path))
            print(Icon.DOWNARROW.repeat(3))
            if folder_path not in folders_by_dist_from_root[path_depth]:
                depth_input, in_action = depth_loop(cli_grouped_objects, cli_objects, branch_depth_from_path)
                match in_action:
                    case MenuActions.SKIP:
                        continue
                    case MenuActions.SKIP_ALL:
                        break
                    case MenuActions.INTERUPT:
                        reload = True
                        break
                    case MenuActions.SUCCESS:
                        folders_added = 0
                        files_added = 0
                        for depth, folder_path, files in iter_hierarchy_until(folder_path, depth_input):
                            folders_by_dist_from_root[depth].append(folder_path)
                            files_by_dist_from_root[depth].extend(files)
                            folders_added += 1
                            files_added += len(files)
                        total_folders_added += folders_added
                        total_files_added += files_added
                        print(Icon.DOWNARROW.repeat(3))
                        print(render_cli_object(cli_objects["info"], "added", path_count=folders_added))
                        print(render_cli_object(cli_objects["info"], "added", path_count=files_added))
                        continue
            else:
                print(render_cli_object(cli_objects["info"], "skipped"))
                # hierarchy resolution, ask whether child should be processed separately, delete all related path from parent search, add new 
        if reload:
            return None, MenuActions.RESTART
        print(Delimiter.DASH.repeat(80))
        print(render_cli_object(cli_objects["info"], "selected", path_count=total_folders_added))
        print(render_cli_object(cli_objects["info"], "selected", path_count=total_files_added))
        print(Icon.DOWNARROW.repeat(3))
        
        if total_folders_added == 0:
            return None, MenuActions.FAILED
        
        return files_by_dist_from_root, MenuActions.SUCCESS

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
                if 0 <= user_input <= branch_depth:
                    return user_input, MenuActions.SUCCESS
                else:
                    print(render_cli_object(cli_objects["warning"], "invalid_input"))
                    continue
            except ValueError:
                print(render_cli_object(cli_objects["warning"], "invalid_input"))

if __name__ == "__main__":
    paths_by_dist_from_root = main_loop(cli_grouped_objects, cli_objects, input_args)
    result = []
    for files in paths_by_dist_from_root.values():
        result.extend(files)
    files_df = pd.DataFrame({"Files": result})
    files_df.to_excel("files.xlsx")
    # if paths_by_dist_from_root:
    #     print(paths_by_dist_from_root)