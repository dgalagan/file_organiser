from enum import StrEnum, auto
import os
os.environ["DISABLE_PANDERA_IMPORT_WARNING"] = "True"
import pandera as pa
from pandera.typing import Series
import pandas as pd
from pandas.errors import ParserError, EmptyDataError
from string import Formatter
import time
from tqdm import tqdm
from typing import Optional, Iterable, Iterator, List, Tuple

# To improve:
# instead of os.walk(), create recursion based on os.scandir()
# self-reporting improvement
# review paths extraction after open_csv()

# Custom Errors
class EmptyDataError(Exception):
    pass
# Schema
class DirPathSchema(pa.DataFrameModel):
    DirPath: Series[str] = pa.Field(coerce=True)
    isInvalid: Series[bool] = pa.Field(default=False)
    isDuplicate: Series[bool] = pa.Field(default=False)
    DirDepth: Series[int] = pa.Field(default=-1)
    BranchDepth: Series[int] = pa.Field(default=-1)
    BranchDepthFromDir: Series[int] = pa.Field(default=-1)
    # cccc: Series[int] = pa.Field(default=-1)
    
    class Config:
        strict = True
        coerce = True

    @classmethod
    def cols(cls):
        return cls.to_schema().columns.keys()
    @classmethod
    def col_dtype(cls, col_name):
        return cls.to_schema().columns[col_name].dtype.type
    @pa.dataframe_parser
    def sort_by_depth(cls, df: pd.DataFrame) -> pd.DataFrame:
        return df.sort_values(cls.DirDepth, ascending=True)
class DirPathData:
    def __init__(self, schema: DirPathSchema):
        # Load schema
        self.schema = schema
        self.schema_cols = schema.cols()
        self._calc_map = {
            self.schema.isInvalid: self._mark_invalid,
            self.schema.isDuplicate: self._mark_duplicates,
            self.schema.DirDepth: self._add_dir_depth,
            self.schema.BranchDepth: self._add_branch_depth,
            self.schema.BranchDepthFromDir: self._add_branch_depth_from_dir
        }
        self._cols_status = {col: False for col in self.schema_cols}
        # Init empty dataframe
        self.dir_df: pd.DataFrame = pd.DataFrame(columns=self.schema_cols)
        # Active mask
        self._active_mask: Optional[pd.Series] = None
        # Log report storage
        self._trace= []
    
    @property
    def df(self): # could not be named df only if i rename attr to _df
        return self.schema.validate(self.dir_df)
    @property
    def active_df(self):
        return self.schema.validate(self.dir_df.loc[self._active_mask])
    @property
    def max_branch_depth(self):
        if not self._active_mask.any():
            raise EmptyDataError
        return int(self.dir_df.loc[self._active_mask, self.schema.BranchDepth].max())

    def load(self, dir_paths: list):
        if not dir_paths:
            raise EmptyDataError("No data to process")
        if not isinstance(dir_paths, list):
            raise TypeError("paths should be a list")
        # Load paths 
        self.dir_df[self.schema.DirPath] = dir_paths
        self._sanitize_paths()
        # Log trace
        self._cols_status[self.schema.DirPath] = True
        # Add log
        return self

    def filter_by(self, rules: List[Tuple[str, bool]] = None):
        if not self._cols_status[self.schema.DirPath]:
            raise RuntimeError("You must load dir paths first")
        mask = pd.Series(True, index=self.dir_df.index)
        if rules:
            for col, value_to_exlude in rules:
                if col not in self.schema_cols:
                    print(f"Warning: Column '{col}' is not in Schema")
                    continue
                if not self.schema.col_dtype(col) == bool:
                    print(f"Warning: Column '{col}' is not boolean")
                    continue
                calc_method = self._calc_map.get(col)
                if calc_method:
                    calc_method()
                mask &= (self.dir_df[col] != value_to_exlude)
        self._active_mask = mask
        return self

    def enrich(self, features: List[str]):
        if not self._cols_status[self.schema.DirPath]:
            raise RuntimeError("You must load dir paths first")
        if not self._cols_status[self.schema.isInvalid]:
            raise RuntimeError("You must filter invalid entries")
        if not self._active_mask.any():
            raise EmptyDataError
        for col in features:
            calc_method = self._calc_map.get(col)
            if calc_method:
                calc_method()
        return self
    
    def _sanitize_paths(self):
        self.dir_df[self.schema.DirPath] = self.dir_df[self.schema.DirPath].apply(get_normalized_path)
        return self
    
    def _mark_invalid(self):
        if self._cols_status.get(self.schema.isInvalid):
            return self
        self.dir_df[self.schema.isInvalid] = self.dir_df[self.schema.DirPath].apply(is_not_dir)
        num_invalids = self.dir_df[self.schema.isInvalid].sum()
        self._log("mark_invalids", {"count": int(num_invalids)})
        self._cols_status[self.schema.isInvalid] = True
        return self

    def _mark_duplicates(self):
        if self._cols_status.get(self.schema.isDuplicate):
            return self
        self.dir_df[self.schema.isDuplicate] = self.dir_df.duplicated(subset=self.schema.DirPath)
        self._cols_status[self.schema.isDuplicate] = True
        num_duplicates = self.dir_df[self.schema.isDuplicate].sum()
        self._log("mark_duplicates", {"count": int(num_duplicates)})
        return self

    def _add_dir_depth(self):
        if self._cols_status.get(self.schema.DirDepth):
            return self
        self.dir_df.loc[self._active_mask, self.schema.DirDepth] = self.dir_df.loc[self._active_mask, self.schema.DirPath].apply(get_dir_depth)
        self._cols_status[self.schema.DirDepth] = True
        self._log("add_path_depth")
        return self

    def _add_branch_depth(self):
        if self._cols_status.get(self.schema.BranchDepth):
            return self
        self.dir_df.loc[self._active_mask, self.schema.BranchDepth] = self.dir_df.loc[self._active_mask, self.schema.DirPath].apply(get_branch_depth_from_root)
        self._cols_status[self.schema.BranchDepth] = True
        self._log("add_branch_depth")
        return self

    def _add_branch_depth_from_dir(self):
        if self._cols_status.get(self.schema.BranchDepthFromDir):
            return self
        self.dir_df.loc[self._active_mask, self.schema.BranchDepthFromDir] = self.dir_df.loc[self._active_mask, self.schema.BranchDepth] - self.dir_df.loc[self._active_mask, self.schema.DirDepth]
        self._cols_status[self.schema.BranchDepthFromDir] = True
        self._log("add_branch_depth_from_path")
        return self

    def get_report(self):
        for line in self._trace:
            print(Delimiter.DASH.repeat(80))
            print(f"{line["action"]} {line["details"]} accomplished at {line["timestamp"]}")
        return self

    def _log(self, action, details=None):
        self._trace.append({
            "action": action,
            "details": details or {},
            "rows_after": self.dir_df.shape[0],
            "timestamp": time.time()
        })

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
    SEP_MSG_SEP = "{start}{sep}{msg:^{width}}{sep}"
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
            "main": {"sep": Delimiter.DASH.repeat(30), "msg": "Main", "width": 15},
            "csv_load": {"sep": Delimiter.DASH.repeat(30), "msg": "CSV load", "width": 15},
            "manual_load": {"sep": Delimiter.DASH.repeat(30), "msg": "Manual load", "width": 15},
            "depth": {"sep": Delimiter.DASH.repeat(30), "msg": "Depth", "width": 15}
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
            "skip": {"msg": "Type 'skip' to skip current dir path"},
            "skip_all": {"msg": "Type 'skipall' to skip the rest of dir path(s)"},
            "csv_load": {"msg": "Type 'csv' to load dir path(s) from CSV"},
            "manual_load": {"msg": "Type 'manual' to provide dir path(s) directly in CLI"},
            "manual_stop": {"msg": "Type 'stop' to finish adding dir path(s)"},
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
            "manual": {"msg": "Enter dir path: "},
            "manual_additional": {"msg": "Add another one: "},
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
            "empty_input": {"msg": "No dir path(s) to process"}, # General
            "csv_load_failed": {"msg": "CSV loading failed with the reason - {error}"}, # CSV
            "dir_paths_processing_failed": {"msg": "Dir paths processing failed with the reason - {error}"}, # Path data
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
            "processing": {"icon": Emoji.HOURGLASS, "sep": Delimiter.SPACE, "msg": "[Processing] -----> {dir_path}"},
            "added": {"msg": "[Added] -----> {dir_paths_count} dirs && {file_paths_count} files"},
            "skipped": {"msg": "[Skipped] -----> as already in scope"},
            "selected":{"icon": Emoji.BULLSEYE.repeat(1), "msg": "[Selected] -----> {dir_paths_count} dirs && {file_paths_count} files"}
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

## String helpers ##

def lower_text(text: str) -> str:
    return text.lower()

def strip_text(text: str, char_to_remove: Optional[str] = None) -> str:
    return text.strip(char_to_remove) 

def lstrip_text(text: str, char_to_remove: Optional[str] = None) -> str: # not used
    return text.lstrip(char_to_remove) 

def rstrip_text(text: str, char_to_remove: Optional[str] = None) -> str: # not used  
    return text.rstrip(char_to_remove) 

def split_text(text: str, separator: Optional[str] = None) -> str:
    if separator is None:
        return text
    return text.split(separator)

def count_char(text: str, char:str) -> int: # not used
    return text.count(char)

def count_letters(text: str) -> int:
    is_letters = [char.isalpha() for char in text]
    return (sum(is_letters))

def find_char(text:str, char:str) -> int:
    return text.find(char)

def get_placeholders(text: str) -> set:
    placeholders = set()
    for _, field_name, format_spec, _ in Formatter().parse(text):
        if field_name:
            placeholders.add(field_name)
            if format_spec:
                placeholders.update(get_placeholders(format_spec))
    return placeholders

## Path helpers ##

# General
def get_abs_path(path: str) -> str:
    return os.path.abspath(path)

def get_common_path(paths: Iterable[str]) -> str:
    return os.path.commonpath(paths)

def get_normalized_path(path: str, path_separator: str = os.sep) -> str:
    normalized_path = strip_text(path, char_to_remove=path_separator)
    letters_count = count_letters(normalized_path)
    chars_count = len(normalized_path)
    if letters_count == 1 and chars_count == 2:
        return normalized_path + path_separator
    elif letters_count == 1 and chars_count == 1:
        return normalized_path + ":" + path_separator 
    return normalized_path

def get_path_length(path: str, path_separator: str = os.sep) -> int:
    path_elements = split_text(path, path_separator)
    path_length = len(path_elements)
    return path_length

# File specific
def is_file(path: str) -> bool:
    return os.path.isfile(path)

def is_not_file(path: str) -> bool:
    return not os.path.isfile(path)

def get_file_extension(path: str, ext_separator: str = '.') -> str:
    if is_not_file(path):
        raise FileNotFoundError(f"No such file: {path}")
    if find_char(path, ext_separator) > 0:
        return os.path.splitext(path)[1]
    else:
        return os.path.splitext(path)[0]

def get_file_basename(path: str, ext_separator: str = '.') -> str: # not used
    if is_not_file(path):
        raise FileNotFoundError(f"No such file: {path}")
    if find_char(path, ext_separator) > 0:
        return os.path.splitext(path)[0]
    else:
        return ""

# Dirs specific
def is_dir(path:str) -> bool:
    return os.path.isdir(path)

def is_not_dir(path:str) -> bool:
    return not os.path.isdir(path)

def is_parent(path: str, of_path: str) -> bool: # not used
    if is_not_dir(path) or is_not_file(of_path):
        raise NotADirectoryError(f"Provided path '{path}' is not a dir")
    abs_path = get_abs_path(path)
    abs_of_path = get_abs_path(of_path)
    common_path = get_common_path([abs_path, abs_of_path])
    return abs_path == common_path and abs_path != abs_of_path

def get_root_dir(path: str) -> str: # not used
    if is_not_dir(path):
        raise NotADirectoryError(f"Provided path '{path}' is not a dir")
    abs_path = get_abs_path(path)
    drive_root, _ = os.path.splitdrive(abs_path)
    return drive_root

def get_dir_depth(path: str) -> int: # depth starting index 0 vs 1 ?
    if is_not_dir(path):
        raise NotADirectoryError(f"Provided path '{path}' is not a dir")
    abs_path = get_abs_path(path)
    normalize_path = strip_text(abs_path, char_to_remove=os.sep)
    return get_path_length(normalize_path) - 1

def get_branch_depth_from_root(path: str) -> tuple[int, int]:
    if is_not_dir(path):
        raise NotADirectoryError(f"Provided path '{path}' is not a dir")
    return max(
        get_dir_depth(root)
        for root, _ , _ in tqdm(os.walk(path), unit=" dir", desc="Computing branch depth")
    )

def iter_dir_hierarchy(path: str, max_depth_from_dir: int) -> Iterator[tuple[int, str]]:
    if is_not_dir(path):
        raise NotADirectoryError(f"Provided path '{path}' is not a dir")
    max_depth_from_root = max_depth_from_dir + get_dir_depth(path)
    for root, dirs, files in tqdm(os.walk(path), unit=" dir", desc="Scanning dir hierarchy"):
        current_depth = get_dir_depth(root)
        if current_depth >= max_depth_from_root:
            dirs[:] = []
        yield current_depth, root, files

## DF helpers ##

def open_csv(path: str) -> pd.DataFrame: # hardcoded "file extension"
    if is_file(path):
        file_extension = lower_text(get_file_extension(path))
        if not file_extension == ".csv":
            raise ValueError(f"File extension '{file_extension}' is not supported")
    return pd.read_csv(path)

## CLI helpers ##

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
    # Fill in the template placeholders
    if "msg" in merged_config:
        text_args = get_placeholders(merged_config["msg"])
        if text_args:
            missing = text_args - runtime_args.keys()
            assert not missing, f"Missing arguments for placeholders: {missing}"
            merged_config["msg"] = merged_config["msg"].format(**runtime_args)
    missing_configs = template_args - merged_config.keys()
    assert not missing_configs, f"Missing config arguments keys: {missing_configs}" 
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
        files_by_dist_from_root, in_action = input_loop(cli_grouped_objects, cli_objects, *args)
        # Loop control parameters check
        match in_action:
            case MenuActions.INTERUPT:
                continue
            case MenuActions.SUCCESS:
                print(render_cli_object(cli_objects["info"], "output_ready"))
                return files_by_dist_from_root

def input_loop(cli_grouped_objects: dict, cli_objects: dict, menu_name, prompt_name): # 2nd level
    while True:
        # Render menu
        print(render_cli_grouped_object(cli_grouped_objects[menu_name], cli_objects))
        input_dir_paths = []
        # Open CSV
        if menu_name == "csv_menu":
            # Request user input
            try:
                user_input = input(render_cli_object(cli_objects["prompt"], prompt_name))
                user_input = strip_text(user_input)
            except KeyboardInterrupt:
                print()
                return None, MenuActions.INTERUPT
            # Open CSV
            try:
                raw_data = open_csv(user_input)
                if DirPathSchema.DirPath in raw_data:
                    input_dir_paths = raw_data[DirPathSchema.DirPath].to_list()
            except (FileNotFoundError, ValueError, PermissionError, ParserError, EmptyDataError) as e:
                print(render_cli_object(cli_objects["warning"], "csv_load_failed", error=e))
                continue
        elif menu_name == "manual_menu":
            # Request user input
            try:
                while True:
                    if len(input_dir_paths) == 0:
                        user_input = input(render_cli_object(cli_objects["prompt"], prompt_name))
                    else:
                        user_input = input(render_cli_object(cli_objects["prompt"], "manual_additional"))
                    user_input = strip_text(user_input)
                    if user_input == "stop":
                        break
                    input_dir_paths.append(user_input)
            except KeyboardInterrupt:
                print()
                return None, MenuActions.INTERUPT
        # Load, normalize and enrich path data
        try:
            processor = (
                DirPathData(DirPathSchema)
                .load(input_dir_paths)
                .filter_by(rules=[("isInvalid", True), ("isDuplicate", True)])
                .enrich(features=["DirDepth", "BranchDepth", "BranchDepthFromDir"])
            )
        except (EmptyDataError, RuntimeError) as e:
            print(render_cli_object(cli_objects["warning"], "dir_paths_processing_failed", error=e))
            continue
        # Get paths by dist from root
        dir_data = processor.active_df
        max_branch_depth = processor.max_branch_depth
        dirs_by_depth = {depth: [] for depth in range(0, max_branch_depth + 1)}
        dir_paths = []
        file_paths = []
        # Resolve parent-child relationship
        reload = False
        total_dirs_added = 0
        total_files_added = 0
        for _, row in dir_data.iterrows():
            dir_path = row[DirPathSchema.DirPath]
            dir_depth = row[DirPathSchema.DirDepth]
            branch_depth_from_dir = row[DirPathSchema.BranchDepthFromDir]
            print(Delimiter.DASH.repeat(80))
            print(render_cli_object(cli_objects["info"], "processing", dir_path=dir_path))
            print(Icon.DOWNARROW.repeat(3))
            if dir_path not in dirs_by_depth[dir_depth]:
                depth_input, in_action = depth_loop(cli_grouped_objects, cli_objects, branch_depth_from_dir)
                match in_action:
                    case MenuActions.SKIP:
                        continue
                    case MenuActions.SKIP_ALL:
                        break
                    case MenuActions.INTERUPT:
                        reload = True
                        break
                    case MenuActions.SUCCESS:
                        dirs_added = 0
                        files_added = 0
                        for depth, dir_path, files in iter_dir_hierarchy(dir_path, depth_input):
                            dirs_by_depth[depth].append(dir_path)
                            dir_paths.append(dir_path)
                            current_files = [os.path.join(dir_path, filename) for filename in files]
                            file_paths.extend(current_files)
                            dirs_added += 1
                            files_added += len(files)
                        total_dirs_added += dirs_added
                        total_files_added += files_added
                        print(Icon.DOWNARROW.repeat(3))
                        print(render_cli_object(cli_objects["info"], "added", dir_paths_count=dirs_added, file_paths_count=files_added))
                        continue
            else:
                print(render_cli_object(cli_objects["info"], "skipped"))
                # hierarchy resolution, ask whether child should be processed separately, delete all related path from parent search, add new 
        if reload:
            return None, MenuActions.RESTART
        print(Delimiter.DASH.repeat(80))
        print(render_cli_object(cli_objects["info"], "selected", dir_paths_count=total_dirs_added, file_paths_count=total_files_added))
        print(Icon.DOWNARROW.repeat(3))
        
        if total_files_added == 0:
            return None, MenuActions.FAILED
        
        return file_paths, MenuActions.SUCCESS

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
            print()
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
    file_paths = main_loop(cli_grouped_objects, cli_objects, input_args)
    files_df = pd.DataFrame(file_paths)
    print(files_df.head())
