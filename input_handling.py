from dataclasses import dataclass, field
from enum import Enum, IntEnum, StrEnum, auto
from itertools import combinations, product
import os
import pandas as pd
from pandas.errors import ParserError
from typing import List, Tuple, Dict, Optional, Any, Iterable, Iterator, Callable

# Glossary
# Depth = maximum processing radius downward

# To improve:
# instead of os.walk(), create recursion based on os.scandir()

## String helpers
def lower_text(text: str) -> str:
    return text.lower()

def strip_text(text: str, char_to_remove: str = None) -> str:
    return text.strip(char_to_remove) 

def lstrip_text(text: str, char_to_remove: str = None) -> str:
    return text.lstrip(char_to_remove) 

def rstrip_text(text: str, char_to_remove: str = None) -> str:
    return text.rstrip(char_to_remove) 

def split_text(text: str, separator: str = None) -> str:
    if separator is None:
        return text
    return text.split(separator)

def transform_text(text: str, *funcs: Callable[[str], str]) -> str:
    for func in funcs:
        text = func(text)
    return text

def parse_text(text: str, separator: Optional[str] = None) -> list[str]:
    if separator is None:
        return text
    return text.split(separator)

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
    path_elements = parse_text(path, path_separator)
    path_length = len(path_elements)
    return path_length

def is_parent(path, of_path):
    assert is_folder(path), f"Provided path is not a folder {path}"
    assert is_folder(of_path), f"Provided path is not a folder {of_path}"
    abs_path = get_abs_path(path)
    abs_of_path = get_abs_path(of_path)
    common_path = get_common_path([abs_path, abs_of_path])
    return abs_path == common_path and abs_path != abs_of_path

def get_path_root(path: str) -> str:
    assert is_folder(path), f"Provided path is not a folder {path}"
    abs_path = get_abs_path(path)
    drive, _ = os.path.splitdrive(abs_path)
    return drive

def get_depth_from_root(path: str) -> int:
    assert is_folder(path), f"Provided path is not a folder {path}"
    abs_path = get_abs_path(path)
    normalize_path = strip_text(abs_path, char_to_remove=os.sep)
    return get_path_length(normalize_path)

def get_max_depth_from_path(path: str) -> int:
    assert is_folder(path), f"Provided path is not a folder {path}"
    base_depth = get_depth_from_root(path)
    return max(
        get_depth_from_root(folder_path)
        for folder_path, _ , _ in os.walk(path)
    )

def get_folders_under(path: str, depth: int, scope_dict) -> dict:
    # counter = 0
    for folder_path, _ , _ in os.walk(path):
        current_depth = get_depth_from_root(folder_path)
        # counter += 1
        if current_depth > depth:
            continue
        scope_dict[current_depth].append(folder_path)
    # print(counter)
    return scope_dict

## Df helpers
def open_csv(path: str) -> pd.DataFrame | None:
    try:
        csv_data = pd.read_csv(path)
        return csv_data
    except FileNotFoundError as e:
        print(f"⚠️  File not found: {e}")
    except PermissionError as e:
        print(f"⚠️  Permission denied: {e}")
    except ParserError as e:
        print(f"⚠️  Parsing error: {e}")
    
    return None

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


class MenuActions(StrEnum):
    EXIT = auto()
    INTERUPT = auto()
    SKIP = auto()
    SKIP_ALL = auto()
    SUCCESS = auto()
    FAILED = auto()

class NotificationLevel(Enum): 
    INFO = "INFO"
    SUCCESS = "SUCCESS"
    FINISH = "FINISH"
    WARNING = "WARNING"
    ERROR = "ERROR"
    EXIT = "EXIT"

# CLI elements
class Menu:
    RETURN = "↩️  Press 'Ctrl+C' to go back"
    RELOAD = "↩️  Press 'Ctrl+C' to cancel current input and retry"
    SKIP = "⌨️  Type 'skip' to skip this folder path"
    SKIP_ALL = "⌨️  Type 'skipall' to skip all folder paths"
    EXIT = "⌨️  Type 'exit' to suspend the script"
    CSV = "⌨️  Type 'csv' to load folder path(s) from CSV"
    MANUAL = "⌨️  Type 'manual' to provide folder path(s) manually"
    CSV_INPUT = "⌨️  Type 'load' to provide link to CSV file"
    MANUAL_INPUT = "⌨️  Type 'enter' to provide one or several folder path(s)"
    DEPTH_RANGE = "⌨️  Type 'depth' value from the range {depth_range}"

    @classmethod
    def main(cls):
        print("\n----Main menu----")
        print(cls.EXIT)
        print(cls.CSV)
        print(cls.MANUAL)

    @classmethod
    def csv(cls):
        print("\n----CSV menu----")
        print(cls.EXIT)
        print(cls.RETURN)
        print(cls.CSV_INPUT)

    @classmethod
    def manual(cls):
        print("\n----Manual menu----")
        print(cls.EXIT)
        print(cls.RETURN)
        print(cls.MANUAL_INPUT)

    @classmethod
    def csv_input(cls):
        print("\n----CSV load menu----")
        print(cls.RETURN)

    @classmethod
    def manual_input(cls):
        print("\n----Manual load menu----")
        print(cls.RETURN)

    @classmethod
    def depth_input(cls, depth_options):
        print("\n----Depth menu----")
        print(cls.RELOAD)
        print(cls.SKIP_ALL)
        print(cls.SKIP)
        print(cls.DEPTH_RANGE.format(depth_range=depth_options))

class Prompt:
    MAIN = "⌨️  Select your option: "
    CSV = "⌨️  Please provide link to CSV file: "
    MANUAL = "⌨️  Please provide one or several folder path(s) separated with {paths_separator}: "

class Notifier:
    ICONS = {
        NotificationLevel.INFO: "ℹ️ ",
        NotificationLevel.SUCCESS: "✅",
        NotificationLevel.FINISH: "➡️ ",
        NotificationLevel.WARNING: "⚠️ ",
        NotificationLevel.EXIT: "🛑",
        NotificationLevel.ERROR: "❌",
    }
    @staticmethod
    def notify(msg: str, level: NotificationLevel = NotificationLevel.INFO):
        icon = Notifier.ICONS.get(level, "")
        print(f"{icon} {msg}")

class Messages:
    
    class Base:
        EXIT = "Script terminated"
        INVALID_INPUT = "Invalid input"
        EMPTY_INPUT = "No folder path(s) to process"
        OUTPUT = "Output ready"

        @staticmethod
        def format(msg: str, **kwargs):
            return msg.format(**kwargs)

    class CsvProcessing(Base):
        # Regular string
        CSV_LOAD_FAILED = "CSV loading failed"
        CSV_LOAD_SUCCEED = "CSV opened"
        FOLDER_PATHS_FILTERED = "Valid folder path(s) filtered"
        # f string with 1 variable
        FILE_NOT_FOUND_f = "Provided path '{path}' is not a file"
        EXTENSION_NOT_SUPPORTED_f = "Provided extension '{ext}' is not supported"
        COLUMNS_MISSING_f = "Required columns {cols} are missing"
        COLUMNS_IDENTIFIED_f = "Required columns {cols} identified"
        DUPLICATES_REMOVED_f = "{count} duplicate path(s) removed"
        # f string with 2 variables
        FOLDER_PATHS_IDENTIFIED_ff = "{count} folder path(s) identified for {key} processing"

    class ManualProcessing(Base):
        # Regular string
        INPUT_RESET = "Input dictionary reset"
        # f string with 1 variable
        PATHS_VALID_f = "Valid folder path(s) identified {paths}"
        DUPLICATED_PATHS_f = "Duplicated folder path(s) identified {paths} and won't be processed"
        CORRUPTED_PATHS_f = "Corrupted folder path(s) identified {paths} and won't be processed"
        # f string with 2 variables
        ADD_FOLDER_PATH_ff = "{path} folder path added to {key}"
        FOLDER_PATHS_IDENTIFIED_ff = "{count} folder path(s) identified for {key} processing"

# CLI application services 
@dataclass
class AppServices:
    menu: Menu
    prompt: Prompt
    messages: Messages
    notifier: Notifier

# CLI helper functions
def prompt_user(
        menu: Callable[[], None],
        prompt_text: str,
        *transform_funcs:Callable[[str], str],
)-> Tuple[str | None, MenuActions | None]:
    try:
        menu
        user_input = input(prompt_text)
        user_input = transform_text(user_input, *transform_funcs)
        return user_input, None
    except KeyboardInterrupt:
        print()
        return None, MenuActions.INTERUPT

# CLI execution logic 
def main_loop(services: AppServices): # 1st level
    
    input_handler = {
        "csv": csv_input_loop,
        "manual": manual_input_loop,
    }
    menu_cls = services.menu
    notifier_cls = services.notifier
    messages_cls = services.messages
    prompt_cls = services.prompt

    while True:
        # Request user input
        user_input, action = prompt_user(
            menu_cls.main(), 
            prompt_cls.MAIN,
            strip_text,
            lower_text
        )
        if action is not None:
            print("\n⚠️  Type 'exit' to terminate the script")
            continue

        #  User input handling
        if user_input == 'exit':
            notifier_cls.notify(messages_cls.Base.EXIT, NotificationLevel.EXIT)
            break

        handler = input_handler.get(user_input)

        if not handler:
            notifier_cls.notify(messages_cls.Base.INVALID_INPUT, NotificationLevel.WARNING)
            continue
        
        loop_func = handler
        folder_scope, in_action = loop_func(services)

        # Loop control parameters check
        match in_action:
            case MenuActions.INTERUPT:
                continue
            case MenuActions.FAILED:
                notifier_cls.notify(messages_cls.Base.EMPTY_INPUT, NotificationLevel.WARNING)
                continue
            case MenuActions.EXIT:
                notifier_cls.notify(messages_cls.Base.EXIT, NotificationLevel.EXIT)
                break
            case MenuActions.SUCCESS:
                notifier_cls.notify(messages_cls.Base.OUTPUT, NotificationLevel.FINISH)
                return folder_scope

def csv_input_loop(services: AppServices): # 2nd level
    
    # Required CSV columns for correct input validation
    required_col1 = "FolderPath"
    test_req1 = "FolderPathTest"
    menu_cls = services.menu
    notifier_cls = services.notifier
    messages_cls = services.messages
    prompt_cls = services.prompt
    
    while True:
        # Request user input
        user_input, action = prompt_user(
            menu_cls.csv_input(), 
            prompt_cls.CSV,
            strip_text
        )
        if action is not None:
            return None, MenuActions.INTERUPT
        # Check whether provided link is file, otherwise continue
        if not is_file(user_input):
            notifier_cls.notify(messages_cls.CsvProcessing.FILE_NOT_FOUND_f.format(path=user_input), NotificationLevel.WARNING)
            continue    
        # Check whether file extension == 'csv', otherwise continue
        file_ext = get_file_extension(user_input)
        file_ext = transform_text(file_ext, strip_text, lower_text)
        if file_ext != '.csv':
            notifier_cls.notify(messages_cls.CsvProcessing.EXTENSION_NOT_SUPPORTED_f.format(ext=file_ext), NotificationLevel.WARNING)
            continue
        # Open CSV file as dataframe
        csv_data = open_csv(user_input)
        if csv_data is None:
            notifier_cls.notify(messages_cls.CsvProcessing.CSV_LOAD_FAILED, NotificationLevel.WARNING)
            continue
        notifier_cls.notify(messages_cls.CsvProcessing.CSV_LOAD_SUCCEED, NotificationLevel.SUCCESS)
        # Validate CSV columns
        if required_col1 not in csv_data.columns:
            notifier_cls.notify(messages_cls.CsvProcessing.COLUMNS_MISSING_f.format(cols=required_col1), NotificationLevel.WARNING)
            continue
        notifier_cls.notify(messages_cls.CsvProcessing.COLUMNS_IDENTIFIED_f.format(cols=required_col1), NotificationLevel.SUCCESS)
        # Validate CSV data
        csv_data[test_req1] = csv_data[required_col1].apply(lambda x: True if is_folder(x) else False)
        # Normalize CSV data
        normalized_csv_data = remove_duplicates(csv_data, column_name=required_col1)
        duplicates_count = csv_data.shape[0] - normalized_csv_data.shape[0]
        if duplicates_count:
            notifier_cls.notify(messages_cls.CsvProcessing.DUPLICATES_REMOVED_f.format(count=duplicates_count), NotificationLevel.SUCCESS)
        # Select valid entries
        condition = normalized_csv_data[test_req1] == True
        filtered_csv_data = filter_df(normalized_csv_data, condition)
        if filtered_csv_data.empty:
            notifier_cls.notify(messages_cls.Base.EMPTY_INPUT, NotificationLevel.WARNING)
            continue
        # Define max depth available
        filtered_csv_data["FolderPathDepth"] = filtered_csv_data[required_col1].apply(lambda x: get_depth_from_root(x))
        filtered_csv_data["BranchMaxDepth"] = filtered_csv_data[required_col1].apply(lambda x: get_max_depth_from_path(x))
        
        # Define depth and resolve parent-child relationship
        max_depth = filtered_csv_data["BranchMaxDepth"].max()
        folder_scope = {depth: [] for depth in range(1, max_depth + 1)}
        folder_depths = sorted(list(set(filtered_csv_data["FolderPathDepth"])))
        skip_all = False
        reload = False
        for folder_depth in folder_depths:
            temp_data = filtered_csv_data.loc[filtered_csv_data["FolderPathDepth"] == folder_depth]
            for idx in temp_data.index:
                folder_path = strip_text(temp_data.loc[idx, "FolderPath"], char_to_remove=os.sep)
                max_depth = temp_data.loc[idx, "BranchMaxDepth"]
                depth_options = [depth for depth in range(folder_depth, max_depth + 1)]
                if folder_path not in folder_scope[folder_depth]:
                    print(f"\n--> In processing {folder_path}")
                    action, depth_input = depth_input_loop(services, depth_options)
                    match action:
                        case MenuActions.SKIP:
                            continue
                        case MenuActions.SKIP_ALL:
                            skip_all = True
                            break
                        case MenuActions.INTERUPT:
                            reload = True
                            break
                        case MenuActions.SUCCESS:
                            get_folders_under(folder_path, depth_input, folder_scope)
                            continue
                else:
                    print(f"\n--> In processing {folder_path}")
                    print(f"Skiped as already in scope. Hierarchy resolution step required")
            
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

def manual_input_loop(services: AppServices): # 2nd level
    
    # Separator to parse user input
    paths_separator = ","
    required_col1 = "FolderPath"
    menu_cls = services.menu
    notifier_cls = services.notifier
    messages_cls = services.messages
    prompt_cls = services.prompt

    while True:
        # Request user input
        user_input, action = prompt_user(
            menu_cls.manual_input(), 
            prompt_cls.MANUAL,
            strip_text
        )
        if action is not None:
            return None, MenuActions.RETURN
        
        # Parse user input
        folder_paths = parse_text(user_input, paths_separator)
        
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
            notifier_cls.notify(messages_cls.Base.EMPTY_INPUT, NotificationLevel.WARNING)
            continue
        notifier_cls.notify(messages_cls.ManualProcessing.PATHS_VALID_f.format(paths=valid_paths), NotificationLevel.SUCCESS)
        # Notify user about duplicated entries
        if duplicated_paths:
            notifier_cls.notify(messages_cls.ManualProcessing.DUPLICATED_PATHS_f.format(paths=duplicated_paths), NotificationLevel.WARNING)
        # Notify user about corrupted entries
        if corrupted_paths:
            notifier_cls.notify(messages_cls.ManualProcessing.CORRUPTED_PATHS_f.format(paths=corrupted_paths), NotificationLevel.WARNING)

        path_data = pd.DataFrame()
        path_data[required_col1] = valid_paths
        path_data["FolderPathDepth"] = path_data[required_col1].apply(lambda x: get_depth_from_root(x))
        path_data["BranchMaxDepth"] = path_data[required_col1].apply(lambda x: get_max_depth_from_path(x))

        # Define depth and resolve parent-child relationship
        max_depth = path_data["BranchMaxDepth"].max()
        folder_scope = {depth: [] for depth in range(1, max_depth + 1)}
        folder_depths = sorted(list(set(path_data["FolderPathDepth"])))
        return_back = False 
        for folder_depth in folder_depths:
            if return_back:
                break
            temp_data = path_data.loc[path_data["FolderPathDepth"] == folder_depth]
            for idx in temp_data.index:
                folder_path = strip_text(temp_data.loc[idx, "FolderPath"], char_to_remove=os.sep)
                max_depth = temp_data.loc[idx, "BranchMaxDepth"]
                depth_options = [depth for depth in range(folder_depth, max_depth + 1)]
                if folder_path not in folder_scope[folder_depth]:
                    print(f"\n--> In processing {folder_path}")
                    action, depth_input = depth_input_loop(services, depth_options)
                    match action:
                        case MenuActions.SKIP:
                            continue
                        case MenuActions.SUCCESS:
                            get_folders_under(folder_path, depth_input, folder_scope)
                            continue
                        case MenuActions.RETURN:
                            folder_scope = {depth: [] for depth in range(1, max_depth + 1)}
                            return_back = True
                            notifier_cls.notify(messages_cls.ManualProcessing.INPUT_RESET, NotificationLevel.INFO)
                            break
                else:
                    print(f"\n--> In processing {folder_path}")
                    print(f"Skiped as already in scope. Hierarchy resolution step required")

        if return_back:
            continue
        
        return folder_scope, MenuActions.SUCCESS

def depth_input_loop(services: AppServices, depth_options): # 3rd level
    
    menu_cls = services.menu
    notifier_cls = services.notifier
    messages_cls = services.messages
    prompt_cls = services.prompt

    while True:
        # Request user input
        depth_input, action = prompt_user(
            menu_cls.depth_input(depth_options),
            prompt_cls.MAIN,
            strip_text,
            lower_text
        )
        if action is not None:
            return MenuActions.INTERUPT, None

        if depth_input == "skip":
            return MenuActions.SKIP, None
        elif depth_input == "skipall":
            return MenuActions.SKIP_ALL, None

        try:
            depth_input = int(depth_input)
            if depth_input in depth_options:
                return MenuActions.SUCCESS, depth_input
            else:
                notifier_cls.notify(messages_cls.Base.INVALID_INPUT, NotificationLevel.WARNING)
        except ValueError:
            notifier_cls.notify(messages_cls.Base.INVALID_INPUT, NotificationLevel.WARNING)

if __name__ == "__main__":
    app_services = AppServices(Menu, Prompt, Messages, Notifier)
    paths_by_depth = main_loop(app_services)