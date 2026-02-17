from enum import Enum, IntEnum, StrEnum, auto
from itertools import combinations, product
import os
import pandas as pd
from pandas.errors import ParserError
from typing import Optional, Any, Iterable, Iterator, Tuple, Callable

def lower_text(text: str) -> str:
    return text.lower()

def strip_text(text: str) -> str:
    return text.strip() 

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

def validate_cols(csv_cols: list[str], required_cols: list[str]) -> Tuple[bool, list | None]: 
    csv_set = set(csv_cols)
    required_set = set(required_cols)
    missing_cols = list(required_set - csv_set)

    if missing_cols:
        return True, missing_cols
    
    return False, None

def remove_duplicates(df: pd.DataFrame, column_name: Optional[str] = None) -> pd.DataFrame:

    # Check whether column provided
    if column_name is None:
        df_normalized = df.drop_duplicates(
            inplace=False
        )
    else:
        df_normalized = df.drop_duplicates(
            column_name, 
            inplace=False
            )
    return df_normalized  

def filter_df(df: pd.DataFrame, condition: str) -> pd.DataFrame | None:
    filtered_df = df[condition]
    
    if filtered_df.empty:
        return None

    return filtered_df

def get_pairs(paths: list[str]) -> Iterator[Tuple[Any, Any]]:
    return combinations(paths, 2)

def get_cross_pairs(paths_a: list[str], paths_b: list[str]) -> Iterator[Tuple[Any, Any]]:
    return product(paths_a, paths_b)

def get_child_folder(path_pair: Tuple[str, str]) -> str | None:
    # Unpack tuple
    path_a, path_b = path_pair
    # Validate input
    assert is_folder(path_a) == True and is_folder(path_b), "Pair must contain folder paths only." 
    # Extract absolute path instead of symbolic 
    abs_paths = [get_abs_path(path_a), get_abs_path(path_b)]
    # If b equals to a common path in pair, mean b is parent so a is child and vice versa
    if abs_paths[0] == get_common_path(abs_paths):
        return abs_paths[1]
    elif abs_paths[1] == get_common_path(abs_paths):
        return abs_paths[0]
    else:
        return None

def find_subpaths_within(paths: list[str]) -> list[str]:
    return [child for path_pair in get_pairs(paths) if (child := get_child_folder(path_pair)) is not None]

def find_subpaths_against(paths: list[str], against_paths:list[str]) -> list[str]:
    return [child for path_pair in get_cross_pairs(paths, against_paths) if (child := get_child_folder(path_pair)) is not None]

def remove_subpaths(paths: list[str], to_remove: list[str]) -> list[str]:
    return list(set(paths) - set(to_remove))

def remove_subpaths_from_input(input_dict):
    # Itentify subpaths
    direct_subpaths = find_subpaths_against(input_dict[ProcessingDepth.DIRECT_SUB], input_dict[ProcessingDepth.FULL_HIERARCHY])
    full_subpaths = find_subpaths_within(input_dict[ProcessingDepth.FULL_HIERARCHY])
    # Remove subpaths
    if direct_subpaths:
        print(f"⚠️  Subpaths {direct_subpaths} removed from {ProcessingDepth(0).name}")
        input_dict[ProcessingDepth.DIRECT_SUB] = remove_subpaths(input_dict[ProcessingDepth.DIRECT_SUB], direct_subpaths)
    if full_subpaths:
        print(f"⚠️  Subpaths {full_subpaths} removed from {ProcessingDepth(1).name}")
        input_dict[ProcessingDepth.FULL_HIERARCHY] = remove_subpaths(input_dict[ProcessingDepth.FULL_HIERARCHY], full_subpaths)
    return input_dict

# Command line interface elements
class ProcessingDepth(IntEnum):
    
    DIRECT_SUB = 0
    FULL_HIERARCHY = 1

class MenuActions(StrEnum):

    EXIT = auto()
    RETURN = auto()
    SUCCESS = auto()
    SKIP = auto()
    ADD_DIRECT_SUB = auto()
    ADD_FULL_HIERARCHY = auto()

class Menu:

    RETURN = "↩️  Press 'Ctrl+C' to go back"
    EXIT = "⌨️  Type 'exit' to suspend the script"
    CSV = "⌨️  Type 'csv' to load folder path(s) from CSV"
    MANUAL = "⌨️  Type 'manual' to provide folder path(s) manually"
    CSV_INPUT = "⌨️  Type 'load' to provide link to CSV file"
    MANUAL_INPUT = "⌨️  Type 'enter' to provide one or several folder path(s)"
    DEPTH_0 = "⌨️  Type '0' to process direct child objects only"
    DEPTH_1 = "⌨️  Type '1' to process the entire nested hierarchy"
    SKIP = "⌨️  Type 'skip' to skip folder path"

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
        print()
        print(cls.RETURN)

    @classmethod
    def manual_input(cls):
        print()
        print(cls.RETURN)

    @classmethod
    def processing_depth(cls):
        print("\n----Processing depth menu----")
        print(cls.RETURN)
        print(cls.DEPTH_0)
        print(cls.DEPTH_1)
        print(cls.SKIP)

class NotificationLevel(Enum): 
    
    INFO = "INFO"
    SUCCESS = "SUCCESS"
    FINISH = "FINISH"
    WARNING = "WARNING"
    ERROR = "ERROR"
    EXIT = "EXIT"

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
    
    class BaseMessages:
        EXIT = "Script terminated"
        INVALID_INPUT = "Invalid input"
        EMPTY_INPUT = "No folder path(s) to process"
        OUTPUT = "Output obtained"

        @staticmethod
        def format(msg: str, **kwargs):
            return msg.format(**kwargs)

    class CsvProcessing(BaseMessages):
        # Regular string
        CSV_LOAD_FAILED = "CSV loading failed"
        CSV_LOAD_SUCCEED = "CSV opened"
        FOLDER_PATHS_FILTERED = "Valid folder path(s) filtered"
        # f string with 1 variable
        FILE_NOT_FOUND_f = "Provided path '{path}' is not a file"
        EXTENSION_NOT_SUPPORTED_f = "Provided extension '{ext}' is not supported"
        COLUMNS_MISSING_f = "Required columns {cols} are missing"
        COLUMNS_IDENTIFIED_f = "Required columns {cols} identified"
        DUPLICATES_REMOVED_f = "{count} duplicate(s) removed"
        # f string with 2 variables
        FOLDER_PATHS_IDENTIFIED_ff = "{count} folder(s) identified for {key} processing"

    class ManualProcessing(BaseMessages):
        # Regular string
        INPUT_RESET = "Input dictionary reset"
        # f string with 1 variable
        PATHS_VALID_f = "Valid folder path(s) identified {paths}"
        DUPLICATED_PATHS_f = "Duplicated folder path(s) identified {paths} and won't be processed"
        CORRUPTED_PATHS_f = "Corrupted folder path(s) identified {paths} and won't be processed"
        # f string with 2 variables
        ADD_DIRECT_COUNTER_ff = "{count} folder path(s) identified for {key} processing"
        ADD_FULL_COUNTER_ff = "{count} folder path(s) identified for {key} processing"

# Command line helper functions
def prompt_user(
        menu_func: Callable[[], None],
        prompt_text: str,
        *transform_funcs:Callable[[str], str],
)-> Tuple[str | None, MenuActions | None]:
    try:
        menu_func()
        user_input = input(prompt_text)
        user_input = transform_text(user_input, *transform_funcs)
        return user_input, None
    except KeyboardInterrupt:
        print()
        return None, MenuActions.RETURN
# Command line execution logic 
def main_loop(menu_cls: Menu, messages_cls: Messages, notifier_cls: Notifier, input_dict): # 1st level
    
    input_handler = {
        "csv": csv_input_loop,
        "manual": manual_input_loop,
    }
    base_messages = messages_cls.BaseMessages

    while True:
        # Request user input
        try:
            menu_cls.main()
            user_input = input("➜  Select your option: ")
            user_input = transform_text(user_input, strip_text, lower_text)
        except KeyboardInterrupt:
            print("\n\n⚠️  Type 'exit' to terminate the script")
            continue

        #  User input handling
        if user_input == 'exit':
            notifier_cls.notify(base_messages.EXIT, NotificationLevel.EXIT)
            break

        handler = input_handler.get(user_input)

        if not handler:
            notifier_cls.notify(base_messages.INVALID_INPUT, NotificationLevel.WARNING)
            continue
        
        loop_func = handler
        in_action = loop_func(menu_cls, messages_cls, notifier_cls, input_dict)
        
        # Loop control parameters check
        match in_action:
            case MenuActions.EXIT:
                notifier_cls.notify(base_messages.EXIT, NotificationLevel.EXIT)
                break
            case MenuActions.RETURN:
                continue
            case MenuActions.SUCCESS:
                input_dict = remove_subpaths_from_input(input_dict)
                notifier_cls.notify(base_messages.OUTPUT, NotificationLevel.FINISH)
                break
    return input_dict
def csv_input_loop(menu_cls: Menu, messages_cls: Messages, notifier_cls: Notifier, input_dict): # 2nd level
    
    # Required CSV columns for correct input validation
    required_col1, required_col2 = ("FolderPath", "ProcessingDepth")
    test_req1, test_req2 = ("FolderPathTest", "ProcessingDepthTest")
    required_cols = [required_col1, required_col2]
    csv_processing = messages_cls.CsvProcessing
    base_messages = messages_cls.BaseMessages
    
    while True:
        # Request user input
        user_input, action = prompt_user(
            menu_cls.csv_input, 
            "⌨️  Please provide link to CSV file: ",
            strip_text
        )
        if action is not None:
            return MenuActions.RETURN
        # Check whether provided link is file, otherwise continue
        if not is_file(user_input):
            notifier_cls.notify(csv_processing.FILE_NOT_FOUND_f.format(path=user_input), NotificationLevel.WARNING)
            continue    
        # Check whether file extension == 'csv', otherwise continue
        file_ext = get_file_extension(user_input)
        file_ext = transform_text(file_ext, strip_text, lower_text)
        if file_ext != '.csv':
            notifier_cls.notify(csv_processing.EXTENSION_NOT_SUPPORTED_f.format(ext=file_ext), NotificationLevel.WARNING)
            continue
        # Open CSV file as dataframe
        csv_data = open_csv(user_input)
        if csv_data is None:
            notifier_cls.notify(csv_processing.CSV_LOAD_FAILED, NotificationLevel.WARNING)
            continue
        notifier_cls.notify(csv_processing.CSV_LOAD_SUCCEED, NotificationLevel.SUCCESS)
        # Validate CSV columns
        is_missing_cols, missing_cols = validate_cols({*csv_data}, required_cols)
        if is_missing_cols:
            notifier_cls.notify(csv_processing.COLUMNS_MISSING_f.format(cols=missing_cols), NotificationLevel.WARNING)
            continue
        notifier_cls.notify(csv_processing.COLUMNS_IDENTIFIED_f.format(cols=required_cols), NotificationLevel.SUCCESS)
        # Validate CSV data
        csv_data[test_req1] = csv_data[required_col1].apply(
            lambda x: True if is_folder(x) else False
            )
        csv_data[test_req2] = csv_data[required_col2].apply(
            lambda x: True if x in [ProcessingDepth.DIRECT_SUB, ProcessingDepth.FULL_HIERARCHY] else False
            )
        # Normalize CSV data
        normalized_csv_data = remove_duplicates(csv_data)
        duplicates_count = csv_data.shape[0] - normalized_csv_data.shape[0]
        if duplicates_count:
            notifier_cls.notify(csv_processing.DUPLICATES_REMOVED_f.format(count=duplicates_count), NotificationLevel.SUCCESS)
        # Select valid entries
        condition = (normalized_csv_data[test_req1] == True) & (normalized_csv_data[test_req2] == True)
        filtered_csv_data = filter_df(normalized_csv_data, condition)
        if filtered_csv_data is None:
            notifier_cls.notify(base_messages.EMPTY_INPUT, NotificationLevel.WARNING)
            continue
        notifier_cls.notify(csv_processing.FOLDER_PATHS_FILTERED, NotificationLevel.SUCCESS)
        # Transform valid data and convert it into ditionary 
        transformed_data = pd.pivot_table(
            filtered_csv_data,
            index=required_col2,
            values=required_col1,
            aggfunc=lambda x: list(x)
        )
        # Update folder scope dictionary
        total_paths_added = 0
        # transformed_data_idxs = list(transformed_data.index.values)
        for idx in transformed_data.index.values:
            input_dict[idx] = transformed_data[required_col1].iloc[idx]
            # Check if dict value is not empty and provide some notification
            if input_dict[idx]:
                path_count = len(input_dict[idx])
                total_paths_added += path_count
                notifier_cls.notify(csv_processing.FOLDER_PATHS_IDENTIFIED_ff.format(count=path_count, key=ProcessingDepth(idx).name), NotificationLevel.SUCCESS)
        # Loop control parameters check
        if total_paths_added > 0:
            return MenuActions.SUCCESS
        else:
            notifier_cls.notify(base_messages.EMPTY_INPUT, NotificationLevel.WARNING)
def manual_input_loop(menu_cls: Menu, messages_cls: Messages, notifier_cls: Notifier, input_dict): # 2nd level
    
    # Separator to parse user input
    paths_separator = ","
    base_messages = messages_cls.BaseMessages
    manual_processing = messages_cls.ManualProcessing
    
    while True:
        # Request user input
        user_input, action = prompt_user(
            menu_cls.manual_input, 
            f"⌨️  Please provide one or several folder path(s) separated with {paths_separator}: ",
            strip_text
        )
        if action is not None:
            return MenuActions.RETURN  
        
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
            notifier_cls.notify(base_messages.EMPTY_INPUT, NotificationLevel.WARNING)
            continue
        notifier_cls.notify(manual_processing.PATHS_VALID_f.format(paths=valid_paths), NotificationLevel.SUCCESS)
        # Notify user about duplicated entries
        if duplicated_paths:
            notifier_cls.notify(manual_processing.DUPLICATED_PATHS_f.format(paths=duplicated_paths), NotificationLevel.WARNING)
        # Notify user about corrupted entries
        if corrupted_paths:
            notifier_cls.notify(manual_processing.CORRUPTED_PATHS_f.format(paths=corrupted_paths), NotificationLevel.WARNING)

        skip_counter = 0
        add_direct_counter = 0
        add_full_counter = 0
        return_back = 0
        
        for valid_path in valid_paths:
            in_action = processing_depth_input_loop(menu_cls, messages_cls, notifier_cls, valid_path, input_dict)
            # Loop control parameters check
            match in_action:
                case MenuActions.RETURN:
                    notifier_cls.notify(manual_processing.INPUT_RESET, NotificationLevel.INFO)
                    # folder_scope = {ProcessingDepth.DIRECT_SUB:[],ProcessingDepth.FULL_HIERARCHY:[]} Why this is working differently
                    input_dict[ProcessingDepth.DIRECT_SUB] = []
                    input_dict[ProcessingDepth.FULL_HIERARCHY] = []
                    skip_counter = 0
                    add_direct_counter = 0
                    add_full_counter = 0
                    return_back += 1
                    break
                case MenuActions.SKIP:
                    skip_counter += 1
                case MenuActions.ADD_DIRECT_SUB:
                    add_direct_counter += 1
                case MenuActions.ADD_FULL_HIERARCHY:
                    add_full_counter += 1

        # User notification
        total_paths_added = add_direct_counter + add_full_counter

        # Loop control parameters check
        if return_back == 1:
            continue
        elif total_paths_added > 0:
            notifier_cls.notify(manual_processing.ADD_DIRECT_COUNTER_ff.format(count=add_direct_counter, key=ProcessingDepth(0).name), NotificationLevel.SUCCESS)
            notifier_cls.notify(manual_processing.ADD_FULL_COUNTER_ff.format(count=add_full_counter, key=ProcessingDepth(1).name), NotificationLevel.SUCCESS)
            return MenuActions.SUCCESS
        elif total_paths_added == 0:
            notifier_cls.notify(base_messages.EMPTY_INPUT, NotificationLevel.WARNING)
def processing_depth_input_loop(menu_cls: Menu, messages_cls: Messages, notifier_cls: Notifier, folder_path, input_dict): # 3rd level
    
    base_messages = messages_cls.BaseMessages

    while True:
        # Request user input
        user_input, action = prompt_user(
            menu_cls.processing_depth, 
            f"➜  Select your option for {folder_path}: ",
            strip_text,
            lower_text
        )
        if action is not None:
            return MenuActions.RETURN
        
        # Loop control parameters check
        if user_input == "skip":
            return MenuActions.SKIP
        elif user_input == "0":
            input_dict[int(user_input)].append(folder_path)
            return MenuActions.ADD_DIRECT_SUB
        elif user_input == "1":
            input_dict[int(user_input)].append(folder_path)
            return MenuActions.ADD_FULL_HIERARCHY
        else:
            notifier_cls.notify(base_messages.EMPTY_INPUT, NotificationLevel.WARNING)

if __name__ == "__main__":
    folder_scope = {ProcessingDepth.DIRECT_SUB:[], ProcessingDepth.FULL_HIERARCHY:[]}
    folder_scope = main_loop(Menu, Messages, Notifier, folder_scope)