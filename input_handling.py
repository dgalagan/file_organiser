from enum import IntEnum, StrEnum, auto
from itertools import combinations, product
import os
import pandas as pd
from typing import Optional, Any, Iterable, Iterator, Tuple

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

def split_string(value: str, separator: Optional[str] = None) -> list[str]:
    
    if separator is None:
        return value
    
    return value.split(separator)

def open_csv(path: str) -> pd.DataFrame | None:
    
    # Check whether provided link is file, otherwise continue
    if not is_file(path):
        print(f"\n❌ Provided path {path} is not a file")
        return None
    
    # Check whether file extension == 'csv', otherwise continue
    file_extension = get_file_extension(path)
    if not file_extension == '.csv':
        print(f"\n❌ Provided file extension is not supported {file_extension}")
        return None

    # Open csv file as dataframe, otherwise continue
    try:
        csv_data = pd.read_csv(path)
        return csv_data
    except:
        print("\n❌ Provided CSV file empty or corrupted and could not be opened")
        return None
    
def validate_df_cols(df_cols, required_cols) -> bool:
    # Check whether required columns are available
    if required_cols > df_cols:
        missing_cols = required_cols - df_cols
        print(f"❌ Required columns {missing_cols} are missing")
        return False
    else:
        print(f"✅ Required columns {required_cols} identified")
        return True

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
        print("Valid entries are absent\n")
        return None
    else:
        return filtered_df

def get_all_pairs(paths: list[str]) -> Iterator[Tuple[Any, Any]]:
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

    # verify parent-child relationship
    # If b equals to a common path in pair, mean b is parent so a is child and vice versa
    if abs_paths[0] == get_common_path(abs_paths):
        return abs_paths[1]
    elif abs_paths[1] == get_common_path(abs_paths):
        return abs_paths[0]
    else:
        return None

def find_subpaths_within(paths: list[str]) -> list[str]:
    return [child for path_pair in get_all_pairs(paths) if (child := get_child_folder(path_pair)) is not None]
  
def find_subpaths_against(paths: list[str], against_paths:list[str]) -> list[str]:
    return [child for path_pair in get_cross_pairs(paths, against_paths) if (child := get_child_folder(path_pair)) is not None]

def remove_subpaths(paths: list[str], to_remove: list[str]) -> list[str]:
        return list(set(paths) - set(to_remove))

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

class ConsoleMenu:

    EXIT = "🛑 Print 'exit' to suspend the script"
    RETURN = "↩️  Press 'Ctrl+C' to go back"
    CSV = "⌨️  Print 'csv' to load folder path(s) from CSV"
    MANUAL = "⌨️  Print 'manual' to provide folder path(s) manually"
    CSV_INPUT = "⌨️  Print 'input' to provide link to CSV file"
    MANUAL_INPUT = "⌨️  Print 'input' to provide one or several folder path(s)"
    DEPTH_0 = "⌨️  Print '0' to process direct child objects only"
    DEPTH_1 = "⌨️  Print '1' to process the entire nested hierarchy"
    SKIP = "⌨️  Print 'skip' to skip folder path"

    @classmethod
    def main_menu(cls):
        print("\n----Main menu----")
        print(cls.EXIT)
        print(cls.CSV)
        print(cls.MANUAL)

    @classmethod
    def csv_menu(cls):
        print("\n----CSV menu----")
        print(cls.EXIT)
        print(cls.RETURN)
        print(cls.CSV_INPUT)

    @classmethod
    def manual_menu(cls):
        print("\n----Manual menu----")
        print(cls.EXIT)
        print(cls.RETURN)
        print(cls.MANUAL_INPUT)

    @classmethod
    def csv_menu_input(cls):
        print()
        print(cls.RETURN)

    @classmethod
    def manual_menu_input(cls):
        print()
        print(cls.RETURN)

    @classmethod
    def processing_depth_menu(cls):
        print("\n----Processing depth menu----")
        print(cls.RETURN)
        print(cls.DEPTH_0)
        print(cls.DEPTH_1)
        print(cls.SKIP)

# Command line interface logic
# 1st level
def main_loop(config):
    # Unpack config
    csv_config = config["csv_config"]
    manual_config = config["manual_config"]
    folder_scope = config["folder_scope"]
    
    while True:
        # Request user inpu
        ConsoleMenu.main_menu()
        user_input = input("➜  Select your option: ").strip().lower()

        # User input handling
        if user_input == 'exit':
            print("\n❌ Script terminated\n")
            break
        elif user_input == 'csv':
            in_action = csv_menu_loop(csv_config, folder_scope)
        elif user_input == 'manual':
            in_action = manual_menu_loop(manual_config, folder_scope)
        else:
            print("\n🔁 Invalid input provided please try again")
            continue
        
        # Loop control parameters check
        match in_action:
            case MenuActions.EXIT:
                print("\n❌ Script terminated\n")
                break
            case MenuActions.RETURN:
                continue
            case MenuActions.SUCCESS:
                # Itentify subpaths
                direct_subpaths = find_subpaths_against(folder_scope[ProcessingDepth.DIRECT_SUB], folder_scope[ProcessingDepth.FULL_HIERARCHY])
                full_subpaths = find_subpaths_within(folder_scope[ProcessingDepth.FULL_HIERARCHY])
                # Remove subpaths
                if direct_subpaths:
                    print(f"⚠️  Subpaths {direct_subpaths} identified in {ProcessingDepth(0).name} and will be removed")
                    folder_scope[ProcessingDepth.DIRECT_SUB] = remove_subpaths(folder_scope[ProcessingDepth.DIRECT_SUB], direct_subpaths)
                if full_subpaths:
                    print(f"⚠️  Subpaths {full_subpaths} identified in {ProcessingDepth(1).name} and will be removed")
                    folder_scope[ProcessingDepth.FULL_HIERARCHY] = remove_subpaths(folder_scope[ProcessingDepth.FULL_HIERARCHY], full_subpaths)
                print(f"➡️  Input obtained successfully {folder_scope}\n")
                break

    return folder_scope

# 2nd level
def csv_menu_loop(csv_config, folder_scope):
    while True:
        # Request user input
        try:
            ConsoleMenu.csv_menu()
            user_input = input("➜  Select your option: ").strip().lower()
        except KeyboardInterrupt:
            print()
            return MenuActions.RETURN
        
        # User input handling
        if user_input == "exit":
            return MenuActions.EXIT
        elif user_input == "input":
            in_action = csv_input_loop(csv_config, folder_scope)
            # Loop control parameters check
            if in_action is MenuActions.RETURN:
                continue
            return in_action
        else:
            print("\n🔁 Invalid input provided please try again")
            continue
def manual_menu_loop(manual_config, folder_scope):
    while True:
        # Request user input
        try:
            ConsoleMenu.manual_menu()
            user_input = input("➜  Select your option: ").strip().lower()
        except KeyboardInterrupt:
            print("")
            return MenuActions.RETURN
        
        # User input handling
        if user_input == "exit":
            return MenuActions.EXIT
        elif user_input == "input":
            in_action = manual_input_loop(manual_config, folder_scope)
            # Loop control parameters check
            if in_action == MenuActions.RETURN:
                continue
            return in_action
        else:
            print("\n🔁 Invalid input provided please try again")
            continue

# 3rd level
def csv_input_loop(csv_config, folder_scope):
    # Unpack csv config
    required_col1 = csv_config["required_col1"]
    required_col2 = csv_config["required_col2"]
    test_req1 = csv_config["test_req1"]
    test_req2 = csv_config["test_req2"]
    required_cols = [required_col1, required_col2]
    
    while True:
        # Request user input
        try:
            ConsoleMenu.csv_menu_input()
            user_input = input("⌨️  Please provide link to CSV file: ").strip().lower()
        except KeyboardInterrupt:
            print()
            return MenuActions.RETURN

        # Open CSV file as dataframe
        csv_data = open_csv(user_input)
        if csv_data is None:
            continue
        print("\n✅ CSV file opened successfully")
        # Validate CSV columns
        csv_cols = {*csv_data}
        if not validate_df_cols(set(csv_cols), set(required_cols)):
            continue
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
            print(f"✅ {duplicates_count} duplicate(s) removed successfully")
        # Select valid entries
        condition = (normalized_csv_data[test_req1] == True) & (normalized_csv_data[test_req2] == True)
        filtered_csv_data = filter_df(normalized_csv_data, condition)
        if filtered_csv_data is None:
            print("\n🔁 Valid folder path(s) are missing, please try again")
            continue
        print("✅ Valid folder path(s) filtered successfully")
        # Transform valid data and convert it into ditionary 
        transformed_data = pd.pivot_table(
            filtered_csv_data,
            index=required_col2,
            values=required_col1,
            aggfunc=lambda x: list(x)
        )
        # Update folder scope dictionary
        total_paths_added = 0
        transformed_data_idxs = list(transformed_data.index.values)
        for idx in transformed_data_idxs:
            folder_scope[idx] = transformed_data[required_col1].loc[idx]
            # Check if dict value is not empty and provide some notification
            if folder_scope[idx]:
                path_count = len(folder_scope[idx])
                total_paths_added += path_count
                print(f"✅ {path_count} folder path(s) identified for {ProcessingDepth(idx).name} processing")
    
        # Loop control parameters check
        if total_paths_added > 0:
            return MenuActions.SUCCESS
        else:
            print("\n🔁 Valid folder path(s) are missing, please try again")
            continue
def manual_input_loop(manual_config, folder_scope):
    # Unpack manual config
    paths_separator = manual_config["paths_separator"]
    
    while True:
        # Request user input
        try:
            ConsoleMenu.manual_menu_input()
            user_input = input(
                f"⌨️  Please provide one or several folder path(s) separated with {paths_separator}: "
                )
        except KeyboardInterrupt:
            print()
            return MenuActions.RETURN
        
        # Process folder paths
        if paths_separator in user_input:
            folder_paths = split_string(user_input, paths_separator)
        else:
            folder_paths = [user_input]
        
        valid_folder_paths = [folder_path for folder_path in folder_paths if is_folder(folder_path)]
        corrupted_folder_paths = [folder_path for folder_path in folder_paths if not is_folder(folder_path)]

        # Notify user about valid entries  
        count_valid_paths = len(valid_folder_paths)
        if count_valid_paths == 0:
            print("\n🔁 Provided folder path(s) are invalid, please try again")
            continue
        elif count_valid_paths == 1:
            print("\n✅ Provided folder path is valid, please proceed with processing depth selection")
        else:
            print("\n✅ Provided folder paths are valid, please proceed with processing depth selection")
        # Notify user about corrupted entries
        count_corrupted_paths = len(corrupted_folder_paths)
        if count_corrupted_paths == 0:
            pass
        elif count_corrupted_paths == 1:
            print(f"\n⚠️  Corrupted folder path identified and won't be processed {corrupted_folder_paths}")
        else:
            print(f"\n⚠️  Corrupted folder paths identified and won't be processed {corrupted_folder_paths}")

        skip_counter = 0
        add_direct_counter = 0
        add_full_counter = 0
        return_back = 0

        for valid_folder_path in valid_folder_paths:
            in_action = processing_depth_input_loop(valid_folder_path, folder_scope)
            # Loop control parameters check
            match in_action:
                case MenuActions.RETURN:
                    print(f"\n⚠️  Please note that folder scope dictionary has been reset")
                    # folder_scope = {ProcessingDepth.DIRECT_SUB:[],ProcessingDepth.FULL_HIERARCHY:[]} Why this is working differently
                    folder_scope[ProcessingDepth.DIRECT_SUB] = []
                    folder_scope[ProcessingDepth.FULL_HIERARCHY] = []
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
            print(f"\n✅ {add_direct_counter} folder path(s) identified for {ProcessingDepth(0).name} processing")
            print(f"✅ {add_full_counter} folder path(s) identified for {ProcessingDepth(1).name} processing")
            return MenuActions.SUCCESS
        elif total_paths_added == 0:
            print("\n🔁 Valid folder path(s) are missing, please try again")
            continue

# 4th level
def processing_depth_input_loop(valid_folder_path, folder_scope):
    
    while True:
        # Request user input
        try:
            ConsoleMenu.processing_depth_menu()
            user_input = input(f"➜  Select your option for {valid_folder_path}: ").strip().lower()
        except KeyboardInterrupt:
            print()
            return MenuActions.RETURN

        if user_input == "skip":
            return MenuActions.SKIP
        elif int(user_input) == 0:
            folder_scope[ProcessingDepth.DIRECT_SUB].append(valid_folder_path)
            return MenuActions.ADD_DIRECT_SUB
        elif int(user_input) == 1:
            folder_scope[ProcessingDepth.FULL_HIERARCHY].append(valid_folder_path)
            return MenuActions.ADD_FULL_HIERARCHY
        else:
            print("\n🔁 Invalid input provided please try again")
            continue

# Configuration
config = {
    "csv_config":{
        "required_col1" : "FolderPath",
        "test_req1": "FolderPathTest",
        "required_col2": "ProcessingDepth",
        "test_req2": "ProcessingDepthTest"
    },
    
    "manual_config":{
        "paths_separator": ","
    },
    
    "folder_scope":{
        ProcessingDepth.DIRECT_SUB:[],
        ProcessingDepth.FULL_HIERARCHY:[]
    }
}

if __name__ == "__main__":
    folder_scope = main_loop(config)