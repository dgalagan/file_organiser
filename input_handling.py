from enum import IntEnum, StrEnum, auto
from itertools import combinations, product
import os
import pandas as pd
from typing import Optional, Any, Iterable, Iterator, Tuple, Callable

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
    try:
        csv_data = pd.read_csv(path)
        return csv_data
    except:
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

class Menu:

    EXIT = "🛑 Type 'exit' to suspend the script"
    RETURN = "↩️  Press 'Ctrl+C' to go back"
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

# Command line helper functions
def prompt_user(menu_func: Callable, prompt_text: str, strip: bool = True, lower: bool = True) -> Tuple[str | None, MenuActions | None]:
    try:
        menu_func()
        user_input = input(prompt_text)
        if strip:
            user_input = user_input.strip()
        if lower:
            user_input = user_input.lower()
        return user_input, None
    except KeyboardInterrupt:
        print()
        return None, MenuActions.RETURN

# Command line execution logic 
# 1st level
def main_loop(menu_obj, input_dict):
    
    input_handler = {
        "csv": (menu_obj, csv_input_loop),
        "manual": (menu_obj, manual_input_loop),
    }

    while True:
        # Request user input
        try:
            menu_obj.main()
            user_input = input("➜  Select your option: ").strip().lower()
        except KeyboardInterrupt:
            print("\n\n⚠️  Type 'exit' to terminate the script")
            continue
    
        # User input handling
        if user_input == 'exit':
            print("\n❌ Script terminated\n")
            break

        handler = input_handler.get(user_input)

        if not handler:
            print("\n⚠️  Invalid input provided, 🔄 restarting")
            continue
        
        menu_func, loop_func = handler
        in_action = loop_func(menu_func, input_dict)
        
        # Loop control parameters check
        match in_action:
            case MenuActions.EXIT:
                print("\n❌ Script terminated\n")
                break
            case MenuActions.RETURN:
                continue
            case MenuActions.SUCCESS:
                # Itentify subpaths
                direct_subpaths = find_subpaths_against(input_dict[ProcessingDepth.DIRECT_SUB], input_dict[ProcessingDepth.FULL_HIERARCHY])
                full_subpaths = find_subpaths_within(input_dict[ProcessingDepth.FULL_HIERARCHY])
                # Remove subpaths
                if direct_subpaths:
                    print(f"⚠️  Subpaths {direct_subpaths} identified in {ProcessingDepth(0).name} and will be removed")
                    input_dict[ProcessingDepth.DIRECT_SUB] = remove_subpaths(input_dict[ProcessingDepth.DIRECT_SUB], direct_subpaths)
                if full_subpaths:
                    print(f"⚠️  Subpaths {full_subpaths} identified in {ProcessingDepth(1).name} and will be removed")
                    input_dict[ProcessingDepth.FULL_HIERARCHY] = remove_subpaths(input_dict[ProcessingDepth.FULL_HIERARCHY], full_subpaths)
                print(f"➡️  Input obtained successfully {input_dict}\n")
                break

    return input_dict

# 2nd level
def csv_input_loop(menu_obj, input_dict):
    
    # Required CSV columns for correct input validation
    required_col1, required_col2 = ("FolderPath", "ProcessingDepth")
    test_req1, test_req2 = ("FolderPathTest", "ProcessingDepthTest")
    required_cols = [required_col1, required_col2]
    
    while True:
        # Request user input
        user_input, action = prompt_user(
            menu_obj.csv_input, 
            "⌨️  Please provide link to CSV file: ",
            lower=False
        )
        if action is not None:
            return MenuActions.RETURN

        # Check whether provided link is file, otherwise continue
        if not is_file(user_input):
            print(f"\n⚠️  Provided path '{user_input}' is not a file, 🔄 restarting")
            continue    
        # Check whether file extension == 'csv', otherwise continue
        file_extension = get_file_extension(user_input)
        if not file_extension == '.csv':
            print(f"\n⚠️  Provided file extension '{file_extension}' is not supported, 🔄 restarting")
            continue
        # Open CSV file as dataframe
        csv_data = open_csv(user_input)
        if csv_data is None:
            print("\n⚠️  Provided CSV file empty or corrupted and could not be opened, 🔄 restarting")
            continue
        print("\n✅ CSV file opened successfully")
        # Validate CSV columns
        is_missing_cols, missing_cols = validate_cols({*csv_data}, required_cols)
        if is_missing_cols:
            print(f"⚠️  Required columns {missing_cols} are missing, please try again")
            continue
        print(f"✅ Required columns {required_cols} identified")
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
            print("\n⚠️ Valid folder path(s) are missing, 🔄 restarting")
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
        # transformed_data_idxs = list(transformed_data.index.values)
        for idx in transformed_data.index.values:
            input_dict[idx] = transformed_data[required_col1].iloc[idx]
            # Check if dict value is not empty and provide some notification
            if input_dict[idx]:
                path_count = len(input_dict[idx])
                total_paths_added += path_count
                print(f"✅ {path_count} folder path(s) identified for {ProcessingDepth(idx).name} processing")
    
        # Loop control parameters check
        if total_paths_added > 0:
            return MenuActions.SUCCESS
        else:
            print("\n⚠️  Valid folder path(s) are missing, 🔄 restarting")
def manual_input_loop(menu_obj, input_dict):
    # Separator to parse user input
    paths_separator = ","
    
    while True:
        # Request user input
        user_input, action = prompt_user(
            menu_obj.manual_input, 
            f"⌨️  Please provide one or several folder path(s) separated with {paths_separator}: ",
            lower=False
        )
        if action is not None:
            return MenuActions.RETURN
        
        # Process folder paths
        if paths_separator in user_input:
            folder_paths = split_string(user_input, paths_separator)
        else:
            folder_paths = [user_input]
        
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
            print("\n⚠️  Provided folder path(s) are invalid, 🔄 restarting")
            continue
        print("\n✅ Provided folder path(s) are valid, please proceed with processing depth selection")
        # Notify user about duplicated entries
        if duplicated_paths:
            print(f"⚠️  Duplicated folder path(s) identified {duplicated_paths} and won't be processed")
        # Notify user about corrupted entries
        if corrupted_paths:
            print(f"⚠️  Corrupted folder path(s) identified {corrupted_paths} and won't be processed ")

        skip_counter = 0
        add_direct_counter = 0
        add_full_counter = 0
        return_back = 0
        
        for valid_path in valid_paths:
            in_action = processing_depth_input_loop(menu_obj, valid_path, input_dict)
            # Loop control parameters check
            match in_action:
                case MenuActions.RETURN:
                    print(f"\n⚠️  Please note that input dictionary has been reset")
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
            print(f"\n✅ {add_direct_counter} folder path(s) identified for {ProcessingDepth(0).name} processing")
            print(f"✅ {add_full_counter} folder path(s) identified for {ProcessingDepth(1).name} processing")
            return MenuActions.SUCCESS
        elif total_paths_added == 0:
            print("\n⚠️  Valid folder path(s) are missing, 🔄 restarting")

# 3rd level
def processing_depth_input_loop(menu_obj, folder_path, input_dict):
    
    while True:
        # Request user input
        user_input, action = prompt_user(
            menu_obj.processing_depth, 
            f"➜  Select your option for {folder_path}: "
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
            print("\n⚠️  Invalid input provided, 🔄 restarting")

if __name__ == "__main__":
    folder_scope = {ProcessingDepth.DIRECT_SUB:[], ProcessingDepth.FULL_HIERARCHY:[]}
    folder_scope = main_loop(Menu, folder_scope)