from enum import StrEnum, auto
import os
import pandas as pd
from pandas.errors import ParserError
from string import Formatter
from typing import Tuple, Optional, Iterable

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

def get_template_vars(template: str):
    return{
        field_name
        for _, field_name, _, _, in Formatter().parse(template)
        if field_name
    }

# CLI elements
class MenuActions(StrEnum):
    EXIT = auto()
    INTERUPT = auto()
    SKIP = auto()
    SKIP_ALL = auto()
    SUCCESS = auto()
    FAILED = auto()

cli_assets = {
    "icons": {
        "empty": '🚫',
        "keyboard":'⌨️',
        "check_mark": '✅',
        "cross_mark": '❌',
        "stop_sign": '🛑',
        "warning_sign": '⚠️',
        "right_arrow": '➡️',
        "leftwards_arrow_with_hook": '↩️',
        "restart": '🔄',
        "information":'ℹ️',
        "bullseye": '🎯'
    },
    "separators": {
        "space": " ",
        "dash": "-"
    },
    "templates": {
        "header": "{new_line}{separator}{text}{separator}",
        "menu_line": "{icon}{separator}{text}",
        "prompt": "{icon}{separator}{text}",
        "notification": "{icon}{separator}{text}",
    }
}

cli_elements = {
    "headers": {
        "defaults": {"template": "header", "new_line": "\n" , "separator": ("dash", 5), "text": "default"},
        "main": {"text": "Main"},
        "csv_load": {"text": "CSV load"},
        "manual_load": {"text": "Manual load"},
        "depth": {"text": "Depth"}
    },
    "menu_lines": {
        "defaults": {"template": "menu_line", "icon":"empty", "separator": ("space", 2), "text": "default"},
        "exit": {"icon":"cross_mark", "separator": ("space", 1), "text": "Press 'Ctrl+C' to suspend the script"},
        "return_back": {"icon":"leftwards_arrow_with_hook", "text": "Press 'Ctrl+C' to go back"},
        "restart": {"icon":"restart",  "separator": ("space", 1), "text": "Press 'Ctrl+C' to cancel current input and retry"},
        "skip": {"icon":"keyboard", "text": "Type 'skip' to skip folder path"},
        "skip_all": {"icon":"keyboard", "text": "Type 'skipall' to skip all folder path(s)"},
        "csv_load": {"icon":"keyboard", "text": "Type 'csv' to load folder path(s) from CSV"},
        "manual_load": {"icon":"keyboard","text": "Type 'manual' to provide folder path(s) directly in CLI"}
    },
    "prompts": {
        "defaults": {"template": "prompt", "icon":"right_arrow", "separator": ("space", 2), "text": "Provide your option: "},
        "csv": {"text": "Provide link to CSV file: "},
        "manual": {"text": "Provide one or several folder path(s) separated with {paths_separator}: "},
        "depth": {"text": "Provide 'depth' value from the range {depth_range}: "}
    },
    "warnings": {
        "defaults": {"template": "notification", "icon": "warning_sign", "separator": ("space", 2), "text": "default"},
        "invalid_input": {"text": "Invalid input"}, # General
        "empty_input": {"text": "No folder path(s) to process"}, # General
        "csv_load_failed": {"text": "CSV loading failed with {error}"}, # CSV
        "file_not_found": {"text": "Provided path '{path}' is not a file"}, # File / Extension
        "extension_not_supported": {"text": "Provided extension '{ext}' is not supported"}, # File / Extension
        "missing_columns": {"text": "Required columns {cols} are missing"}, # Columns
    },
    "infos": {
        "defaults": {"template": "notification", "icon": "information", "separator": ("space", 2), "text": "default"},
        "exit": {"text": "Script terminated"}, # General
        "output_ready": {"text": "Output ready"}, # General
        "valid_paths": {"text": "Valid folder path(s) identified {paths}"}, # Folder paths
        "duplicated_removed": {"text": "{count} duplicate path(s) removed"}, # Folder paths
        "corrupted_removed": {"text": "{count} corrupted path(s) removed"}, # Folder paths
        "hierarchy_clash": {"text": "Skipped as already in scope. Hierarchy resolution step required"}, # Folder paths
    }
}

menus = {
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
        ("headers", "depth"), 
        ("menu_lines", "restart"), 
        ("menu_lines", "skip"), 
        ("menu_lines", "skip_all")
    ]
}

# CLI execution logic
def render_cli_element(config: dict, context: str, cli_assets: dict) -> str:
    # unpack assets
    icons = cli_assets["icons"]
    separators = cli_assets["separators"]
    templates = cli_assets["templates"]
    # context is a key in dict, so we need to check if it available in provided config
    assert isinstance(config, dict), f"config should be a dictionary, {type(config)} provided instead"
    assert context in config, f"{context} is not found in config"
    # i need to understand the template that needs to be populated
    defaults = config.get("defaults", {})
    assert "template" in defaults, f"Template should be defined in defaults"
    template_name = defaults.get("template")
    template = templates[template_name]
    template_vars = get_template_vars(template)
    default_config = {k : defaults.get(k, None) for k in template_vars if defaults.get(k, None) is not None}
    # unpack specific context configurations
    context_config = {k : config[context].get(k, None) for k in template_vars if config[context].get(k, None) is not None} 
    # Update default config with context specific values
    default_config.update(context_config)
    if "icon" in default_config:
        default_config["icon"] = icons[default_config["icon"]]
    if "separator" in default_config:
        separator = separators[default_config["separator"][0]]
        separator_len = default_config["separator"][1]
        default_config["separator"] = separator * separator_len
    ui_element = template.format(**default_config)
    return ui_element

def prompt_user(prompt: str)-> Tuple[str | None, MenuActions | None]:
    try:
        user_input = input(prompt)
        return user_input, MenuActions.SUCCESS
    except KeyboardInterrupt:
        return None, MenuActions.INTERUPT

def main_loop(menus, cli_elements, cli_assets): # 1st level
    
    input_handler = {
        "csv": csv_loop,
        "manual": manual_loop,
    }

    while True:
        # Request user input
        main_menu = menus["main_menu"]
        for menu_line in main_menu:
            config_name, context = menu_line
            print(render_cli_element(cli_elements[config_name], context, cli_assets))
        user_input, action = prompt_user(render_cli_element(cli_elements["prompts"], "defaults", cli_assets))
        match action:
            case MenuActions.SUCCESS:
                user_input = lower_text(strip_text(user_input))
            case MenuActions.INTERUPT:
                print()
                print(render_cli_element(cli_elements["infos"], "exit", cli_assets))
                break

        #  User input handling
        handler = input_handler.get(user_input)

        if not handler:
            print(render_cli_element(cli_elements["warnings"], "invalid_input", cli_assets))
            continue
        
        loop_func = handler
        folder_scope, in_action = loop_func(menus, cli_elements, cli_assets)

        # Loop control parameters check
        match in_action:
            case MenuActions.INTERUPT:
                continue
            case MenuActions.FAILED:
                print(render_cli_element(cli_elements["warnings"], "empty_input", cli_assets))
                continue
            case MenuActions.SUCCESS:
                print(render_cli_element(cli_elements["infos"], "output_ready", cli_assets))
                return folder_scope

def csv_loop(menus, cli_elements, cli_assets): # 2nd level
    
    # Required CSV columns for correct input validation
    required_col1 = "FolderPath"
    test_req1 = "FolderPathTest"
    
    while True:
        # Request user input
        csv_menu = menus["csv_menu"]
        for menu_line in csv_menu:
            config_name, context = menu_line
            print(render_cli_element(cli_elements[config_name], context, cli_assets))
        user_input, action = prompt_user(render_cli_element(cli_elements["prompts"], "csv", cli_assets))
        match action:
            case MenuActions.SUCCESS:
                user_input = strip_text(user_input)
            case MenuActions.INTERUPT:
                print()
                return None, MenuActions.INTERUPT
        
        # Check whether provided link is file, otherwise continue
        if not is_file(user_input):
            print(render_cli_element(cli_elements["warnings"], "file_not_found", cli_assets).format(path=user_input))
            continue
        # Check whether file extension == 'csv', otherwise continue
        file_ext = get_file_extension(user_input)
        file_ext = lower_text(strip_text(file_ext))
        if file_ext != '.csv':
            print(render_cli_element(cli_elements["warnings"], "extension_not_supported", cli_assets).format(ext=file_ext))
            continue
        # Open CSV file as dataframe
        csv_data, error = open_csv(user_input)
        if error:
            print(render_cli_element(cli_elements["warnings"], "csv_load_failed", cli_assets).format(ext=file_ext))
            continue
        # Validate CSV columns
        if required_col1 not in csv_data.columns:
            print(render_cli_element(cli_elements["warnings"], "missing_columns", cli_assets).format(cols=required_col1))
            continue
        # Validate CSV data
        csv_data[test_req1] = csv_data[required_col1].apply(lambda x: True if is_folder(x) else False)
        # Normalize CSV data
        normalized_csv_data = remove_duplicates(csv_data, column_name=required_col1)
        duplicates_count = csv_data.shape[0] - normalized_csv_data.shape[0]
        if duplicates_count:
            print(render_cli_element(cli_elements["infos"], "duplicated_removed", cli_assets).format(count=duplicates_count))
        # Select valid entries
        condition = normalized_csv_data[test_req1] == True
        filtered_csv_data = filter_df(normalized_csv_data, condition)
        if filtered_csv_data.empty:
            print(render_cli_element(cli_elements["warnings"], "empty_input", cli_assets))
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
                    depth_input, in_action = depth_loop(menus, depth_options)
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
                            get_folders_under(folder_path, depth_input, folder_scope)
                            continue
                else:
                    print(f"\n--> In processing {folder_path}")
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

def manual_loop(menus, cli_elements, cli_assets): # 2nd level
    
    # Separator to parse user input
    separator = ","
    required_col1 = "FolderPath"

    while True:
        # Request user input
        manual_menu = menus["manual_menu"]
        for menu_line in manual_menu:
            config_name, context = menu_line
            print(render_cli_element(cli_elements[config_name], context, cli_assets))
        user_input, action = prompt_user(render_cli_element(cli_elements["prompts"], "manual", cli_assets).format(paths_separator=separator))
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
            print(render_cli_element(cli_elements["warnings"], "empty_input", cli_assets))
            continue
        print(render_cli_element(cli_elements["infos"], "valid_paths", cli_assets).format(count=len(valid_paths)))
        # Notify user about duplicated entries
        if duplicated_paths:
            print(render_cli_element(cli_elements["infos"], "duplicated_removed", cli_assets).format(count=len(duplicated_paths)))
        # Notify user about corrupted entries
        if corrupted_paths:
            print(render_cli_element(cli_elements["infos"], "corrupted_removed", cli_assets).format(count=len(corrupted_paths)))

        path_data = pd.DataFrame()
        path_data[required_col1] = valid_paths
        path_data["FolderPathDepth"] = path_data[required_col1].apply(lambda x: get_depth_from_root(x))
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
                    print(f"\n--> In processing {folder_path}")
                    depth_input, in_action = depth_loop(menus, depth_options)
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
                            get_folders_under(folder_path, depth_input, folder_scope)
                            continue
                else:
                    print(f"\n--> In processing {folder_path}")
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

def depth_loop(menus, cli_elements, cli_assets, depth_options): # 3rd level

    while True:
        # Request user input
        depth_menu = menus["depth_menu"]
        for menu_line in depth_menu:
            config_name, context = menu_line
            print(render_cli_element(cli_elements[config_name], context, cli_assets))
        depth_input, action = prompt_user(render_cli_element(cli_elements["prompts"], "depth", cli_assets).format(depth_range=depth_options))
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
                print(render_cli_element(cli_elements["warnings"], "invalid_input", cli_assets))
                continue
        except ValueError:
            print(render_cli_element(cli_elements["warnings"], "invalid_input", cli_assets))

if __name__ == "__main__":
    paths_by_depth = main_loop(menus, cli_elements, cli_assets)