from cli.renderer import render_cli_object, render_cli_grouped_object
from core.df_processor import DfProcessor
from enum import StrEnum, auto
import pandas as pd
import os
from utils.path import is_parent, is_dir, clean_dir, is_file
from utils.text import lowercase_text, strip_text
from configs.transformation_cfg import PIPELINE

# To improve:
# instead of os.walk(), create recursion based on os.scandir()
# self-reporting improvement
# review error handling
# manage lowercase path cases in manual input

# Actions
class MenuActions(StrEnum):
    EXIT = auto()
    INTERUPT = auto()
    SKIP = auto()
    SKIP_ALL = auto()
    SUCCESS = auto()
    FAILED = auto()
    RESTART = auto()

# Destination directory for categorized files
def get_dest_dir(cli_objects: dict) -> str:
    while True:
        print(render_cli_object(cli_objects["header"], element_name="dest_dir"))
        try:
            dest_dir_path = input("➡️  Provide empty directory for organized files: ")
        except KeyboardInterrupt:
            print()
            return ''
        
        if os.path.exists(dest_dir_path):
            if is_dir(dest_dir_path):
                dir_content = os.listdir(dest_dir_path)
                if not dir_content:
                    return dest_dir_path
                if prepare_dest_dir(dest_dir_path, cli_objects):
                    return dest_dir_path
                else:
                    continue
            elif is_file(dest_dir_path):
                print(render_cli_object(cli_objects["warning"], "invalid_input"))
                continue
            else:
                print(render_cli_object(cli_objects["warning"], "invalid_input"))
                continue
        else:
            try:
                os.makedirs(dest_dir_path, exist_ok=True)
                return dest_dir_path
            except Exception as e:
                print(e)
                print(render_cli_object(cli_objects["warning"], "invalid_input"))
                continue

def prepare_dest_dir(path: str, cli_objects: dict) -> bool:
    # Interact with user in case directory has files
    while True:
        try:
            permission = input(render_cli_object(cli_objects["prompt"], element_name="setup_env", target_path=path))
        except KeyboardInterrupt:
            print()
            return False
        
        if permission == "y":
            try:
                clean_dir(path)
                return True
            except Exception as e:
                raise RuntimeError(f"Failed to clean {path}. Reason {e}")
        elif permission == "n":
            return False
        else:
            print(render_cli_object(cli_objects["warning"], "invalid_input"))
            continue

# Source directories for file processing
def get_input_dirs(cli_grouped_objects: dict, cli_objects: dict) -> tuple: # 1st level
    while True:
        # Render menu
        print(render_cli_grouped_object(cli_grouped_objects["input_dirs_menu"], cli_objects))
        # Request user input
        try:
            input_option = input(render_cli_object(cli_objects["prompt"]))
            input_option = lowercase_text(strip_text(input_option))
        except KeyboardInterrupt:
            print()
            return ()
        
        # User input handling
        selected_dirs, in_action = load_dirs(cli_grouped_objects, cli_objects, input_option)
        # Loop control parameters check
        match in_action:
            case MenuActions.INTERUPT:
                continue
            case MenuActions.FAILED:
                continue
            case MenuActions.SUCCESS:
                print(render_cli_object(cli_objects["divider"]))
                return selected_dirs

def load_dirs(cli_grouped_objects: dict, cli_objects: dict, input_option: str): # 2nd level
    while True:
        # CSV Load
        if input_option == "csv":
            print(render_cli_grouped_object(cli_grouped_objects["csv_menu"], cli_objects))
            # Request user input
            try:
                csv_path = input(render_cli_object(cli_objects["prompt"], "csv"))
                csv_path = strip_text(csv_path)
            except KeyboardInterrupt:
                print()
                return None, MenuActions.INTERUPT
            # Open CSV
            try:
                df = pd.read_csv(csv_path)
            except (ValueError, FileNotFoundError, PermissionError, RuntimeError) as e:
                print(render_cli_object(cli_objects["warning"], "csv_load_failed", error=e))
                continue
        # MANUAL Load
        elif input_option == "manual":
            # Render menu
            print(render_cli_grouped_object(cli_grouped_objects["manual_menu"], cli_objects))
            # Request user input
            try:
                input_dirs = {"DirPath": []}
                while True:
                    prompt_key = "manual" if not input_dirs else "manual_additional"
                    input_dir = input(render_cli_object(cli_objects["prompt"], prompt_key))
                    input_dir = strip_text(input_dir)
                    if input_dir == "stop":
                        break
                    input_dirs["DirPath"].append(input_dir)
            except KeyboardInterrupt:
                print()
                return None, MenuActions.INTERUPT
            try:
                df = pd.DataFrame(input_dirs)
            except TypeError as e:
                print(render_cli_object(cli_objects["warning"], "manual_load_failed", error=e))
                continue
        # Invalid Input
        else:
            print(render_cli_object(cli_objects["warning"], "invalid_input"))
            return None, MenuActions.FAILED
        
        # Get data
        dir_data = DfProcessor(df).run_pipeline(PIPELINE["user_dirs"]).active_selection.sort_values("DirDepth", ascending=True).reset_index()

        # Resolve parent-child relationship clash
        reload = False
        selected_dirs = []
        skip_ids = []
        while True:
            pending_dir_ids = dir_data.index[~dir_data.index.isin(skip_ids)]
            if pending_dir_ids.empty:
                break
            dir_id = pending_dir_ids[0]
            dir_path = dir_data["DirPath"].iloc[dir_id]
            dir_depth = dir_data["DirDepth"].iloc[dir_id]
            branch_depth_from_dir = dir_data["BranchDepthFromDir"].iloc[dir_id]
            # CLI element
            print(render_cli_object(cli_objects["divider"]))
            print(render_cli_object(cli_objects["info"], "processing", dir_path=dir_path))
            print(render_cli_object(cli_objects["flow_marker"]))
            # Get user input on required processing depth
            depth_input, in_action = set_processing_depth(cli_grouped_objects, cli_objects, branch_depth_from_dir)
            dir_processing_depth = dir_depth + depth_input
            match in_action:
                case MenuActions.SKIP:
                    continue
                case MenuActions.SKIP_ALL:
                    break
                case MenuActions.INTERUPT:
                    reload = True
                    break
                case MenuActions.SUCCESS:
                    dir_data.at[dir_id, "UserInputDepth"] = depth_input
                    dir_data.at[dir_id, "DirProcessingDepth"] = dir_processing_depth
                    selected_dirs.append((dir_path, dir_processing_depth))
                    skip_ids.append(dir_id)
            
            # Check if child exist next to the
            if pending_dir_ids.size > 1:
                for pending_dir_id in pending_dir_ids[1:]:
                    pending_child = dir_data["DirPath"].iloc[pending_dir_id]
                    pending_child_depth = dir_data["DirDepth"].iloc[pending_dir_id]
                    if is_parent(dir_path, pending_child):
                        if pending_child_depth <= dir_processing_depth:
                            # CLI element
                            print(render_cli_object(cli_objects["divider"]))
                            print(render_cli_object(cli_objects["info"], "skipped", dir_path=pending_child))
                            skip_ids.append(pending_dir_id)
            else:
                break

        if reload:
            continue
        
        if not selected_dirs:
            print(render_cli_object(cli_objects["warning"], "empty_input"))
            continue
        
        print(render_cli_object(cli_objects["divider"]))
        print(render_cli_object(cli_objects["info"], "selected", dir_paths_count=len(selected_dirs)))

        return selected_dirs, MenuActions.SUCCESS

def set_processing_depth(cli_grouped_objects: dict, cli_objects: dict, branch_depth_from_dir: int): # 3rd level
    while True:
        # Render menu
        depth_options = f"0-{branch_depth_from_dir}" if branch_depth_from_dir else "0"
        print(render_cli_grouped_object(cli_grouped_objects["depth_menu"], cli_objects, depth_range=depth_options))
        # Request user input
        try:
            depth_input = input(render_cli_object(cli_objects["prompt"]))
            depth_input = lowercase_text(strip_text(depth_input))
        except KeyboardInterrupt:
            print()
            return None, MenuActions.INTERUPT
        # Process input
        if depth_input == "skip":
            return None, MenuActions.SKIP
        elif depth_input == "skipall":
            return None, MenuActions.SKIP_ALL
        else:
            try:
                depth_input = int(depth_input)
                if 0 <= depth_input <= branch_depth_from_dir:
                    return depth_input, MenuActions.SUCCESS
                else:
                    print(render_cli_object(cli_objects["warning"], "invalid_input"))
                    continue
            except ValueError:
                print(render_cli_object(cli_objects["warning"], "invalid_input"))