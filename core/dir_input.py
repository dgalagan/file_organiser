from cli.components import Header, MenuLine, Prompt, Warning, Info
from cli.tokens import Separator, Icon
from enum import StrEnum, auto
import os
import pandas as pd
from pipelines import user_input_pipeline
from utils.path import is_parent, is_dir, clean_dir
from utils.text import lowercase_text, strip_text

# Actions
class MenuActions(StrEnum):
    EXIT = auto()
    INTERUPT = auto()
    SKIP = auto()
    SUCCESS = auto()
    FAILED = auto()
    RESTART = auto()

# Destination directory for categorized files
def get_dest_dir() -> str:
    while True:
        print(Header.ELEMENTS["dest_dir"].generate())
        try:
            dest_dir_path = input("➡️  Provide empty directory for organized files: ")
        except KeyboardInterrupt:
            print()
            return ''
        
        if os.path.exists(dest_dir_path) and is_dir(dest_dir_path):
            return dest_dir_path
            # if is_dir(dest_dir_path):
            #     dir_content = os.listdir(dest_dir_path)
            #     if not dir_content:
            #         return dest_dir_path
            #     if prepare_dest_dir(dest_dir_path):
            #         return dest_dir_path
            #     else:
            #         continue
            # else:
            #     print(Warning.ELEMENTS["invalid_input"].generate())
            #     continue
        else:
            # try:
            #     os.makedirs(dest_dir_path, exist_ok=True)
            #     return dest_dir_path
            # except OSError as e:
            #     print(f"Failed to create directory '{dest_dir_path}': {e}")
            print(Warning.ELEMENTS["invalid_input"].generate())
            continue

def prepare_dest_dir(path: str) -> bool:
    # Interact with user in case directory has files
    while True:
        try:
            permission = input(Prompt.ELEMENTS["clean"].generate(path=path))
        except KeyboardInterrupt:
            print()
            return False
        
        if permission == "y":
            try:
                clean_dir(path)
                return True
            except OSError as e:
                print(f"Failed to clean directory '{path}': {e}")
                continue
        elif permission == "n":
            return False
        else:
            print(Warning.ELEMENTS["invalid_input"].generate())
            continue

# Source directories for file processing
def get_input_data() -> pd.DataFrame: # 1st level
    while True:
        # Render menu
        print("\n".join([Header.ELEMENTS["src_dirs"].generate(), MenuLine.ELEMENTS["exit"].generate(), MenuLine.ELEMENTS["csv_load"].generate(), MenuLine.ELEMENTS["manual_load"].generate()]))
        # Request user input
        try:
            input_option = input(Prompt.ELEMENTS["base"].generate())
            input_option = lowercase_text(strip_text(input_option))
        except KeyboardInterrupt:
            print("Execution interrupted")
            return pd.DataFrame()
        
        # User input handling
        selected_dirs, in_action = upload_dirs(input_option)
        # Loop control parameters check
        match in_action:
            case MenuActions.INTERUPT:
                continue
            case MenuActions.FAILED:
                continue
            case MenuActions.SUCCESS:
                return selected_dirs

def upload_dirs(input_option: str) -> tuple[pd.DataFrame, StrEnum]: # 2nd level
    while True:
        # CSV LOAD
        if input_option == "csv":
            # Render menu
            print("\n".join([Header.ELEMENTS["csv_load"].generate(), MenuLine.ELEMENTS["cancel"].generate()]))
            # Request user input
            try:
                csv_path = input(Prompt.ELEMENTS["csv"].generate())
                csv_path = strip_text(csv_path)
                df = pd.read_csv(csv_path)
            except KeyboardInterrupt:
                print(f"CSV upload interrupted")
                return pd.DataFrame(), MenuActions.INTERUPT
            except (ValueError, FileNotFoundError, PermissionError, RuntimeError) as e:
                print(Warning.ELEMENTS["load_failed"].generate(option=input_option, e=e))
                continue
        # MANUAL LOAD
        elif input_option == "manual":
            # Render menu
            print("\n".join([Header.ELEMENTS["manual_load"].generate(), MenuLine.ELEMENTS["cancel"].generate()]))
            # Request user input
            try:
                input_dirs = []
                while True:
                    prompt_key = "manual" if not input_dirs else "manual_additional"
                    input_dir = input(Prompt.ELEMENTS[prompt_key].generate())
                    input_dir = strip_text(input_dir)
                    if input_dir == "stop":
                        break
                    input_dirs.append(input_dir)
                    df = pd.DataFrame(input_dirs, columns=["DirPath"])
            except KeyboardInterrupt:
                print(f"Manual upload interrupted")
                return pd.DataFrame(), MenuActions.INTERUPT
            except TypeError as e:
                print(Warning.ELEMENTS["load_failed"].generate(option=input_option, e=e))
                continue
        # INVALID INPUT
        else:
            print(Warning.ELEMENTS["invalid_input"].generate())
            return pd.DataFrame(), MenuActions.FAILED
        
        # Process input dataframe
        dir_data = user_input_pipeline().execute(df).sort_values("DirDepth", ascending=True)
        # Resolve parent-child relationship clash
        reload = False
        dir_data["IsSelected"] = True
        pending = list(dir_data.index)
        for pos, row_id in enumerate(pending):
            dir_path = dir_data.at[row_id, "DirPath"]
            dir_depth = dir_data.at[row_id, "DirDepth"]
            branch_depth_from_dir = dir_data.at[row_id, "BranchDepthFromDir"]
            # CLI element
            print("\n".join([Separator.DASH.repeat(100), Info.ELEMENTS["processing"].generate(dir_path=dir_path), Icon.DOWNARROW.repeat(3)]))
            # Get user input on required processing depth
            depth_input, in_action = set_processing_depth(branch_depth_from_dir)
            match in_action:
                case MenuActions.SKIP:
                    dir_data.at[row_id, "IsSelected"] = False
                    pending.remove(row_id)
                    continue
                case MenuActions.INTERUPT:
                    reload = True
                    break
                case MenuActions.SUCCESS:
                    dir_data.at[row_id, "UserInputDepth"] = depth_input
            # Check if child exist next to the
            for next_row_id in pending[pos+1:]:
                pending_child = dir_data.at[next_row_id, "DirPath"]
                pending_child_depth = dir_data.at[next_row_id, "DirDepth"]
                if is_parent(dir_path, pending_child):
                    dir_processing_depth = dir_depth + depth_input
                    if pending_child_depth <= dir_processing_depth:
                        # CLI element
                        print("\n".join([Separator.DASH.repeat(100), Info.ELEMENTS["skipped"].generate(path=pending_child)]))
                        dir_data.at[next_row_id, "IsSelected"] = False
                        pending.remove(next_row_id)
        if reload:
            continue
        
        selected_dirs = dir_data.loc[dir_data["IsSelected"]==True, ["DirPath", "UserInputDepth"]]

        if selected_dirs.empty:
            print(Warning.ELEMENTS["empty_input"].generate())
            continue
        
        return selected_dirs, MenuActions.SUCCESS

def set_processing_depth(branch_depth_from_dir: int) -> tuple[int, StrEnum]: # 3rd level
    while True:
        # Render menu
        depth_options = f"0-{branch_depth_from_dir}" if branch_depth_from_dir else "0"
        print(MenuLine.ELEMENTS["depth"].generate(depth_options=depth_options))
        # Request user input
        try:
            depth_input = input(Prompt.ELEMENTS["base"].generate()) # Your choice (leave empty to skip):
            depth_input = lowercase_text(strip_text(depth_input))
        except KeyboardInterrupt:
            print(f"Depth input interrupted")
            return -1, MenuActions.INTERUPT
        # Process input
        if depth_input == "":
            return -1, MenuActions.SKIP
        else:
            try:
                depth_input = int(depth_input)
                if 0 <= depth_input <= branch_depth_from_dir:
                    return depth_input, MenuActions.SUCCESS
                else:
                    print(Warning.ELEMENTS["invalid_input"].generate())
                    continue
            except ValueError:
                print(Warning.ELEMENTS["invalid_input"].generate())