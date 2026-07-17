from enum import StrEnum, auto
import pandas as pd
import os
from utils.path import is_parent, is_dir, clean_dir, is_file, is_not_dir, get_normalized_path, get_dir_depth, get_branch_depth
from utils.text import lowercase_text, strip_text

from cli.components import Header, MenuLine, Prompt, Warning, Info
from cli.tokens import Separator, Icon

from dataframe.pipeline import Pipeline, AssignTags, FilterCols, FilterRows, Compute, Transform
from dataframe.col_filter import KeywordFilter, NameFilter, TagFilter
from dataframe.processor import ElementProcessor, RowProcessor, ColProcessor
from dataframe.predicate import Condition, And

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
def get_dest_dir() -> str:
    while True:
        print(Header.get("dest_dir"))
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
                if prepare_dest_dir(dest_dir_path):
                    return dest_dir_path
                else:
                    continue
            elif is_file(dest_dir_path):
                print(Warning.get("invalid_input"))
                continue
            else:
                print(Warning.get("invalid_input"))
                continue
        else:
            try:
                os.makedirs(dest_dir_path, exist_ok=True)
                return dest_dir_path
            except Exception as e:
                print(e)
                print(Warning.get("invalid_input"))
                continue

def prepare_dest_dir(path: str) -> bool:
    # Interact with user in case directory has files
    while True:
        try:
            permission = input(Prompt.get("clean", path=path))
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
            print(Warning.get("invalid_input"))
            continue

# Source directories for file processing
def get_src_dirs() -> tuple: # 1st level
    while True:
        # Render menu
        print("\n".join([Header.get("src_dirs"), MenuLine.get("csv_load"), MenuLine.get("manual_load")]))
        # Request user input
        try:
            input_option = input(Prompt.get("base"))
            input_option = lowercase_text(strip_text(input_option))
        except KeyboardInterrupt:
            print()
            return ()
        
        # User input handling
        selected_dirs, in_action = load_dirs(input_option)
        # Loop control parameters check
        match in_action:
            case MenuActions.INTERUPT:
                continue
            case MenuActions.FAILED:
                continue
            case MenuActions.SUCCESS:
                print(Separator.DASH.repeat(100))
                return selected_dirs

def load_dirs(input_option: str): # 2nd level
    while True:
        # CSV LOAD
        if input_option == "csv":
            # Render menu
            print("\n".join([Header.get("csv_load"), MenuLine.get("cancel")]))
            # Request user input
            try:
                csv_path = input(Prompt.get("csv"))
                csv_path = strip_text(csv_path)
            except KeyboardInterrupt:
                print()
                return None, MenuActions.INTERUPT
            # Upload user input into dataframe
            try:
                df = pd.read_csv(csv_path)
            except (ValueError, FileNotFoundError, PermissionError, RuntimeError) as e:
                print(Warning.get("load_failed", e=e))
                continue
        # MANUAL LOAD
        elif input_option == "manual":
            # Render menu
            print("\n".join([Header.get("manual_load"), MenuLine.get("cancel")]))
            # Request user input
            try:
                input_dirs = {}
                while True:
                    prompt_key = "manual" if not input_dirs else "manual_additional"
                    input_dir = input(Prompt.get(prompt_key))
                    input_dir = strip_text(input_dir)
                    if input_dir == "stop":
                        break
                    input_dirs.setdefault("DirPath", []).append(input_dir)
            except KeyboardInterrupt:
                print()
                return None, MenuActions.INTERUPT
            # Upload user input into dataframe
            try:
                df = pd.DataFrame(input_dirs)
            except TypeError as e:
                print(Warning.get("load_failed", e=e))
                continue
        # INVALID INPUT
        else:
            print(Warning.get("invalid_input"))
            return None, MenuActions.FAILED
        # Process input dataframe
        pipeline = Pipeline(
            [
                Transform(ElementProcessor(get_normalized_path), NameFilter("DirPath")),
                Compute(ElementProcessor(is_not_dir), NameFilter("DirPath"), "isInvalid"),
                Compute(ColProcessor(pd.DataFrame.duplicated), NameFilter("DirPath"), "isDuplicate"),
                FilterRows(And([Condition("isInvalid", "eq", False), Condition("isDuplicate", "eq", False)])),
                Compute(ElementProcessor(get_dir_depth), NameFilter("DirPath"), "DirDepth"),
                Compute(ElementProcessor(get_branch_depth), NameFilter("DirPath"), "BranchDepth"),
                Compute(RowProcessor(lambda r: r["BranchDepth"] - r["DirDepth"]), NameFilter(["BranchDepth", "DirDepth"]), "BranchDepthFromDir"),
            ]
        )
        # Get data
        dir_data = pipeline.execute(df).sort_values("DirDepth", ascending=True).reset_index()
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
            print("\n".join([Separator.DASH.repeat(100), Info.get("processing", dir_path=dir_path), Icon.DOWNARROW.repeat(3)]))
            # Get user input on required processing depth
            depth_input, in_action = set_processing_depth(branch_depth_from_dir)
            match in_action:
                case MenuActions.SKIP:
                    continue
                case MenuActions.SKIP_ALL:
                    break
                case MenuActions.INTERUPT:
                    reload = True
                    break
                case MenuActions.SUCCESS:
                    dir_processing_depth = dir_depth + depth_input
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
                            print("\n".join([Separator.DASH.repeat(100), Info.get("skipped", dir_path=dir_path)]))
                            skip_ids.append(pending_dir_id)
            else:
                break
        
        if reload:
            continue
        
        if not selected_dirs:
            print(Warning.get("empty_input"))
            continue
        
        print("\n".join([Separator.DASH.repeat(100), Info.get("selected", dir_count=len(selected_dirs))]))

        return selected_dirs, MenuActions.SUCCESS

def set_processing_depth(branch_depth_from_dir: int): # 3rd level
    while True:
        # Render menu
        depth_options = f"0-{branch_depth_from_dir}" if branch_depth_from_dir else "0"
        print(MenuLine.get("depth", depth_options=depth_options))
        # Request user input
        try:
            depth_input = input(Prompt.get("base"))
            depth_input = lowercase_text(strip_text(depth_input))
        except KeyboardInterrupt:
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
                    print(Warning.get("invalid_input"))
                    continue
            except ValueError:
                print(Warning.get("invalid_input"))