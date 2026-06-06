from cli.renderer import render_cli_object, render_cli_grouped_object
from core.df_processor import DfProcessor, EmptyDataError
from enum import StrEnum, auto
import os
from pandas.errors import ParserError
import pandas as pd
import sys
from tqdm import tqdm
from utils.path import is_parent, iter_dir_hierarchy
from utils.text import lowercase_text, strip_text
from configs.transformation_cfg import PIPELINE

# To improve:
# instead of os.walk(), create recursion based on os.scandir()
# self-reporting improvement
# review error handling

# Actions
class MenuActions(StrEnum):
    EXIT = auto()
    INTERUPT = auto()
    SKIP = auto()
    SKIP_ALL = auto()
    SUCCESS = auto()
    FAILED = auto()
    RESTART = auto()

## Loops
def get_user_dirs(cli_grouped_objects: dict, cli_objects: dict): # 1st level
    while True:
        # Render menu
        print(render_cli_grouped_object(cli_grouped_objects["main_menu"], cli_objects))
        # Request user input
        try:
            input_option = input(render_cli_object(cli_objects["prompt"]))
            input_option = lowercase_text(strip_text(input_option))
        except KeyboardInterrupt:
            print()
            print(render_cli_object(cli_objects["flow_marker"]))
            print(render_cli_object(cli_objects["info"], "exit"))
            sys.exit(0)
        # User input handling
        selected_dirs, in_action = input_loop(cli_grouped_objects, cli_objects, input_option)
        # Loop control parameters check
        match in_action:
            case MenuActions.INTERUPT:
                continue
            case MenuActions.FAILED:
                continue
            case MenuActions.SUCCESS:
                print(render_cli_object(cli_objects["divider"]))
                return selected_dirs

def input_loop(cli_grouped_objects: dict, cli_objects: dict, input_option: str): # 2nd level
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
            except (ValueError, FileNotFoundError, PermissionError, EmptyDataError, ParserError, RuntimeError) as e:
                print(render_cli_object(cli_objects["warning"], "csv_load_failed", error=e))
                continue
        # Manual Load
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
            except (TypeError, EmptyDataError) as e:
                print(render_cli_object(cli_objects["warning"], "manual_load_failed", error=e))
                continue
        # Invalid Input
        else:
            print(render_cli_object(cli_objects["warning"], "invalid_input"))
            return None, MenuActions.FAILED
        # Get data
        dir_data = DfProcessor(df).run_pipeline(PIPELINE["user_dirs"]).active_selection.sort_values("DirDepth", ascending=True)
        # Resolve parent-child relationship clash
        reload = False
        processed_dirs = []
        for idx, row in dir_data.iterrows():
            dir_path = row["DirPath"]
            dir_depth = row["DirDepth"]
            branch_depth_from_dir = row["BranchDepthFromDir"]
            # CLI element
            print(render_cli_object(cli_objects["divider"]))
            print(render_cli_object(cli_objects["info"], "processing", dir_path=dir_path))
            print(render_cli_object(cli_objects["flow_marker"]))
            # Check if parents exist across processed dirs
            parents = [processed_dir for processed_dir in processed_dirs if is_parent(processed_dir, dir_path)]
            # Check if parent cover scope of dir in processig
            scope_overlap = False
            for parent in parents:
                defined_depth = dir_data.loc[dir_data["DirPath"]==parent, "ProcessingDepth"].item()
                if dir_depth <= defined_depth:
                    scope_overlap = True
                    break
            if scope_overlap:
                # hierarchy resolution, ask whether child should be processed separately, delete all related path from parent search, add new
                print(render_cli_object(cli_objects["info"], "skipped"))
                continue
            # Get user input on depth
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
                    dir_data.at[idx, "UserInputDepth"] = depth_input
                    dir_data.at[idx, "ProcessingDepth"] = dir_depth + depth_input
                    processed_dirs.append(dir_path)
        if reload:
            continue
        
        selected_dirs = list(dir_data[["DirPath", "ProcessingDepth"]].dropna().itertuples(index=False, name=None))
        
        if not selected_dirs:
            print(render_cli_object(cli_objects["warning"], "empty_input"))
            continue
        
        print(render_cli_object(cli_objects["divider"]))
        print(render_cli_object(cli_objects["info"], "selected", dir_paths_count=len(selected_dirs)))

        return selected_dirs, MenuActions.SUCCESS

def depth_loop(cli_grouped_objects: dict, cli_objects: dict, branch_depth_from_dir: int): # 3rd level
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

def collect_files_to_organise(input_dirs: list[str], cli_objects: dict = None):
    dirs = set()
    files = set()
    tqdm_desc = "Extracting files from input dirs:"
    divider = ""
    flow_marker = ""
    if cli_objects is not None:
        divider = render_cli_object(cli_objects["divider"])
        flow_marker = render_cli_object(cli_objects["flow_marker"])
    dirs_counter = 0
    files_counter = 0
    for input_dir, max_depth in tqdm(input_dirs, desc=f"{tqdm_desc:<40}", bar_format="{l_bar}{bar:60}{r_bar}{bar:-10b}"):
        for depth, dir, filenames in iter_dir_hierarchy(input_dir, max_depth):
            dirs.add(dir)
            dirs_counter += 1
            for filename in filenames:
                file_path = os.path.join(dir, filename)
                files.add(file_path)
                files_counter += 1
    print(flow_marker)
    print(f"{dirs_counter} dirs scanned, {files_counter} files extracted")
    print(divider)
    return dirs, files