from cli.assets import Delimiter, Emoji, Icon, Template
from cli.renderer import render_cli_object, render_cli_grouped_object
from core.df_processor import DfProcessor, EmptyDataError
from enum import StrEnum, auto
import os
from pandas.errors import ParserError
import pandas as pd
import sys
from utils.path import is_not_dir, is_parent, get_normalized_path, get_dir_depth, get_branch_depth, clean_dir
from utils.text import lowercase_text, strip_text
from configs.transformation_cfg import PIPELINE

# To improve:
# instead of os.walk(), create recursion based on os.scandir()
# self-reporting improvement
# review error handling
# clean up cli objects

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
def main_loop(cli_grouped_objects: dict, cli_objects: dict): # 1st level
    while True:
        # Render menu
        print(render_cli_grouped_object(cli_grouped_objects["main_menu"], cli_objects))
        # Request user input
        try:
            input_option = input(render_cli_object(cli_objects["prompt"]))
            input_option = lowercase_text(strip_text(input_option))
        except KeyboardInterrupt:
            print()
            print(Icon.DOWNARROW.repeat(3))
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
                print(Delimiter.DASH.repeat(80))
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
        dir_data = DfProcessor(df).run_pipeline(PIPELINE["path_input"]).active_selection.sort_values("DirDepth", ascending=True)
        # Resolve parent-child relationship clash
        reload = False
        processed_dirs = []
        for idx, row in dir_data.iterrows():
            dir_path = row["DirPath"]
            dir_depth = row["DirDepth"]
            branch_depth_from_dir = row["BranchDepthFromDir"]
            # CLI element
            print(Delimiter.DASH.repeat(80))
            print(render_cli_object(cli_objects["info"], "processing", dir_path=dir_path))
            print(Icon.DOWNARROW.repeat(3))
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
        
        print(Delimiter.DASH.repeat(80))
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

cli_objects = {  
    "header": {
        "template": Template.SEP_MSG_SEP,
        "defaults": {
            "start": "\n",
            "sep": Delimiter.DASH,
            "msg": "empty"
        },
        "elements":{
            "setup_env": {"sep": Delimiter.DASH.repeat(30), "msg": "Setup Env", "width": 20},
            "main": {"sep": Delimiter.DASH.repeat(30), "msg": "Input Methods", "width": 20},
            "csv_load": {"sep": Delimiter.DASH.repeat(30), "msg": "CSV load", "width": 20},
            "manual_load": {"sep": Delimiter.DASH.repeat(30), "msg": "Manual load", "width": 20},
            "depth": {"sep": Delimiter.DASH.repeat(30), "msg": "Depth", "width": 20}
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
            "setup_env": {"msg": "Delete content from {target_path} permanently? (y/n): "},
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
            "csv_load_failed": {"msg": "CSV path load failed with the reason - {error}"}, # CSV
            "manual_load_failed": {"msg": "Manual path load failed with the reason - {error}"}, # CSV
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
            "exit": {"msg": "Script terminated"},
            "processing": {"icon": Emoji.HOURGLASS, "sep": Delimiter.SPACE, "msg": "[Processing] -----> {dir_path}"},
            "added": {"msg": "[Added] -----> {dir_paths_count} dirs"},
            "skipped": {"msg": "[Skipped] -----> as already in scope"},
            "selected":{"icon": Emoji.BULLSEYE, "sep": Delimiter.SPACE.repeat(1), "msg": "[Selected] -----> {dir_paths_count} dirs"},
            "output_ready": {"icon": Emoji.CHEQUEREDFLAG, "msg": "Files aquisition completed"},
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

## Result
def setup_environment(path: str) -> bool:
    # If path does not exist, create it
    if not os.path.exists(path):
        try:
            os.makedirs(path)
            return True
        except Exception as e:
            print(f"Error creating directory: {e}")
            print(Icon.DOWNARROW.repeat(3))
            print(render_cli_object(cli_objects["info"], "exit"))
            return False

    # If directory empty, moving forward
    dir_content = os.listdir(path)
    if not dir_content:
        return True
    
    # Interact with user in case directory has files
    while True:
        print(render_cli_object(cli_objects["header"], element_name="setup_env"))
        permission = input(render_cli_object(cli_objects["prompt"], element_name="setup_env", target_path=path))
        if permission == "y":
            success = clean_dir(path)
            print(Icon.DOWNARROW.repeat(3))
            print("Done" if success else "Failed")
            return success
        elif permission == "n":
            print(Icon.DOWNARROW.repeat(3))
            print("Cancelled")
            print(Icon.DOWNARROW.repeat(3))
            print(render_cli_object(cli_objects["info"], "exit"))
            return False
        else:
            print(render_cli_object(cli_objects["warning"], "invalid_input"))
def get_user_input():
    return main_loop(cli_grouped_objects, cli_objects)