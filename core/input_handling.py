from cli.assets import Delimiter, Emoji, Icon, Template
from cli.renderer import render_cli_object, render_cli_grouped_object
from core.df_processor import DfProcessor, EmptyDataError
from enum import StrEnum, auto
import os
os.environ["DISABLE_PANDERA_IMPORT_WARNING"] = "True"
import pandera as pa
from pandera.typing import Series
from pandas.errors import ParserError
from typing import Optional
from utils.path import is_not_dir, is_parent, get_normalized_path, get_dir_depth, get_branch_depth
from utils.text import lower_text, strip_text
import sys

# To improve:
# instead of os.walk(), create recursion based on os.scandir()
# self-reporting improvement
# review error handling
# clean up cli objects

# Schema
class DirPathSchema(pa.DataFrameModel):
    DirPath: Series[str] = pa.Field(coerce=True)
    isInvalid: Optional[Series[bool]] = pa.Field(nullable=True)
    isDuplicate: Optional[Series[bool]] = pa.Field(nullable=True)
    DirDepth: Optional[Series["Int64"]] = pa.Field(ge=0, nullable=True)
    BranchDepth: Optional[Series["Int64"]] = pa.Field(ge=0, nullable=True)
    BranchDepthFromDir: Optional[Series["Int64"]] = pa.Field(ge=0, nullable=True)
    UserInputDepth: Optional[Series["Int64"]] = pa.Field(ge=0, nullable=True)
    ProcessingDepth: Optional[Series["Int64"]] = pa.Field(ge=0, nullable=True)
    
    class Config:
        strict = True
        coerce = True

CALCULATION_LOGIC_REGISTRY = {
    DirPathSchema.DirPath: lambda df, src: df[src[0]].apply(get_normalized_path),
    DirPathSchema.isInvalid: lambda df, src: df[src[0]].apply(is_not_dir),
    DirPathSchema.isDuplicate: lambda df, src: df[src[0]].duplicated(),
    DirPathSchema.DirDepth: lambda df, src: df[src[0]].apply(get_dir_depth),
    DirPathSchema.BranchDepth: lambda df, src: df[src[0]].apply(get_branch_depth),
    DirPathSchema.BranchDepthFromDir: lambda df, src: df[src[0]] - df[src[1]]
}

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
            input_option = lower_text(strip_text(input_option))
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
                print(render_cli_object(cli_objects["info"], "output_ready"))
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
                processor = DfProcessor(DirPathSchema, CALCULATION_LOGIC_REGISTRY).load_csv(csv_path)
            except (ValueError, FileNotFoundError, PermissionError, EmptyDataError, ParserError, RuntimeError) as e:
                print(render_cli_object(cli_objects["warning"], "csv_load_failed", error=e))
                continue
        # Manual Load
        elif input_option == "manual":
            # Render menu
            print(render_cli_grouped_object(cli_grouped_objects["manual_menu"], cli_objects))
            # Request user input
            try:
                input_dirs = []
                while True:
                    prompt_key = "manual" if not input_dirs else "manual_additional"
                    input_dir = input(render_cli_object(cli_objects["prompt"], prompt_key))
                    input_dir = strip_text(input_dir)
                    if input_dir == "stop":
                        break
                    input_dirs.append(input_dir)
            except KeyboardInterrupt:
                print()
                return None, MenuActions.INTERUPT
            try:
                processor = DfProcessor(DirPathSchema, CALCULATION_LOGIC_REGISTRY).load_list(input_dirs, col=DirPathSchema.DirPath)
            except (TypeError, EmptyDataError) as e:
                print(render_cli_object(cli_objects["warning"], "manual_load_failed", error=e))
                continue
        # Invalid Input
        else:
            print(render_cli_object(cli_objects["warning"], "invalid_input"))
            return None, MenuActions.FAILED
        # Normalize and enrich loaded data
        print(Delimiter.DASH.repeat(80))
        (
            processor
            .transform(
                pipeline=[
                    (DirPathSchema.DirPath, None),
                    (DirPathSchema.DirPath, DirPathSchema.isInvalid),
                    (DirPathSchema.DirPath, DirPathSchema.isDuplicate)
                ]
            )
            .filter(
                pipeline=[
                    (DirPathSchema.isInvalid, True),
                    (DirPathSchema.isDuplicate, True)
                ]
            )
            .transform(
                pipeline=[
                    (DirPathSchema.DirPath, DirPathSchema.DirDepth),
                    (DirPathSchema.DirPath, DirPathSchema.BranchDepth),
                    ([DirPathSchema.BranchDepth, DirPathSchema.DirDepth], DirPathSchema.BranchDepthFromDir)
                ]
            )
            .sort(DirPathSchema.DirDepth)
        )
        # Get data
        dir_data = processor.active_df
        # Resolve parent-child relationship
        reload = False
        processed_dirs = []
        for idx, row in dir_data.iterrows():
            dir_path = row[DirPathSchema.DirPath]
            dir_depth = row[DirPathSchema.DirDepth]
            branch_depth_from_dir = row[DirPathSchema.BranchDepthFromDir]
            # CLI element
            print(Delimiter.DASH.repeat(80))
            print(render_cli_object(cli_objects["info"], "processing", dir_path=dir_path))
            print(Icon.DOWNARROW.repeat(3))
            # Check if parents exist across processed dirs
            parents = [processed_dir for processed_dir in processed_dirs if is_parent(processed_dir, dir_path)]
            # Check if parent cover scope of dir in processig
            scope_overlap = False
            for parent in parents:
                defined_depth = dir_data[DirPathSchema.ProcessingDepth].loc[dir_data[DirPathSchema.DirPath]==parent].item()
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
                    dir_data.at[idx, DirPathSchema.UserInputDepth] = depth_input
                    dir_data.at[idx, DirPathSchema.ProcessingDepth] = dir_depth + depth_input
                    processed_dirs.append(dir_path)
        if reload:
            continue
        
        selected_dirs = list(dir_data[[DirPathSchema.DirPath, DirPathSchema.ProcessingDepth]].dropna().itertuples(index=False, name=None))
        
        if not selected_dirs:
            print(render_cli_object(cli_objects["warning"], "empty_input"))
            continue
        
        print(Delimiter.DASH.repeat(80))
        print(render_cli_object(cli_objects["info"], "selected", dir_paths_count=len(selected_dirs)))
        print(Icon.DOWNARROW.repeat(3))

        return selected_dirs, MenuActions.SUCCESS
def depth_loop(cli_grouped_objects: dict, cli_objects: dict, branch_depth_from_dir: int): # 3rd level
    while True:
        # Render menu
        depth_options = f"0-{branch_depth_from_dir}" if branch_depth_from_dir else "0"
        print(render_cli_grouped_object(cli_grouped_objects["depth_menu"], cli_objects, depth_range=depth_options))
        # Request user input
        try:
            depth_input = input(render_cli_object(cli_objects["prompt"]))
            depth_input = lower_text(strip_text(depth_input))
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

## Result
def get_user_input():
    cli_objects = {  
        "header": {
            "template": Template.SEP_MSG_SEP,
            "defaults": {
                "start": "\n",
                "sep": Delimiter.DASH,
                "msg": "empty"
            },
            "elements":{
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
            "exit": {"msg": "Script terminated"}, # General
            "output_ready": {"icon": Emoji.CHEQUEREDFLAG, "msg": "Output ready"},
            "processing": {"icon": Emoji.HOURGLASS, "sep": Delimiter.SPACE, "msg": "[Processing] -----> {dir_path}"},
            "added": {"msg": "[Added] -----> {dir_paths_count} dirs"},
            "skipped": {"msg": "[Skipped] -----> as already in scope"},
            "selected":{"icon": Emoji.BULLSEYE.repeat(1), "msg": "[Selected] -----> {dir_paths_count} dirs"}
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
    return main_loop(cli_grouped_objects, cli_objects)