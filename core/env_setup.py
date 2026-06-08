from configs.cli_cfg import cli_objects
from configs.env_cfg import PLATFORM_SYS
from cli.renderer import render_cli_object
import os
import sys
from utils.path import clean_dir, is_dir, is_file

def check_exiftool_path(path: str):
    if not is_file(path):
        print(f"❌ ExifTool executable has not been found at: {path}")
        return False
    
    if PLATFORM_SYS in ("Darwin", "Linux"):
        if not os.access(path, os.X_OK):
            print(f"❌ ExifTool found, but it is missing execution permissions. Run 'chmod +x {path}'")
            return False
    
    return True

def get_target_dir():
    while True:
        try:
            target_dir_path = input("Provide directory for organized files: ")
        except KeyboardInterrupt:
            print()
            print(render_cli_object(cli_objects["flow_marker"]))
            print(render_cli_object(cli_objects["info"], "exit"))
            sys.exit(0)
        
        if os.path.exists(target_dir_path):
            if is_dir(target_dir_path):
                break
            elif is_file(target_dir_path):
                print(render_cli_object(cli_objects["warning"], "invalid_input"))
                continue
            else:
                print(render_cli_object(cli_objects["warning"], "invalid_input"))
                continue
        else:
            try:
                os.makedirs(target_dir_path, exist_ok=True)
                break
            except Exception as e:
                print(e)
                print(render_cli_object(cli_objects["warning"], "invalid_input"))
                continue
    
    return target_dir_path

def prepare_target_dir(path: str) -> bool:

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
            print(render_cli_object(cli_objects["flow_marker"]))
            print("Done" if success else "Failed")
            return success
        elif permission == "n":
            print(render_cli_object(cli_objects["flow_marker"]))
            print(render_cli_object(cli_objects["info"], "exit"))
            return False
        else:
            print(render_cli_object(cli_objects["warning"], "invalid_input"))