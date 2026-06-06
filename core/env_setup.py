from cli.assets import Icon
from cli.renderer import render_cli_object
from configs.cli_cfg import cli_objects
import os
from utils.path import clean_dir

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