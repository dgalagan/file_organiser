from configs.cli_cfg import cli_objects
from cli.renderer import render_cli_object
import os
import sys
from utils.path import clean_dir, is_dir, is_file
import urllib.request
import io
import zipfile
import tarfile
import shutil


def download_tool(url: str, dest_dir: str) -> None:
    
    print(f"Downloading from {url}...")
    with urllib.request.urlopen(url) as response:
        data = io.BytesIO(response.read())
        final_url = response.url
    
    print(f"Extracting {final_url}...")
    if ".zip" in final_url:
        with zipfile.ZipFile(data) as z:
            root = z.namelist()[0].split("/")[0] 
            z.extractall(dest_dir)
        nested = os.path.join(dest_dir, root)
        for item in os.listdir(nested):
            shutil.move(os.path.join(nested, item), os.path.join(dest_dir, item))
        os.rmdir(nested)
    elif ".tar.gz" in final_url or ".tgz" in final_url:
        with tarfile.open(fileobj=data, mode="r:gz") as t:
            t.extractall(dest_dir)
    else:
        raise RuntimeError(f"Unsupported archive format: {url}")


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