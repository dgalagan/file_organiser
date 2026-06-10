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
            target_dir_path = input("➡️  Provide empty directory for organized files: ")
        except KeyboardInterrupt:
            print()
            print(render_cli_object(cli_objects["flow_marker"]))
            print(render_cli_object(cli_objects["info"], "exit"))
            sys.exit(0)
        
        if os.path.exists(target_dir_path):
            if is_dir(target_dir_path):
                dir_content = os.listdir(target_dir_path)
                if not dir_content:
                    break
                if prepare_target_dir(target_dir_path):
                    print(render_cli_object(cli_objects["flow_marker"]))
                    print("Done")
                    break
                else:
                    print(render_cli_object(cli_objects["flow_marker"]))
                    continue
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
    # Interact with user in case directory has files
    while True:
        permission = input(render_cli_object(cli_objects["prompt"], element_name="setup_env", target_path=path))
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