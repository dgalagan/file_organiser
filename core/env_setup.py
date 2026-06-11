import os
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