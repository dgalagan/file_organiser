from dataclasses import dataclass, field
from dataframe.write import Writer
from dataframe.load import Loader
from exiftool import ExifTool
import json
import os
import pandas as pd
from typing import Iterator

def get_batches(files: list[str], batch_size: int) -> list[list[str]]:
    if batch_size <= 0:
        return [files]
    return [files[i:i + batch_size] for i in range(0, len(files), batch_size)]

@dataclass
class Cache:
    path: str
    clear_cache: bool
    writer: Writer
    loader: Loader

    def load(self) -> pd.DataFrame:
        if self.clear_cache or not os.path.exists(self.path):
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            self.writer.save(pd.DataFrame(), self.path)
        return self.loader.load(self.path)

    def save(self, df: pd.DataFrame) -> None:
        self.writer.save(df, self.path)

@dataclass
class Reference:
    path: str
    loader: Loader

    def load(self) -> pd.DataFrame:
        return self.loader.load(self.path)

@dataclass
class Exif:
    path: str
    batch_size: int
    args: list[str] = field(default_factory=list)

    def extract(self, files: list[str]) -> Iterator[dict]:
        with ExifTool(encoding="utf-8", executable=self.path) as et:
            for batch in get_batches(files, self.batch_size):
                raw_output = et.execute(*self.args, *batch)
                yield from json.loads(raw_output)

@dataclass
class Config:
    register: Cache
    data: Cache
    ref: Reference
    exif: Exif

# class PathComponents:
#     def __init__(self):
#         self.component_structure: dict[str, list[str]] = {
#             "General": ["DuplicateLabel", "FileCategory", "CreationYear", "FileExtension"],
#             "Image": ["CameraModel", "ImageCountry"],
#             "Data-Excel": ["WorksheetCount"]
#         }
#         self.component_aliases: dict[str, str] = {
#             "DuplicateLabel": "DuplicateLabel",
#             "FileCategory": "category",
#             "CreationYear": "Year",
#             "FileExtension": "File:FileTypeExtension",
#             "CameraModel": "EXIF:Model",
#             "ImageCountry": "Country",
#             "WorksheetCount": "CountWorksheets"
#         }

#     def reorder(self, group: str, component: str, new_position: int):
#         group_components = self.component_structure[group]
#         group_components[group].remove(component)
#         group_components[group].insert(new_position, component)
#         return self

#     def remove(self, group: str, component: str):
#         self.component_structure[group].remove(component)
#         return self

#     def resolve(self) -> list[str]:
#         return [self.component_aliases[component] for components in self.component_structure.values() for component in components]
