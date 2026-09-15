from dataclasses import dataclass
from exiftool import ExifTool
import json
from typing import Iterator

def get_batches(files: list[str], batch_size: int) -> list[list[str]]:
    if batch_size is None or batch_size <= 0:
        return [files]
    return [files[i:i + batch_size] for i in range(0, len(files), batch_size)]

@dataclass
class Exif:
    path: str = None # default = PATH
    encoding: str = None # default = locale.getpreferredencoding()
    batch_size: int = None

    def _build_exif(self) -> ExifTool:
        if self.path:
            return ExifTool(encoding=self.encoding, executable=self.path)
        return ExifTool(encoding=self.encoding)

    def extract(self, files: list[str], args: list[str]) -> Iterator[dict]:
        with self._build_exif() as et:
            for batch in get_batches(files, self.batch_size):
                raw_output = et.execute(*args, *batch)
                yield from json.loads(raw_output)