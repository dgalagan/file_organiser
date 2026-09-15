from typing import Literal, get_args

# all_categories = csv_loader.load(EXTENSION_MAP_PATH)[Cols.FILE_CATEGORY].drop_duplicates().to_list()
Category = Literal[
    "Application", "Archive", "Audio", "Cache", "Config", "Database",
    "Data-Excel", "Data-PowerPoint", "Data-PowerBI", "Data-Other",
    "Documents-Text", "Documents-Word", "Documents-PDF", "Documents-BIB",
    "Ebook", "Email", "Image","Script", "SystemBackup", "Video", "Web", "Other"
]

class CategorySelection:
    def __init__(self):
        self._state = {c: True for c in get_args(Category)}
    # ui
    def toggle(self, category: str):
        self._state[category] = not self._state[category]
    def select_all(self):
        for c in self._state:
            self._state[c] = True
    def clear_all(self):
        for c in self._state:
            self._state[c] = False
    # script: bulk apply
    def include(self, incl: list[Category]):
        for c in incl:
            if c in self._state:
                self._state[c] = True
        return self
    def exclude(self, excl: list[Category]):
        for c in excl:
            if c in self._state:
                self._state[c] = False
    def allowlist(self, allowed: list[Category]):
        for c in self._state:
            self._state[c] = c in allowed
        return self
    def blocklist(self, blocked: list[Category]):
        for c in self._state:
            self._state[c] = c not in blocked
        return self
    # derive selection
    def get(self) -> list[str]:
        return [c for c, on in self._state.items() if on]