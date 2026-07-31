from dataclasses import dataclass, field
from dataframe.tag_store import TagStore
from core.transformation import DateParser
from typing import Any

@dataclass
class Context:
    store: TagStore = field(default_factory=TagStore)
    parser: DateParser | None = None
    geocoder: Any | None = None