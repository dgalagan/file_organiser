from dataclasses import dataclass, field
from dataframe.tag_store import TagStore

@dataclass
class Context:
    store: TagStore = field(default_factory=TagStore)