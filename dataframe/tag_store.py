class TagStore:
    def __init__(self):
        self.tagged_items: dict[str, set] = {}

    @property
    def assigned_tags(self) -> set:
        if not self.tagged_items:
            return set()
        return set().union(*self.tagged_items.values())

    def assign_tags(self, item: str, tags: list[str] | str):
        if isinstance(tags, str):
            tags = [tags]
        for tag in tags:
            self.tagged_items.setdefault(item, set()).add(tag)
        return self
    
    def rename_tag(self, old_tag: str, new_tag: str):
        if old_tag not in self.assigned_tags:
            raise ValueError(f"Provided tag {old_tag} does not exist")
        for tags in self.tagged_items.values():
            if old_tag in tags:
                tags.remove(old_tag)
                tags.add(new_tag)
        return self

    def find_items(self, tags: list[str] | str) -> list[str]:
        if isinstance(tags, str):
            tags = [tags]
        wanted = set(tags)
        return sorted([item for item, item_tags in self.tagged_items.items() if wanted.intersection(item_tags)])