# def match_keywords(items: list[str], keywords: list[str]) -> Iterator[tuple[str, str]]:
#     for kw in keywords:
#         lk = kw.lower()
#         for i in items:
#             if lk in i.lower():
#                 yield kw, i

class TagStore:
    def __init__(self):
        self.tagged_items: dict[str, set] = {}

    @property
    def assigned_tags(self) -> set:
        if not self.tagged_items:
            return set()
        return set().union(*self.tagged_items.values())

    # def apply_config(self, tag_cfg: dict[str, dict[str, list[str]]], scope: list[str], default_tag: str = "build"):
    #     if not tag_cfg:
    #         return self
    #     for tag, specs in tag_cfg.items():
    #         for kw, item in match_keywords(scope, specs.get("keywords", [])):
    #             self.assign_tags(item, [default_tag, tag, kw])
    #         for item in specs.get("items", []):
    #             if item in scope:
    #                 self.assign_tags(item, [default_tag, tag])
    #             else: 
    #                 warnings.warn(f"[Skipped] '{item}' does not belong to match_scope")
    #     return self

    def assign_tag(self, item: str, tag: str):
        self.tagged_items.setdefault(item, set()).add(tag)
        return self

    def assign_tags(self, item: str, tags: list[str]):
        for tag in tags:
            self.assign_tag(item, tag)
        return self
    
    def rename_tag(self, old_tag: str, new_tag: str):
        if old_tag not in self.assigned_tags:
            raise ValueError(f"Provided tag {old_tag} does not exist")
        for tags in self.tagged_items.values():
            if old_tag in tags:
                tags.remove(old_tag)
                tags.add(new_tag)
        return self

    def find_items(self, tags: str | list[str]) -> list[str]:
        if isinstance(tags, str):
            tags = [tags]
        wanted = set(tags)
        return sorted([item for item, item_tags in self.tagged_items.items() if wanted.intersection(item_tags)])