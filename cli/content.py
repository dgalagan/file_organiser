from utils.strings import color

class Content:
    def __init__(self, *parts: "str | Content") -> None:
        self.parts = parts

    def __str__(self) -> str:
        return "".join(str(p) for p in self.parts)

    def __len__(self) -> int:
        return sum(len(p) for p in self.parts)

    def __add__(self, other: "str | Content") -> "Content":
        return Content(self, other)

    @property
    def invisible(self) -> int:
        return len(str(self)) - len(self)
    
class Colored(Content):
    def __init__(self, text: str, color_code: int) -> None:
        super().__init__(text)
        self.color_code = color_code

    def __str__(self) -> str:
        return color(super().__str__(), self.color_code)

    # def __len__(self) -> int:
    #     return len(super().__str__())