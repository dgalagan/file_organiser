from dataclasses import dataclass
from cli.content import Content
from typing import Literal

ALIGNERS = {
    "left": str.ljust,
    "right": str.rjust,
    "center": str.center
}

@dataclass(frozen=True)
class Cell:
    width: int
    align: Literal["left", "right", "center"] = "left"

    def render(self, content: str | Content) -> str:
        if len(content) > self.width:
            raise ValueError(f"Cell is width {self.width} is too short for {content}")
        invisible = content.invisible if isinstance(content, Content) else 0
        return ALIGNERS[self.align](str(content), self.width + invisible)

@dataclass(frozen=True)
class Layout:
    cells: list[Cell]
    separator: str = '|'
    left_border: str = '|'
    right_border: str = '|'
    
    def render(self, contents: list[str | Content]) -> str:
        return f"{self.left_border}{self.separator.join(cell.render(content) for cell, content in zip(self.cells, contents, strict=True))}{self.right_border}"