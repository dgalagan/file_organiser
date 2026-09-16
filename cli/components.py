from dataclasses import dataclass, field
from cli.tokens import Emoji, Separator, Icon, Color
from typing import Literal, ClassVar, get_args

@dataclass
class Template():
    construct: list = field(default_factory=list)

    # String items
    def token(self, value):
        self.construct.append(value)
        return self
    def message(self, value):
        self.construct.append(value)
        return self
    def padding(self, num: int, align: Literal["<", ">", "^"] = "<"):
        to_collapse = []
        for item in list(self.construct):
            if isinstance(item, tuple):
                continue
            else:
                to_collapse.append(item)
                self.construct.remove(item)
        self.construct.append(("".join(to_collapse), num, align))
        return self

    # String items assembly
    def build(self, indent: int = 0, **kwargs):
        result = ""
        for item in self.construct:
            if isinstance(item, tuple):
                part, pad, align = item
                result += f"{part.format(**kwargs):{align}{pad}}"
            else:
                result += item
        return f"{indent * '  '}{result.format(**kwargs)}"

@dataclass
class Component():

    def __init_subclass__(cls):
        declared = set(get_args(cls.Options))
        defined = set(cls.ELEMENTS)
        if declared != defined:
            raise TypeError(f"Option drift in {cls.__name__}: {defined ^ declared}")

@dataclass
class Prompt(Component):

    ICON:  ClassVar[str] = f"{Color.CYAN}{Icon.GREATERTHAN}{Color.RESET}" #{5chars}{3chars}{4chars}
    SEPARATOR: ClassVar[str] = Separator.SPACE
    ANSI_LEN: ClassVar[int] = len(Color.CYAN + Color.RESET)

    Options = Literal["depth_input"]
    ELEMENTS: ClassVar[dict[Options, Template]] = {
        "depth_input": (
        Template()
        .token(ICON)
        .message(" {num:^5} {dir_path}")
        )
    }

@dataclass
class Errors(Component):

    ICON:  ClassVar[str] = f"{Color.RED}{Icon.CROSSMARK}{Color.RESET}" #{5chars}{3chars}{4chars}
    SEPARATOR: ClassVar[str] = Separator.SPACE
    ANSI_LEN: ClassVar[int] = len(Color.RED + Color.RESET)

    Options = Literal["empty", "exception", "unknown_value", "low_disk_space"]
    ELEMENTS: ClassVar[dict[Options, Template]] = {
        "empty":(
            Template()
            .token(ICON)
            .token(SEPARATOR)
            .message("{subject}: not found")
        ),
        "exception":(
             Template()
             .token(ICON)
             .token(SEPARATOR)
             .message("Failed {path}: [{errno}]")
        ),
        "unknown_value":(
            Template()
            .token(ICON)
            .token(SEPARATOR)
            .message("Unknown value: {received}. Expected: {expected}")
        ),
        "low_disk_space":(
            Template()
            .token(ICON)
            .token(SEPARATOR)
            .message("Not enough space to {op} files: need {required} GB, {free} GB free")
        )
    }

@dataclass
class Warnings(Component):

    ICON:  ClassVar[str] = f"{Color.YELLOW}{Icon.WARNING}{Color.RESET}"
    SEPARATOR: ClassVar[str] = Separator.SPACE
    ANSI_LEN: ClassVar[int] = len(Color.YELLOW + Color.RESET)

    Options = Literal["base", "invalid_input"]
    ELEMENTS: ClassVar[dict[Options, Template]] = {
        "base": (
            Template()
            .token(ICON)
            .token(SEPARATOR)
        ),
        "invalid_input": (
            Template()
            .token(ICON)
            .token(SEPARATOR)
            .message("Invalid input")
        ),
    }

@dataclass
class Notifications(Component):

    ICON:  ClassVar[str] = Icon.INFORMATION
    SEPARATOR: ClassVar[str] = Separator.SPACE
    
    Options = Literal["root_stat", "cache_load", "filtered", "op_done", "op_failed", "save_done"]
    ELEMENTS: ClassVar[dict[Options, Template]] = {
        "root_stat": (
            Template()
            .token(ICON)
            .message("{n:^15,}{dir_path}")
        ),
        "cache_load": (
            Template()
            .token(ICON)
            .token(SEPARATOR)
            .message("Metadata from cache")
            .padding(30)
            .message("- {n:>6,} of {n_total:>6,} files ({share:>6.1%})")
        ),
        "filtered": (
            Template()
            .token(ICON)
            .token(SEPARATOR)
            .message("Filtered")
            .padding(30)
            .message("- {n:>6,} of {n_total:>6,} files ({share:>6.1%})")
        ),
        "op_done": (
            Template()
            .token(f"{Color.GREEN}{Icon.CHECKMARK}{Color.RESET}")
            .token(SEPARATOR)
            .message(f"Done")
            .padding(42)
            .message("- {n:>6,} of {n_total:>6,} files ({share:>6.1%})")
        ),
        "op_failed": (
            Template()
            .token(f"{Color.RED}{Icon.CROSSMARK}{Color.RESET}")
            .token(SEPARATOR)
            .message("Failed")
            .padding(35)
            .message("- {n:>6,} of {n_total:>6,} files ({share:>6.1%})")
        ),
        "save_done": (
            Template()
            .message("Summary saved -> {path}")
        ),
    }

@dataclass
class TQDMDesc(Component):
    
    ICON:  ClassVar[str] = Icon.GREATERTHAN
    SEPARATOR: ClassVar[str] = Separator.SPACE

    Options = Literal["extract", "copy", "move", "remove"]
    ELEMENTS: ClassVar[dict[Options, Template]] = {
        "extract": (
            Template()
            .token(ICON)
            .token(SEPARATOR)
            .message("Extract files metadata")
            .padding(30)
        ),
        "copy": (
            Template()
            .token(ICON)
            .token(SEPARATOR)
            .message("Copy files")
            .padding(30)
        ),
        "move": (
            Template()
            .token(ICON)
            .token(SEPARATOR)
            .message("Move files")
            .padding(30)
        ),
        "remove": (
            Template()
            .token(ICON)
            .token(SEPARATOR)
            .message("Remove dirs")
            .padding(30)
        ),
    }