from dataclasses import dataclass, field
from cli.tokens import Emoji, Separator, Icon
from typing import Literal, ClassVar, get_args

GREEN = "\033[32m"
RED = "\033[31m"
CYAN = "\033[36m"
GREY  = "\033[90m"
RESET = "\033[0m"

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
    def build(self, **kwargs):
        result = ""
        for item in self.construct:
            if isinstance(item, tuple):
                part, pad, align = item
                result += f"{part.format(**kwargs):{align}{pad}}"
            else:
                result += item
        return result.format(**kwargs)

@dataclass
class Component():

    def __init_subclass__(cls):
        declared = set(get_args(cls.Options))
        defined = set(cls.ELEMENTS)
        if declared != defined:
            raise TypeError(f"Option drift in {cls.__name__}: {defined ^ declared}")

@dataclass
class Prompt(Component):

    START: ClassVar[str] = ""
    EMOJI:  ClassVar[str] = Icon.GREATERTHAN
    SEPARATOR: ClassVar[str] = Separator.SPACE

    Options = Literal["depth_input"]
    ELEMENTS: ClassVar[dict[Options, Template]] = {
        "depth_input": (
        Template()
        # .message(f"{CYAN}{EMOJI}{RESET} {{dir_path}} {CYAN}({{num}}){RESET}")
        # .message(f"{CYAN}{EMOJI} {{num}}{RESET} {{dir_path}}")
        .message(f"{CYAN}{EMOJI} {{num}}{RESET}")
        .padding(18)
        .message("{dir_path}")
        )
    }

@dataclass
class Errors(Component):

    START: ClassVar[str] = ""
    EMOJI:  ClassVar[str] = Emoji.CROSSMARK
    SEPARATOR: ClassVar[str] = Separator.SPACE 

    Options = Literal["empty_input", "exception", "unknown_value", "low_disk_space"]
    ELEMENTS: ClassVar[dict[Options, Template]] = {
        "empty_input":(
            Template()
            .token(EMOJI)
            .token(SEPARATOR)
            .message("No {subject} available")
        ),
        "exception":(
             Template()
             .token(EMOJI)
             .token(SEPARATOR)
             .message("Error while {op}: {e:.30}")
        ),
        "unknown_value":(
            Template()
            .token(EMOJI)
            .token(SEPARATOR)
            .message("Unknown value: {received}. Expected: {expected}")
        ),
        "low_disk_space":(
            Template()
            .token(EMOJI)
            .token(SEPARATOR)
            .message("Not enough space to {op} files: need {required} GB, {free} GB free")
        )
    }

@dataclass
class Warnings(Component):

    START: ClassVar[str] = ""
    EMOJI:  ClassVar[str] = Emoji.WARNINGSIGN
    SEPARATOR: ClassVar[str] = Separator.SPACE

    Options = Literal["invalid_input", "not_found"]
    ELEMENTS: ClassVar[dict[Options, Template]] = {
        "invalid_input": (
            Template()
            .token(EMOJI)
            .token(SEPARATOR)
            .message("Invalid input")
        ),
        "not_found": (
            Template()
            .token(EMOJI)
            .token(SEPARATOR)
            .message("Columns not found")
            .padding(31)
            .message("- {cols}")
        )
    }

@dataclass
class Notifications(Component):
    
    START: ClassVar[str] = ""
    EMOJI:  ClassVar[str] = ""
    SEPARATOR: ClassVar[str] = Separator.SPACE
    
    Options = Literal["root_stat", "cache_load", "filtered", "op_done", "op_failed", "save_done", "save_failed"]
    ELEMENTS: ClassVar[dict[Options, Template]] = {
        "root_stat": (
            Template()
            .token(Icon.INFORMATION)
            .token(SEPARATOR)
            .message("{n:>9,} | {dir_path}")
        ),
        "cache_load": (
            Template()
            .token(Icon.INFORMATION)
            .token(SEPARATOR)
            .message("Metadata from cache")
            .padding(30)
            .message("- {n:>6,} of {n_total:>6,} files ({share:>6.1%})")
        ),
        "filtered": (
            Template()
            .token(Icon.INFORMATION)
            .token(SEPARATOR)
            .message("Filtered")
            .padding(30)
            .message("- {n:>6,} of {n_total:>6,} files ({share:>6.1%})")
        ),
        "op_done": (
            Template()
            .token(f"{GREEN}{Icon.CHECKMARK}{RESET}")
            .token(SEPARATOR)
            .message(f"Done")
            .padding(35)
            .message("- {n:>6,} of {n_total:>6,} files ({share:>6.1%})")
        ),
        "op_failed": (
            Template()
            .token(f"{RED}{Icon.CROSSMARK}{RESET}")
            .token(SEPARATOR)
            .message("Failed")
            .padding(35)
            .message("- {n:>6,} of {n_total:>6,} files ({share:>6.1%})")
        ),
        "save_done": (
            Template()
            .token(f"{GREEN}{Icon.CHECKMARK}{RESET}")
            .token(SEPARATOR)
            .padding(22)
            .message("| {path}")
        ),
        "save_failed": (
            Template()
            .token(f"{RED}{Icon.CROSSMARK}{RESET}")
            .token(SEPARATOR)
            .message("{reason:>9} | {path}")
        ),
    }

@dataclass
class TQDMDesc(Component):
    
    EMOJI:  ClassVar[str] = Icon.GREATERTHAN
    SEPARATOR: ClassVar[str] = Separator.SPACE

    Options = Literal["extract", "copy", "move", "remove"]
    ELEMENTS: ClassVar[dict[Options, Template]] = {
        "extract": (
            Template()
            .token(EMOJI)
            .token(SEPARATOR)
            .message("Extract files metadata")
            .padding(30)
        ),
        "copy": (
            Template()
            .token(EMOJI)
            .token(SEPARATOR)
            .message("Copy files")
            .padding(30)
        ),
        "move": (
            Template()
            .token(EMOJI)
            .token(SEPARATOR)
            .message("Move files")
            .padding(30)
        ),
        "remove": (
            Template()
            .token(EMOJI)
            .token(SEPARATOR)
            .message("Remove dirs")
            .padding(30)
        ),
    }