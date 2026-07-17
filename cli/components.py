from dataclasses import dataclass, field
from cli.tokens import Emoji, Separator
from typing import Literal, ClassVar, get_args

@dataclass
class Template():
    construct: list = field(default_factory=list)

    # String items
    def start(self, value):
        self.construct.append(value)
        return self
    def separator(self, value):
        self.construct.append(value)
        return self
    def emoji(self, value):
        self.construct.append(value)
        return self
    def message(self, value):
        self.construct.append(value)
        return self
    
    # String items assembly
    def generate(self, **kwargs):
        return "".join(self.construct).format(**kwargs)

@dataclass
class Component():

    def __init_subclass__(cls):
        declared = set(get_args(cls.Options))
        defined = set(cls.TEMPLATES)
        if declared != defined:
            raise TypeError(f"Option drift in {cls.__name__}: {defined ^ declared}")

@dataclass
class Header(Component):

    START: ClassVar[str] = "\n"
    SEPARATOR: ClassVar[str] = Separator.DASH.repeat(40)
    WIDTH: ClassVar[int] = 20
    
    Options = Literal["dest_dir", "src_dirs", "csv_load", "manual_load", "depth"]
    TEMPLATES: ClassVar[dict[Options, Template]] = {
        "dest_dir":     Template().start(START).separator(SEPARATOR).message("Select Dest Dir".center(WIDTH)).separator(SEPARATOR),
        "src_dirs":     Template().start(START).separator(SEPARATOR).message("Select Src Dirs".center(WIDTH)).separator(SEPARATOR),
        "csv_load":     Template().start(START).separator(SEPARATOR).message("CSV load".center(WIDTH)).separator(SEPARATOR),
        "manual_load":  Template().start(START).separator(SEPARATOR).message("Manual load".center(WIDTH)).separator(SEPARATOR),
        "depth":        Template().start(START).separator(SEPARATOR).message("Depth".center(WIDTH)).separator(SEPARATOR),
    }

    @classmethod
    def get(cls, option: Options, **kwargs) -> str:
        return cls.TEMPLATES[option].generate(**kwargs)

@dataclass
class MenuLine(Component):

    START: ClassVar[str] = ""
    EMOJI:  ClassVar[str] = Emoji.KEYBOARD
    SEPARATOR: ClassVar[str] = Separator.SPACE

    Options = Literal["exit", "cancel", "restart", "skip", "skip_all", "csv_load", "manual_load", "manual_stop", "depth"]
    TEMPLATES: ClassVar[dict[Options, Template]] = {
        "exit":         Template().start(START).emoji(Emoji.CROSSMARK).separator(SEPARATOR).message("Press 'Ctrl+C' to suspend the script"),
        "cancel":       Template().start(START).emoji(Emoji.LEFTWARDARROW).separator(SEPARATOR).message("Press 'Ctrl+C' to cancel"),
        "restart":      Template().start(START).emoji(EMOJI).separator(SEPARATOR).message("Press 'Ctrl+C' to cancel current input and retry"),
        "skip":         Template().start(START).emoji(EMOJI).separator(SEPARATOR).message("Type 'skip' to skip current dir path"),
        "skip_all":     Template().start(START).emoji(EMOJI).separator(SEPARATOR).message("Type 'skipall' to skip the rest of dir path(s)"),
        "csv_load":     Template().start(START).emoji(EMOJI).separator(SEPARATOR).message("Type 'csv' to load dir path(s) from CSV"),
        "manual_load":  Template().start(START).emoji(EMOJI).separator(SEPARATOR).message("Type 'manual' to provide dir path(s) directly in CLI"),
        "manual_stop":  Template().start(START).emoji(EMOJI).separator(SEPARATOR).message("Type 'stop' to finish adding dir path(s)"),
        "depth":        Template().start(START).emoji(EMOJI).separator(SEPARATOR).message("Select 'depth level' from {depth_options}"),
    }

    @classmethod
    def get(cls, option: Options, **kwargs) -> str:
        return cls.TEMPLATES[option].generate(**kwargs)

@dataclass
class Prompt(Component):
    
    START: ClassVar[str] = ""
    EMOJI:  ClassVar[str] = Emoji.RIGHTARROW
    SEPARATOR: ClassVar[str] = Separator.SPACE

    Options = Literal["base", "clean", "csv", "manual", "manual_additional"]    
    TEMPLATES: ClassVar[dict[Options, Template]] = {
        "base":                 Template().start(START).emoji(EMOJI).separator(SEPARATOR).message("Provide your option: "),
        "clean":                Template().start(START).emoji(EMOJI).separator(SEPARATOR).message("Delete content from {path} permanently (y/n)? "),
        "csv":                  Template().start(START).emoji(EMOJI).separator(SEPARATOR).message("Enter link to CSV file: "),
        "manual":               Template().start(START).emoji(EMOJI).separator(SEPARATOR).message("Enter dir path: "),
        "manual_additional":    Template().start(START).emoji(EMOJI).separator(SEPARATOR).message("Add another one: "),
    }

    @classmethod
    def get(cls, option: Options, **kwargs) -> str:
        return cls.TEMPLATES[option].generate(**kwargs)

@dataclass
class Warning(Component):

    START: ClassVar[str] = ""
    EMOJI:  ClassVar[str] = Emoji.WARNINGSIGN
    SEPARATOR: ClassVar[str] = Separator.SPACE

    Options = Literal["invalid_input", "empty_input", "load_failed"]
    TEMPLATES: ClassVar[dict[Options, Template]] = {
        "invalid_input":    Template().start(START).emoji(EMOJI).separator(SEPARATOR).message("Invalid input"),
        "empty_input":      Template().start(START).emoji(EMOJI).separator(SEPARATOR).message("No dir path(s) to process"),
        "load_failed":      Template().start(START).emoji(EMOJI).separator(SEPARATOR).message("Load failed with the reason - {e}}"),
    }

    @classmethod
    def get(cls, option: Options, **kwargs) -> str:
        return cls.TEMPLATES[option].generate(**kwargs)

@dataclass
class Info(Component):
    
    START: ClassVar[str] = ""
    EMOJI:  ClassVar[str] = ""
    SEPARATOR: ClassVar[str] = Separator.SPACE
    
    Options = Literal["exit", "processing", "skipped", "selected"]
    TEMPLATES: ClassVar[dict[Options, Template]] = {
        "exit":         Template().start(START).emoji(Emoji.STOPSIGN).separator(SEPARATOR).message("[Terminated]"),
        "processing":   Template().start(START).emoji(Emoji.HOURGLASS).separator(SEPARATOR).message("[Processing] -----> {dir_path}"),
        "skipped":      Template().start(START).emoji(Emoji.GEAR).separator(SEPARATOR).message("[Skipped]    -----> {dir_path}"),
        "selected":     Template().start(START).emoji(Emoji.BULLSEYE).separator(SEPARATOR).message("[Selected]   -----> {dir_count} dirs"),
    }

    @classmethod
    def get(cls, option: Options, **kwargs) -> str:
        return cls.TEMPLATES[option].generate(**kwargs)