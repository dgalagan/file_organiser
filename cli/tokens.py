from enum import Enum

class Token(str, Enum):

    def repeat_with_delim(self, count: int, delim: str = "") -> str:
        if not isinstance(count, int) or count < 1:
            raise ValueError(f"count must be an int > 0, got {type(count).__name__} with value {count}")
        if not isinstance(delim, str):
            raise ValueError(f"delimiter must be a string, got {type(delim).__name__}")
        return delim.join([self.value] * count)
    
    def repeat(self, count: int) -> str:
        if not isinstance(count, int) or count < 1:
            raise ValueError(f"count must be an int > 0, got {type(count).__name__} with value {count}")
        return self.value * count

class Separator(Token):
    SPACE = " "
    DASH = "-"
    COMMA = ","
    PIPE = "|"
    FORWARDSLASH = "/"
    BACKSLASH = "\\"

class Emoji(Token):
    KEYBOARD = '⌨️' + Separator.SPACE
    CHECKMARK = '✅'
    CROSSMARK = '❌'
    STOPSIGN = '🛑'
    WARNINGSIGN = '⚠️' + Separator.SPACE
    RIGHTARROW = '➡️' + Separator.SPACE
    DOWNARROW = "⬇️" + Separator.SPACE
    LEFTWARDARROW = '↩️' + Separator.SPACE
    RESTART = '🔄'
    INFORMATION = 'ℹ️' + Separator.SPACE
    BULLSEYE = '🎯'
    HOURGLASS = '⏳'
    CHEQUEREDFLAG = '🏁'
    GEAR = '⚙️' + Separator.SPACE

class Icon(Token):
    DOWNARROW = "↓"
