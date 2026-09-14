from enum import Enum

GREEN = "\033[32m"
RED = "\033[31m"
CYAN = "\033[36m"
RESET = "\033[0m"

class Token:

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

    def get(self):
        return self.value

class Separator(Token):
    SPACE =             ' '
    DASH =              '-'
    COMMA =             ','
    COLON =             ':'
    PIPE =              '|'
    FORWARDSLASH =      '/'
    BACKSLASH =         '\\'

class Emoji(Token):
    KEYBOARD =          '⌨️' + Separator.SPACE
    SCISSORS =          '✂️' + Separator.SPACE
    GEAR =              '⚙️' + Separator.SPACE
    WARNINGSIGN =       '⚠️' + Separator.SPACE
    RIGHTARROW =        '➡️' + Separator.SPACE
    DOWNARROW =         '⬇️' + Separator.SPACE
    LEFTWARDARROW =     '↩️' + Separator.SPACE
    INFORMATION =       'ℹ️' + Separator.SPACE
    NEXTTRACK =         '⏭️' + Separator.SPACE
    RESTART =           '🔄'
    CHECKMARK =         '✅'
    CROSSMARK =         '❌'
    STOPSIGN =          '🛑'
    BARCHART =          '📊'
    MAGNIFYINGGLASS =   '🔎'
    FLOPPYDISK =        '💾'
    INBOXTRAY =         '📥'
    BULLSEYE =          '🎯'
    HOURGLASS =         '⏳'
    CHEQUEREDFLAG =     '🏁'
    FILEFOLDER =        '📁'
    OPENFILEFOLDER =    '📂'

class Icon(Token):
    CHECKMARK =          "[✓]"
    CROSSMARK =          "[✗]"
    INPUT =              "-->"
    GREATERTHAN =        "[>]"
    INFORMATION =        "[i]"
    
