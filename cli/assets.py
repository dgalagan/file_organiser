class Template:
    SEP_MSG_SEP = "{start}{sep}{msg:^{width}}{sep}"
    ICON_SEP_MSG = "{start}{icon}{sep}{msg}"
class Token:
    def __init__(self, token: str):
        self.token = token
    def repeat_with_delim(self, count: int, delim: str = "") -> str:
        if not isinstance(count, int) or count < 1:
            raise ValueError(f"count must be an int > 0, got {type(count).__name__} with value {count}")
        if not isinstance(delim, str):
            raise ValueError(f"delimiter must be a string, got {type(delim).__name__}")
        return delim.join([self.token] * count)
    def repeat(self, count: int) -> str:
        return self.token * count
    def __str__(self):
        return self.token
class Delimiter:
    SPACE = Token(" ")
    DASH = Token("-")
    COMMA = Token(",")
    PIPE = Token("|")
    FORWARDSLASH = Token("/")
    BACKSLASH = Token("\\")
class Emoji:
    KEYBOARD = Token('⌨️')
    CHECKMARK = Token('✅')
    CROSSMARK = Token('❌')
    STOPSIGN = Token('🛑')
    WARNINGSIGN = Token('⚠️')
    RIGHTARROW = Token('➡️')
    DOWNARROW = Token("⬇️")
    LEFTWARDARROW = Token('↩️')
    RESTART = Token('🔄')
    INFORMATION = Token('ℹ️')
    BULLSEYE = Token('🎯')
    HOURGLASS = Token('⏳')
    CHEQUEREDFLAG = Token('🏁')
class Icon:
    DOWNARROW = Token("↓")