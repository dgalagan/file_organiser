from enum import StrEnum, IntEnum

class Ansi(IntEnum):
    CYAN = 6
    RED = 9
    GREEN = 10
    YELLOW = 11
    ISLAMIC_GREEN = 34
    MENTHOL = 157
    GRAY = 244

class Separator(StrEnum):
    SPACE =             ' '
    DASH =              '-'
    COMMA =             ','
    COLON =             ':'
    PIPE =              '|'
    FORWARDSLASH =      '/'
    BACKSLASH =         '\\'

class Icon:
    CHECKMARK =          "-v-"
    CROSSMARK =          "-x-"
    GREATERTHAN =        "[>]"
    INFORMATION =        "[i]"
    WARNING =            "[!]"
    DASH =               "[-]"

class Emoji:
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