from cli.content import Colored, Content
from cli.layout import Cell, Layout
from cli.tokens import Icon, Ansi
import pandas as pd
from typing import Mapping

STEP_LAYOUT = Layout([Cell(20), Cell(20, "left"), Cell(20)])
COUNT_LAYOUT = Layout([Cell(20), Cell(5, "center"), Cell(20)])

def bool_summary(
        data: pd.Series,
        layout: Layout = STEP_LAYOUT,
        *,
        desc: str = "",
        icon: Mapping[bool, str | Colored] = {True: Colored(Icon.CHECKMARK, Ansi.GREEN), False: Colored(Icon.CROSSMARK, Ansi.RED)},
        label: Mapping[bool, str] = {True: "succeeded", False: "failed"},
        stat_format: str = "{n:,}/{total:,}"
) -> str:
    total = len(data)
    counts = data.value_counts()
    return '\n'.join(layout.render([desc, icon.get(flag, '') + ' ' + label.get(flag, ''), stat_format.format(n=n, total=total)]) for flag, n in counts.items())

def search_count(
        layout: Layout = COUNT_LAYOUT,
        *,
        desc: str = '',
        path: str = '',
        n: int = 0
) -> str:
    return layout.render([desc, str(n), path])

def tqdm_bar(cell_widths: tuple[int, int] = (20, 20)) -> str:
    return f"|{{desc:{cell_widths[0]}}}|{{bar:{cell_widths[1]}}}|{{n_fmt}}/{{total_fmt}} [{{elapsed}}<{{remaining}}, {{rate_fmt}}{{postfix}}]|"