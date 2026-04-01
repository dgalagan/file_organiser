import os
os.environ["DISABLE_PANDERA_IMPORT_WARNING"] = "True"
import pandera as pa
import pandas as pd
from typing import Type, Union, Optional, Callable, List, Tuple
from utils.path import is_file, get_file_extension
from utils.text import lower_text

class EmptyDataError(Exception):
    pass

class DfProcessor:
    def __init__(self, schema: Type[pa.DataFrameModel], calc_logic: dict):
        # check if schema inheritce from pa.DataFrameModel
        if not issubclass(schema, pa.DataFrameModel):
            raise TypeError(f"{schema} must inherit from pandera.DataFrameModel")
        # Load schema
        self.schema = schema
        self.fields_dict = {col: col_attr.dtype.type for col, col_attr in schema.to_schema().columns.items()}
        self.calc_logic = calc_logic
        self.df = pd.DataFrame(columns=self.fields_dict.keys())
        self._active_mask = pd.Series(dtype=bool)
        self._col_status = {col: False for col in self.fields_dict.keys()}
    
    @property
    def _df(self):
        return self.schema.validate(self.df)
    
    @property
    def active_df(self):
        return self.schema.validate(self.df.loc[self._active_mask])
    
    def _apply_logic(self, src_cols: list, target_col: str, logic: Callable):
        self.df.loc[self._active_mask, target_col] = logic(self.df.loc[self._active_mask], src_cols)

    def load_list(self, items: list, col: str):
        if not items:
            raise EmptyDataError("No data to process")
        if not isinstance(items, list):
            raise TypeError("paths should be a list")
        # Load paths 
        self.df[col] = items
        # Set up active mask
        self._active_mask = pd.Series(True, index=self.df.index)
        return self
    
    def load_csv(self, path:str):
        if not is_file(path):
            raise FileNotFoundError(f"Path is not a file: {path}")
        file_extension = lower_text(get_file_extension(path))
        if not file_extension == ".csv":
            raise ValueError(f"File extension '{file_extension}' is not supported")

        raw_df = pd.read_csv(path)
        self.df = self.schema.validate(raw_df)
        self._active_mask = pd.Series(True, index=self.df.index)
        return self

    def transform(self, pipeline: List[Tuple[Union[str, List[str]], Optional[str]]]):
        for src_cols, target_col in pipeline:
            
            # Type check
            if not isinstance(src_cols, (str, list)):
                print(f"[Skipped] Invalid source: Expected 'str' or 'list', but got '{type(src_cols).__name__} ({src_cols})'")
                continue
            
            if target_col is not None and not isinstance(target_col, str):
                print(f"[Skipped] Invalid Target: Expected 'str' or 'None', but got '{type(target_col).__name__}' ({target_col})")
                continue

            sources = [src_cols] if isinstance(src_cols, str) else src_cols
            effective_target = target_col if target_col is not None else sources[0]

            # Skip if calculated
            if self._col_status.get(effective_target, False):
                continue
            
            # Check of sorce exist
            can_calc = True
            for s in sources:
                if s is None or s not in self.df.columns or self.df[s].isnull().all():
                    print(f"[Skipped] {effective_target}: Dependency '{s}' not met.")
                    can_calc = False
                    break
            
            # Logic lookup
            if can_calc:
                logic = self.calc_logic.get(effective_target)
                if logic:
                    self._apply_logic(sources, effective_target, logic)
                    self._col_status[effective_target] = True
                else:
                    print(f"[Skipped] {effective_target}: No logic defined in Registry.")
        return self 

    def filter(self, pipeline: List[Tuple[str, bool]]):
        for col, value_to_exlude in pipeline:
            if not self._col_status.get(col):
                print(f"[Filter Skipped] '{col}' has not been calculated/processed.")
                continue
            if not self.fields_dict[col] == bool:
                print(f"[Filter Skipped]: '{col}' is not boolean")
                continue
            self._active_mask &= (self.df[col] != value_to_exlude)
        return self
    
    def sort(self, col: str):
        self.df = self.df.sort_values(col, ascending=True)
        return self