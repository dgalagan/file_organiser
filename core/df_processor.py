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

    def load_json(self, path: str, orient: str | None = None):
        if not is_file(path):
            raise FileNotFoundError(f"Path is not a file: {path}")
        file_extension = lower_text(get_file_extension(path))
        if not file_extension == ".json":
            raise ValueError(f"File extension '{file_extension}' is not supported")

        raw_df = pd.read_json(path, orient=orient)
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
    

class DfProcessorEXP:
    def __init__(self):
        self.df = pd.DataFrame()
        self.history = {}
        self.cols_selection = None
        self.rows_selection = None
    
    def init_schema(self, schema: pa.DataFrameModel | None = None):
        self.schema = schema
        schema_cols = schema.to_schema().columns.items()
        self.df.columns = schema_cols
        self.fields_dict = {col: col_attr.dtype.type for col, col_attr in schema.to_schema().columns.items()}
        return self

    def load_list(self, items: list, col: str):
        if not items:
            raise EmptyDataError("No data to process")
        if not isinstance(items, list):
            raise TypeError("paths should be a list")
        
        # Load paths
        self.df[col] = items

        return self
    
    def load_csv(self, path:str):
        if not is_file(path):
            raise FileNotFoundError(f"Path is not a file: {path}")
        file_extension = lower_text(get_file_extension(path))
        if not file_extension == ".csv":
            raise ValueError(f"File extension '{file_extension}' is not supported")

        self.df = pd.read_csv(path)

        return self

    def load_json(self, path: str, orient: str | None = None):
        if not is_file(path):
            raise FileNotFoundError(f"Path is not a file: {path}")
        file_extension = lower_text(get_file_extension(path))
        if not file_extension == ".json":
            raise ValueError(f"File extension '{file_extension}' is not supported")

        self.df = pd.read_json(path, orient=orient)
        return self

    def load_df(self, df: pd.DataFrame):
        self.df = df
        return self

    def set_index(self, col: str):
        self.df = self.df.set_index(col)
        return self

    def _backup(self, cols):
        for col in cols:
            if col not in self.history:
                self.history[col] = []
            self.history[col].append(self.df[col].copy)

    def compute(self, func, func_mode="element", store_col = "", col_names = None, col_keywords = None, row_condition = None):
        
        # 1. Overrides selection attributes if criterion provided
        self.set_cols_selection(names=col_names, keywords=col_keywords).set_rows_selection(condition=row_condition)

        rows = self.rows_selection if self.rows_selection is not None else slice(None)
        cols = self.cols_selection if self.cols_selection is not None else self.df.columns.tolist()

        # 2. Execute based on mode
        if func_mode == "element":
            result = self.df.loc[rows, cols].map(func)
        elif func_mode == "series":
            result = self.df.loc[rows, cols].apply(func)
        elif func_mode == "row":
            result = self.df.loc[rows, cols].apply(func, axis=1)
        
        # 3. Assign result
        self.df.loc[rows, store_col] = result.values

        # 4. Reset selection attributes
        if col_names is not None or col_keywords is not None:
            self.reset_cols_selection()
        if row_condition is not None:
            self.reset_rows_selection()

        return self

    def transform(self, func, func_mode = "element", backup = True, *, col_names = None, col_keywords = None, row_condition = None):

        # 1. Overrides selection attributes if criterion provided
        self.set_cols_selection(names=col_names, keywords=col_keywords).set_rows_selection(condition=row_condition)

        # 2. If row, col selection not exist, select all
        rows = self.rows_selection if self.rows_selection is not None else slice(None)
        cols = self.cols_selection if self.cols_selection is not None else self.df.columns.tolist()

        # 2. Check if backup required
        if backup:
            self._backup(cols)

        # 3. Execute based on mode
        if func_mode == "element":
            result = self.df.loc[rows, cols].map(func)
        elif func_mode == "series":
            result = self.df.loc[rows, cols].apply(func)
        elif func_mode == "row":
            result = self.df.loc[rows, cols].apply(func, axis=1)

        # 4.1 Force columns to object so they can accept ANY type from function
        self.df[cols] = self.df[cols].astype(object)
        # 4.2 Assign result
        self.df.loc[rows, cols] = result.values
        # 4.3 Force Pandas to pick up most suitable datatype
        self.df[cols] = self.df[cols].convert_dtypes()

        # 5. Reset selection attributes
        if col_names is not None or col_keywords is not None:
            self.reset_cols_selection()
        if row_condition is not None:
            self.reset_rows_selection()

        return self

    def set_cols_selection(self, names: list[str] | None = None, keywords: list[str] | None = None):
        
        # Behavior: 
            # overrides,                 if at least one 'names' or 'keywords' is provided. 
            # preserves existing state,  if both are absent

        # 1a. Exit if nothing is provided
        if not names and not keywords:
            return self
        
        # 1b. Init search criterion
        names = names or []
        keywords = keywords or []
        
        # 2. Keyword matching logic
        cols_by_keyword = set()
        if keywords:
            keywords_lower = [keyword.lower() for keyword in keywords]
            cols_lower = [col.lower() for col in self.df.columns]
            for id, col in enumerate(cols_lower):
                for keyword in keywords_lower:
                    if keyword in col:
                        cols_by_keyword.add(self.df.columns[id])
                        break
        
        # 3. Update attribute
        self.cols_selection = list(cols_by_keyword | set(names))
        
        return self

    def set_rows_selection(self, condition = None):
        
        # Behavior: 
        # overrides,                 if 'condition' provided. 
        # preserves existing state,  if 'condition' absent
        
        # 1. Exit if nothing is provided
        if not condition:
            return self
        
        # 2. Update attribute
        self.rows_selection = condition

        return self

    def reset_rows_selection(self):
        self.rows_selection = None
        return self

    def reset_cols_selection(self):
        self.cols_selection = None
        return self

    def sort(self, col: str):
        self.df = self.df.sort_values(col, ascending=True)
        return self