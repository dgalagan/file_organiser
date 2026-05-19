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
        self.cols_selection = []
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

    def load_excel(self, path:str):
        if not is_file(path):
            raise FileNotFoundError(f"Path is not a file: {path}")
        # file_extension = lower_text(get_file_extension(path))
        # if not file_extension == ".csv":
        #     raise ValueError(f"File extension '{file_extension}' is not supported")

        self.df = pd.read_excel(path)

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

    def _apply_func(self, subset, func, func_mode):
        
        # helper expects subset to be DataFrame object
        if not isinstance(subset, pd.DataFrame):
            print(f"Incompatible datatype received {type(subset)} expected 'pd.DataFrame'")
        
        # https://www.geeksforgeeks.org/python/difference-between-map-applymap-and-apply-methods-in-pandas/
        # the map() method is used to transform values by applying a function, dict, or Series mapping
        # works on both Series and DataFrame, making it a unified alternative for element-wise operations

        if func_mode == "element":              # -> receive DF will return DF
            return subset.map(func)
        elif func_mode == "row":                # -> receive DF most probably will return pd.Series
            return subset.apply(func, axis=1)
        elif func_mode == "col":                # -> receive DF most probably will return Scalar or pd.Series
            return subset.apply(func, axis=0)
        else:
            print(f"Incompatible func mode for pd.DataFrame {func}")

    def compute(self, func, func_mode="element", store_col = "", col_names = None, col_keywords = None, row_condition = None):
        
        # 1. Overrides selection attributes if criterion provided
        self.set_cols_selection(names=col_names, keywords=col_keywords).set_rows_selection(condition=row_condition)

        rows = self.rows_selection if self.rows_selection is not None else slice(None)
        cols = self.cols_selection if self.cols_selection is not None else self.df.columns.tolist()

        # 2. Execute based on mode
        subset = self.df.loc[rows, cols]
        result = self._apply_func(subset, func, func_mode)
        print(f"Subset shape is {subset.shape} and type {type(subset).__name__}, Result shape is {result.shape} and type {type(result).__name__}")

        # 3. Assign result
        if isinstance(result, pd.DataFrame):
            result = result.rename(columns={result.columns[0]: store_col})
            self.df = self.df.join(result, how="left")
        elif isinstance(result, pd.Series):
            self.df = self.df.join(result.rename(store_col), how="left")

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
        rows = self.rows_selection if self.rows_selection is not None else slice(None, None, None)
        cols = self.cols_selection if self.cols_selection is not None else self.df.columns.to_list()

        # 2. Check if backup required
        if backup:
            self._backup(cols)

        # 3. Execute based on mode
        subset = self.df.loc[rows, cols]
        result = self._apply_func(subset, func, func_mode)
        print(f"Subset shape is {subset.shape} and type {type(subset).__name__}, Result shape is {result.shape} and type {type(result).__name__}")

        # 4.1 Force columns to object so they can accept ANY type from function
        self.df[cols] = self.df[cols].astype(object)
        # 4.2 Assign result
        self.df.loc[rows, cols] = result
        # 4.3 Force Pandas to pick up most suitable datatype
        self.df[cols] = self.df[cols].convert_dtypes()

        # 5. Reset selection attributes
        if col_names is not None or col_keywords is not None:
            self.reset_cols_selection()
        if row_condition is not None:
            self.reset_rows_selection()

        return self

    def set_cols_selection(self, names: str | list[str] = None, keywords: str | list[str] = None):
        
        # Behavior: 
            # overrides,                 if at least one 'names' or 'keywords' is provided. 
            # preserves existing state,  if both are absent

        # 1a. Exit if nothing is provided
        if not names and not keywords:
            return self
        
        # 1b. Validate data type
        if not isinstance(names, (type(None), str, list)) or not isinstance(keywords, (type(None), str, list)):
            raise TypeError(f"Unsupported data type provided names {type(names)}, keywords {type(keywords)}")
        
        # 1c. Cast input variables
        names_set = set()
        keywords_set = set()
 
        if isinstance(names, str):
            names_set.add(names)
        elif isinstance(names, list):
            names_set.update(names)

        if isinstance(keywords, str):
            keywords_set.add(keywords)
        elif isinstance(keywords, list):
            keywords_set.update(keywords)
        
        # 1d. Cast df columns list
        all_cols_set = set(self.df.columns)

        # 2. Matching logic
        consolidated_cols = set()
        if names_set:
            names_set.intersection_update(all_cols_set) # select names available in df columns 
            consolidated_cols.update(names_set)
        if keywords_set:
            for col in all_cols_set:
                for keyword in keywords_set:
                    if keyword.lower() in col.lower():
                        consolidated_cols.add(col)
                        break
        # 3. Extend cols selection if exist
        if self.cols_selection:
            self.cols_selection.extend(consolidated_cols)
        else:
            self.cols_selection = list(consolidated_cols)
        
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
        self.cols_selection = []
        return self

    def sort(self, col: str):
        self.df = self.df.sort_values(col, ascending=True)
        return self