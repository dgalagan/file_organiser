import pandas as pd
from utils.path import is_file, get_file_extension
from utils.text import lowercase_text

# To improve:
# incorporate schema validation
# automate file loading process

class EmptyDataError(Exception):
    pass

class DfProcessor:
    def __init__(self, df: pd.DataFrame = pd.DataFrame()):
        self.df = df
        self.history = {}
        self.cols_selection = []
        self.rows_selection = None

    @property
    def active_selection(self):
        rows = self.rows_selection if self.rows_selection is not None else slice(None)
        cols = self.cols_selection if self.cols_selection else self.df.columns.tolist()
        return self.df.loc[rows, cols]

    def load_list(self, items: list, col: str):
        if not items:
            raise EmptyDataError("No data to process")
        if not isinstance(items, list):
            raise TypeError("paths should be a list")
        
        # Load paths
        self.df[col] = items

        return self
    
    def load_dict(self, items: dict | list[dict], orient: str | None = None, cols = None):
        if not items:
            raise EmptyDataError("No data to process")
        # if not isinstance(items, dict):
        #     raise TypeError("paths should be a list")
        
        # Load paths
        self.df = self.df.from_dict(items, orient=orient, columns=cols)

        return self
    
    def load_csv(self, path:str):
        if not is_file(path):
            raise FileNotFoundError(f"Path is not a file: {path}")
        file_extension = lowercase_text(get_file_extension(path))
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
        file_extension = lowercase_text(get_file_extension(path))
        if not file_extension == ".json":
            raise ValueError(f"File extension '{file_extension}' is not supported")

        self.df = pd.read_json(path, orient=orient)
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
        cols = self.cols_selection if self.cols_selection else self.df.columns.tolist()

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
        cols = self.cols_selection if self.cols_selection else self.df.columns.to_list()

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
        names_sort_map = None
        keywords_set = set()
 
        if isinstance(names, str):
            names_set.add(names)
        elif isinstance(names, list):
            names_set.update(names)
            names_sort_map = {name: idx for idx, name in enumerate(names)}

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
        # Cast from set to list
        consolidated_cols = list(consolidated_cols)
        
        # Apply original list order
        if names_sort_map is not None:
            for col_name, idx in names_sort_map.items():
                if col_name in consolidated_cols:
                    idx_in_cons = consolidated_cols.index(col_name)
                    col_to_move = consolidated_cols.pop(idx_in_cons)
                    consolidated_cols.insert(idx, col_to_move)
        
        # 3. Extend cols selection if exist
        self.cols_selection.extend(consolidated_cols)
        
        return self

    def set_rows_selection(self, condition = None):
        
        # Behavior: 
        # overrides,                 if 'condition' provided. 
        # preserves existing state,  if 'condition' absent
        
        # 1. Exit if nothing is provided
        if not condition:
            return self
        
        # 2. Update attribute
        if callable(condition):
            self.rows_selection = condition(self.df)

        return self

    def reset_rows_selection(self):
        self.rows_selection = None
        return self

    def reset_cols_selection(self):
        self.cols_selection = []
        return self

    def sort(self, col: str, ascending=True):
        self.df = self.df.sort_values(col, ascending=ascending)
        return self