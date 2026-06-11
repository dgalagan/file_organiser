import pandas as pd
import operator
import os

OPERATORS = {
    "AND": operator.and_,
    "OR": operator.or_,
    "==": operator.eq,
    "!=": operator.ne
}

class DfProcessor:
    
    def __init__(self, df: pd.DataFrame = pd.DataFrame()):
        self.df = df
        self.history = {}
        self.cols_filter = []
        self.rows_filter = pd.Series(True, index=df.index)

    @property
    def active_selection(self) -> pd.DataFrame:
        rows = self.rows_filter if not self.rows_filter.empty else pd.Series(True, index=self.df.index)
        cols = self.cols_filter if self.cols_filter else self.df.columns.tolist()
        return self.df.loc[rows, cols]
    
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

    def compute(self, func, func_mode, calc_col = "", use_cols = None, use_keywords = None, row_condition = None):

        # 1. Overrides selection attributes if criterion provided
        self.filter_cols(names=use_cols, keywords=use_keywords).filter_rows(cond=row_condition)

        rows = self.rows_filter if not self.rows_filter.empty else pd.Series(True, index=self.df.index)
        cols = self.cols_filter if self.cols_filter else self.df.columns.tolist()

        # 2. Execute based on mode
        subset = self.df.loc[rows, cols]
        result = self._apply_func(subset, func, func_mode)

        # 3. Assign result
        if isinstance(result, pd.DataFrame):
            result = result.rename(columns={result.columns[0]: calc_col})
            self.df = self.df.join(result, how="left")
        elif isinstance(result, pd.Series):
            self.df = self.df.join(result.rename(calc_col), how="left")

        # 4. Reset selection attributes
        if use_cols is not None or use_keywords is not None:
            self.reset_cols_filter()
        if row_condition is not None:
            self.reset_rows_filter()

        return self

    def transform(self, func, func_mode, use_cols = None, use_keywords = None, row_condition = None, backup = True):

        # 1. Overrides selection attributes if criterion provided
        self.filter_cols(names=use_cols, keywords=use_keywords).filter_rows(cond=row_condition)

        # 2. If row, col selection not exist, select all
        rows = self.rows_filter if not self.rows_filter.empty else pd.Series(True, index=self.df.index)
        cols = self.cols_filter if self.cols_filter else self.df.columns.to_list()

        # 2. Check if backup required
        if backup:
            self._backup(cols)
        
        # 3. Execute based on mode
        subset = self.df.loc[rows, cols]
        result = self._apply_func(subset, func, func_mode)

        # 4.1 Force columns to object so they can accept ANY type from function
        self.df[cols] = self.df[cols].astype(object)
        # 4.2 Assign result
        if len(cols) == 1:
            self.df.loc[rows, cols[0]] = result[cols[0]]
        else:
            self.df.loc[rows, cols] = result
        # 4.3 Force Pandas to pick up most suitable datatype
        self.df[cols] = self.df[cols].convert_dtypes()

        # 5. Reset selection attributes
        if use_cols is not None or use_keywords is not None:
            self.reset_cols_filter()
        if row_condition is not None:
            self.reset_rows_filter()

        return self

    def filter_cols(self, names: str | list[str] = None, keywords: str | list[str] = None):
        
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
        self.cols_filter.extend(consolidated_cols)
        
        return self

    def filter_rows(self, cond = None):

        if self.rows_filter.empty:
            self.rows_filter = pd.Series(True, index=self.df.index)
        
        if cond is not None:
            mask_junc_func = OPERATORS.get(cond.get("mask_junc"))
            comparator_func = OPERATORS.get(cond.get("comparator"))
            cond_col = cond.get("col")
            cond_val = cond.get("val")
            cond_mask =  comparator_func(self.df[cond_col], cond_val)
            self.rows_filter = mask_junc_func(self.rows_filter, cond_mask) 
        
        return self

    def reset_rows_filter(self):
        self.rows_filter = pd.Series(True, index=self.df.index)
        return self

    def reset_cols_filter(self):
        self.cols_filter = []
        return self
    
    def run_pipeline(self, pipeline: list[dict]):
        for step in pipeline:
            op = step.get("op")
            if op == "compute":
                func, mode = step.get("func")
                calc_col = step.get("calc_col")
                use_cols = step.get("use_cols")
                use_keywords = step.get("use_keywords")
                self.compute(func, func_mode=mode, calc_col=calc_col, use_cols=use_cols, use_keywords=use_keywords)
            elif op == "transform":
                func, mode = step.get("func")
                use_cols = step.get("use_cols")
                use_keywords = step.get("use_keywords")
                self.transform(func, func_mode=mode, use_cols=use_cols, use_keywords=use_keywords)
            elif op == "filter_rows":
                condition = step.get("cond")
                self.filter_rows(cond=condition)
            elif op == "filter_cols":
                cols = step.get("cols", None)
                keywords = step.get("keywords", None)
                self.filter_cols(names=cols, keywords=keywords)
            elif op == "save":
                components = step.get("file_components", {})
                dst_dir = components.pop("dir")
                name = components.pop("name")
                ext = components.pop("extension")
                full_name = name + '.' + ext
                path = os.path.join(dst_dir, full_name)
                DfWriter.write(df=self.active_selection, extension=ext, filepath=path, **components)
            else:
                raise NotImplementedError("DF operation not implemented")
        return self

class DfWriter:
    """Handles all export and persistence operations for DataFrames."""

    @staticmethod
    def write(df: pd.DataFrame, extension: str, filepath: str, **kwargs) -> None:
        
        if not filepath:
            raise ValueError("A filepath must be defined.")
        
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        if extension == "csv":
            df.to_csv(filepath, **kwargs)
        elif extension == "excel":
            df.to_excel(filepath, **kwargs)
        else:
            raise NotImplementedError(f"Unsupported export format: {format}")

        print(f"DF successfully saved to {filepath}")