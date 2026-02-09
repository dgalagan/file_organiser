import os
import sys
import pandas as pd
from itertools import combinations, product
from enum import IntEnum

def get_data_input_option():

    # Request user to select data provision options
    input_option = input(
        "Print 'csv' to load folder path(s) from CSV\n" \
        "Print 'manual' to provide folder path(s) manually\n" \
        "Print 'close' to suspend the script\n"
        "Enter your option: "
        ).strip().lower()
    
    return input_option

def is_file(path):
    return os.path.isfile(path)

def is_dir(path):
    return os.path.isdir(path)

def get_file_extension(path):
    _, file_extension = os.path.splitext(path)
    return file_extension

def get_file_basename(path):
    file_basename, _ = os.path.splitext(path)
    return file_basename

def get_child_dir(path_pair):
    path_a, path_b = path_pair
    a_abs = os.path.abspath(path_a)
    b_abs = os.path.abspath(path_b)

    #check if a child of b
    if os.path.commonpath([b_abs])==os.path.commonpath([b_abs, a_abs]):
        return path_a
    elif os.path.commonpath([a_abs])==os.path.commonpath([a_abs, b_abs]):
        return path_b
    else:
        return False

def open_csv(csv_path):
    
    # Check whether provided link is file, otherwise continue
    if not is_file(csv_input):
        print(f"\nProvided path {csv_input} is not a file\n")
        return None
    
    # Check whether file extension == 'csv', otherwise continu
    file_extension = get_file_extension(csv_input)
    if not file_extension == '.csv':
        print(f"\nProvided file extension is wrong {file_extension}\n")
        return None

    # Open csv file as dataframe, otherwise continue
    try:
        csv_data = pd.read_csv(csv_path)
        return csv_data
    except:
        print("\nProvided csv file empty or corrupted and could not be opened\n")
        return None
    
def validate_df_cols(df_cols, required_cols):
    # Check whether required columns are available
    if required_cols > df_cols:
        missing_cols = required_cols - df_cols
        print(f"\nRequired columns {missing_cols} are missing\n")
        return False
    elif required_cols < df_cols:
        additional_cols = df_cols - required_cols
        print(f"\nNeeded columns {required_cols} identified\n")
        print(f"\nAdditional columns {additional_cols} identified\n")
        return True
    else:
        print(f"\nNeeded columns {df_cols} identified\n")
        return True

def remove_duplicates(df, column=None):

    # Check whether column provided
    if column is None:
        print(f"Please provide column to remove dupliates from")
        return None
    
    df_normalized = df.drop_duplicates(
            column, 
            inplace=False
            )
    return df_normalized  

def filter_df(df, condition):
    filtered_df = df[condition]
    
    if filtered_df.empty:
        print("Valid entries are absent\n")
        return None
    else:
        return filtered_df

def parse_path(path, separator=None):
    
    if separator is None:
        return path
    
    return path.split(separator)


class ProcessingDepth(IntEnum):
    DIRECT_SUB = 0
    FULL_HIERARCHY = 1

FOLDER_PATH_COL = "FolderPath"
PROCESSING_DEPTH_COL = "ProcessingDepth"
REQUIRED_COLS = [FOLDER_PATH_COL, PROCESSING_DEPTH_COL]

FOLDER_PATH_TEST_COL = "FolderPathTest"
PROCESSING_DEPTH_TEST_COL = "ProcessingDepthTest"
TEST_COLS = [FOLDER_PATH_TEST_COL, PROCESSING_DEPTH_TEST_COL]




if __name__ == "__main__":
    
    folder_scope = {ProcessingDepth.DIRECT_SUB:[], ProcessingDepth.FULL_HIERARCHY:[]}

    exit_main_loop = False
    exit_condition = False

    while True:
        
        # Main loop exit condition
        if exit_main_loop:
            break

        if exit_condition:
            break

        # Request user to select data provision options
        main_menu_input = input(
            "\n----Main menu----\n"
            "Print 'csv' to load folder path(s) from CSV\n"
            "Print 'manual' to provide folder path(s) manually\n"
            "Print 'close' to suspend the script\n"
            "Select your option: "
        ).strip().lower()

        # Exit options
        if main_menu_input == 'close':
            exit_main_loop = True
            break
        
        # User input processing
        if main_menu_input == 'csv':
            while True:
                # Request user to provide link to csv file
                csv_menu_input = input(
                    "\n----CSV menu----\n"
                    "Provide link to csv file\n"
                    "Print 'esc' to return to previous menu\n"
                    "Print 'close' to suspend the script\n"
                    "Enter your option: "
                )
                if csv_menu_input == "esc":
                    break
                elif csv_menu_input == "close":
                    exit_main_loop = True
                    break
                
                # Open CSV file as dataframe
                csv_data = open_csv(csv_menu_input)
                if csv_data is None:
                    continue
                
                # Validate CSV columns
                csv_cols = {*csv_data}
                
                if not validate_df_cols(set(csv_cols), set(REQUIRED_COLS)):
                    continue
                
                # Validate processing depth
                csv_data[PROCESSING_DEPTH_TEST_COL] = csv_data[PROCESSING_DEPTH_COL].apply(
                    lambda x: True if x in [ProcessingDepth.DIRECT_SUB, ProcessingDepth.FULL_HIERARCHY] else False
                    )
                csv_data[FOLDER_PATH_TEST_COL] = csv_data[FOLDER_PATH_COL].apply(
                    lambda x: True if is_dir(x) else False
                    )
                
                # Normalize CSV data 
                normalized_csv_data = remove_duplicates(csv_data, column=FOLDER_PATH_COL)
                
                if normalized_csv_data is None:
                    continue
                
                # Select valid entries
                condition = (normalized_csv_data[FOLDER_PATH_TEST_COL] == True) & (normalized_csv_data[PROCESSING_DEPTH_TEST_COL] == True)
                filtered_csv_data = filter_df(normalized_csv_data, condition)
                
                if filtered_csv_data is None:
                    continue
                
                # Transform valid data and convert it into ditionary 
                transformed_data = pd.pivot_table(
                    filtered_csv_data,
                    index=PROCESSING_DEPTH_COL,
                    values=FOLDER_PATH_COL,
                    aggfunc=lambda x: list(x)
                )
                
                # Update input dictionary
                folder_scope = transformed_data.to_dict()[FOLDER_PATH_COL]
                
                # Exit condition check
                if folder_scope[ProcessingDepth.DIRECT_SUB] or folder_scope[ProcessingDepth.FULL_HIERARCHY]:
                    exit_condition = True
                    break
                else:
                    print(f"\nNo folder paths to process {folder_scope}")
                    exit_main_loop = True
                    break

        elif main_menu_input == 'manual':
            while True:
                manual_menu_input = input(
                    "\n----Manual menu----\n"
                    "Print one or several folder path(s) separated by , without spaces\n"
                    "Print 'esc' to return to previous menu\n"
                    "Print 'close' to suspend the script\n"
                    "Select your option: "
                ).strip().lower()
                
                # Exit options
                if manual_menu_input == "esc":
                    break
                elif manual_menu_input == "close":
                    exit_main_loop = True
                    break

                # Parse folder path(s)
                path_separator = ','
                
                if path_separator in manual_menu_input:
                    folder_paths_list = manual_menu_input.split(path_separator)
                else:
                    folder_paths_list = [manual_menu_input]
                
                valid_folder_paths = [folder_path for folder_path in folder_paths_list if is_dir(folder_path)]
                corrupted_folder_paths = [folder_path for folder_path in folder_paths_list if not is_dir(folder_path)]

                # Check whether valid folder path(s) exist
                if valid_folder_paths:
                    print(f"Valid input identified {valid_folder_paths}")    
                else:
                    print("Valid folder paths has not been identified")
                    continue
                
                # Notify user about corrupted entries
                if corrupted_folder_paths:
                    print(f"Corrupted input identified and won't be processed {corrupted_folder_paths}")

                for valid_folder_path in valid_folder_paths:
                    
                    exit_for_loop = False
                    if exit_for_loop:
                        break
                    # Remove path from the list ---- Check how to handle potential duplicates
                    valid_folder_paths.remove(valid_folder_path)

                    while True:
                        processing_depth_menu_input = input(
                            "\n----Processing depth menu----\n"
                            "Print 0 to process direct child files only\n"
                            "Print 1 to process the entire nested hierarchy\n"
                            "Print 'skip' to skip folder path\n"
                            "Print 'esc' to return to previous menu\n"
                            "Print 'close' to suspend the script\n"
                            f"Select option for {valid_folder_path}: "
                        ).strip().lower()

                        # Exit options
                        if processing_depth_menu_input == "skip":
                            break
                        elif processing_depth_menu_input == "esc":
                            exit_for_loop = True
                            break
                        elif processing_depth_menu_input == "close":
                            exit_for_loop = True
                            exit_main_loop = True
                            break
                        # User input processing
                        if int(processing_depth_menu_input) == 0:
                            folder_scope[ProcessingDepth.DIRECT_SUB].append(valid_folder_path)
                            break
                        elif int(processing_depth_menu_input) == 1:
                            folder_scope[ProcessingDepth.FULL_HIERARCHY].append(valid_folder_path)
                            break
                        else:
                            print("Invalid input please try again\n")
                            continue                        
                
                # Exit handling 
                if exit_for_loop == True and exit_main_loop == False:
                    continue
                elif exit_for_loop == True and exit_main_loop == True:
                    break
                else:
                    pass 
            
                # Exit condition check 
                if folder_scope[ProcessingDepth.DIRECT_SUB] or folder_scope[ProcessingDepth.FULL_HIERARCHY]:
                    exit_condition = True
                    break
                else:
                    print(f"\nNo folder paths to process {folder_scope}")
                    exit_main_loop = True
                    break
        
        else:
            print("\nInvalid input provided please try again\n")
            continue
    
    if exit_main_loop:
        
        print("\nScript terminated\n")
            
    if exit_condition:
        
        # Verify Parent-Child relationship
        pairs_key_0_and_1 = []
        pairs_key_1 = []
        
        if (len(folder_scope[ProcessingDepth.FULL_HIERARCHY])) > 1:
            pairs_key_1 = list(combinations(folder_scope[ProcessingDepth.FULL_HIERARCHY], 2))

        if folder_scope[ProcessingDepth.DIRECT_SUB] and folder_scope[ProcessingDepth.FULL_HIERARCHY]:
            pairs_key_0_and_1 = list(product(folder_scope[ProcessingDepth.DIRECT_SUB], folder_scope[ProcessingDepth.FULL_HIERARCHY]))

        # Identify folder path(s) that needs to be removed
        remove_from_key_2 = [get_child_dir(path_pair) for path_pair in pairs_key_1]
        remove_from_key_1 = [get_child_dir(path_pair) for path_pair in pairs_key_0_and_1]

        # Normalize folder path(s) scope
        folder_scope[ProcessingDepth.FULL_HIERARCHY] = list(set(folder_scope[ProcessingDepth.FULL_HIERARCHY]) - set(remove_from_key_2))
        folder_scope[ProcessingDepth.DIRECT_SUB] = list(set(folder_scope[ProcessingDepth.DIRECT_SUB]) - set(remove_from_key_1))

        print("\nInput obtained successfully\n")
        print(folder_scope)

    