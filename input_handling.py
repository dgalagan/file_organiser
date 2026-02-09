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
    
def validate_df_cols(df, required_cols):
    # Check whether required columns are available
    df_cols = {*df}
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

class ProcessingDepth(IntEnum):
    DIRECT_SUB = 0
    FULL_HIERARCHY = 1

FOLDER_PATH_COL = "FolderPath"
PROCESSING_DEPTH_COL = "ProcessingDepth"
REQUIRED_COLS = [FOLDER_PATH_COL, PROCESSING_DEPTH_COL]

FOLDER_PATH_TEST_COL = "FolderPathTest"
PROCESSING_DEPTH_TEST_COL = "ProcessingDepthTest"
TEST_COLS = [FOLDER_PATH_TEST_COL, PROCESSING_DEPTH_TEST_COL]

exit_flag = False
file_scope = {ProcessingDepth.DIRECT_SUB:[], ProcessingDepth.FULL_HIERARCHY:[]}

if __name__ == "__main__":
    
    while True:
        
        # Main loop exit condition
        if exit_flag:
            break
        
        # Request user to select data provision options
        print("\n----Main menu----\n")
        data_input_option = get_data_input_option()

        if data_input_option == 'csv':
            print("\n----CSV menu----\n")
            while True:
                # Request user to provide link to csv file
                csv_input = input(
                    "Provide link to csv file\n"
                    "Print 'esc' to return to previous menu\n"
                    "Print 'close' to suspend the script\n"
                    "Enter your option: "
                )
                if csv_input == 'esc':
                    break
                
                # Open CSV file as dataframe
                csv_data = open_csv(csv_input)
                if csv_data is None:
                    continue
                
                # Validate CSV columns
                validated_csv_data = validate_df_cols(csv_data, REQUIRED_COLS)
                if not validated_csv_data:
                    continue
                
                # Validate processing depth
                validated_csv_data[PROCESSING_DEPTH_TEST_COL] = validated_csv_data[PROCESSING_DEPTH_COL].apply(
                    lambda x: True if x in [ProcessingDepth.DIRECT_SUB, ProcessingDepth.FULL_HIERARCHY] else False
                    )
                validated_csv_data[FOLDER_PATH_TEST_COL] = validated_csv_data[PROCESSING_DEPTH_COL].apply(
                    lambda x: True if is_dir(x) else False
                    )
                
                # Normalize CSV data 
                normalized_csv_data = remove_duplicates(validated_csv_data, column=FOLDER_PATH_COL)
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
                input_dict = transformed_data.to_dict()[FOLDER_PATH_COL]
                
                # Exit loop
                exit_flag = True
                break
        elif data_input_option == 'manual':
            print("\n----Manual menu----\n")
            while True:
                folder_paths = input(
                    "Print one or several folder path(s) separated by , without spaces\n"
                    "Print 'esc' to return to previous menu\n"
                    "Print 'close' to suspend the script\n"
                    "Select your option: "
                )
                if folder_paths == 'esc':
                    break
                
                # Parse provided string and identify valid folder path(s)             
                valid_folder_paths = []
                corrupted_folder_paths = []
                duplicated_folder_path = []
                if ',' in folder_paths:
                    folder_paths_list = folder_paths.split(',')
                    for folder_path in folder_paths_list:
                        if os.path.isdir(folder_path):
                            if folder_path not in valid_folder_paths:
                                valid_folder_paths.append(folder_path)
                            else:
                                duplicated_folder_path.append(folder_path)
                        else:
                            corrupted_folder_paths.append(folder_path)
                else:
                    if os.path.isdir(folder_path):
                        valid_folder_paths.append(folder_path)
                    else:
                        corrupted_folder_paths.append(folder_path)

                # Check whether valid folder path(s) exist
                if  valid_folder_paths:
                    print(f"\nValid folder paths has been identified {valid_folder_paths}")
                else:
                    print("Valid folder paths has not been identified")    
                    continue
                if duplicated_folder_path:
                    print(f"Duplicates were excluded {duplicated_folder_path}")
                if corrupted_folder_paths:
                    print(f"Corrupted input that won't be processed {corrupted_folder_paths}")

                for valid_folder_path in valid_folder_paths:
                    while True:
                        print("\n----Processing depth menu----\n")
                        processing_depth = input(
                            "Print 1 to process the entire nested hierarchy\n"
                            "Print 0 to process direct child files only\n"
                            "Print 'skip' to skip folder path\n"
                            "Print 'esc' to return to previous menu\n"
                            "Print 'close' to suspend the script\n"
                            f"Select option for {valid_folder_path}: "
                        )
                        if processing_depth == "skip":
                            break
                        elif int(processing_depth) == 0:
                            input_dict[0].append(valid_folder_path)
                            break
                        elif int(processing_depth) == 1:
                            input_dict[1].append(valid_folder_path)
                            break
                        else:
                            print("Invalid input please try again\n")
                            continue

                # Exit loop
                exit_flag = True
                break
        elif data_input_option == 'close':
            print("\nScript terminated\n")
            break
        else:
            print("\nInvalid input provided please try again\n")
            continue

    # Verify Parent-Child relationship
    if (len(input_dict[2])) > 1:
        pairs_key_2 = list(combinations(input_dict[2], 2))

    if input_dict[1] and input_dict[2]:
        pairs_key_1_and_2 = list(product(input_dict[1], input_dict[2]))

    print(pairs_key_2)
    print("-----")
    print(pairs_key_1_and_2)

    # Identify folder path(s) that needs to be removed
    remove_from_key_2 = [get_child_dir(path_pair) for path_pair in pairs_key_2]
    remove_from_key_1 = [get_child_dir(path_pair) for path_pair in pairs_key_1_and_2]

    # Normalize folder path(s) scope
    input_dict[2] = list(set(input_dict[2]) - set(remove_from_key_2))
    input_dict[1] = list(set(input_dict[1]) - set(remove_from_key_1))

    print("Input obtained successfully")
    print(input_dict)