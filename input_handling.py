import os
import pandas as pd
from itertools import combinations, product
from enum import IntEnum

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

def parse_path(path, separator=None):
    
    if separator is None:
        return path
    
    return path.split(separator)

def open_csv(path):
    
    # Check whether provided link is file, otherwise continue
    if not is_file(path):
        print(f"\n❌ Provided path {path} is not a file")
        return None
    
    # Check whether file extension == 'csv', otherwise continu
    file_extension = get_file_extension(path)
    if not file_extension == '.csv':
        print(f"\n❌ Provided file extension is not supported {file_extension}")
        return None

    # Open csv file as dataframe, otherwise continue
    try:
        csv_data = pd.read_csv(path)
        return csv_data
    except:
        print("\n❌ Provided CSV file empty or corrupted and could not be opened")
        return None
    
def validate_df_cols(df_cols, required_cols):
    # Check whether required columns are available
    if required_cols > df_cols:
        missing_cols = required_cols - df_cols
        print(f"❌ Required columns {missing_cols} are missing")
        return False
    else:
        print(f"✅ Required columns {required_cols} identified")
        return True

def remove_duplicates(df, column_name=None):

    # Check whether column provided
    if column_name is None:
        print(f"❌ Please provide column name to remove dupliates from")
        return None
    
    df_normalized = df.drop_duplicates(
            column_name, 
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

if __name__ == "__main__":
    
    path_separator = ','
    folder_scope = {ProcessingDepth.DIRECT_SUB:[], ProcessingDepth.FULL_HIERARCHY:[]}
    
    # Fixed data schema
    FOLDER_PATH_COL = "FolderPath"
    PROCESSING_DEPTH_COL = "ProcessingDepth"
    REQUIRED_COLS = [FOLDER_PATH_COL, PROCESSING_DEPTH_COL]

    FOLDER_PATH_TEST_COL = "FolderPathTest"
    PROCESSING_DEPTH_TEST_COL = "ProcessingDepthTest"
    TEST_COLS = [FOLDER_PATH_TEST_COL, PROCESSING_DEPTH_TEST_COL]

    # Loop control parameters
    return_to_main = False
    return_to_csv = False
    return_to_manual = False
    return_to_folder_path = False
    terminate_main_loop = False
    terminate_for_loop = False
    exit_condition = False

    while True:
        # Request user to select data provision options
        main_menu_input = input(
            "\n----Main menu----\n"
            "🛑 Print 'exit' to suspend the script\n"
            "⌨️  Print 'csv' to load folder path(s) from CSV\n"
            "⌨️  Print 'manual' to provide folder path(s) manually\n"
            "➜  Select your option: "
        ).strip().lower()

        # User input handling
        if main_menu_input == 'exit':
            terminate_main_loop = True
        elif main_menu_input == 'csv':
            while True:
                # Request user to choose option from csv menu
                try:
                    csv_menu_input = input(
                        "\n----CSV menu----\n"
                        "🛑 Print 'exit' to suspend the script\n"
                        "↩️  Press 'Ctrl+C' to go back\n"
                        "⌨️  Print 'input' to provide link to CSV file\n"
                        "➜  Select your option: "
                    )
                except KeyboardInterrupt:
                    print("")
                    return_to_main = True
                    break
                
                # User input handling
                if csv_menu_input == "exit":
                    terminate_main_loop = True
                    break
                elif csv_menu_input == "input":
                    while True:
                        # Request user to provide a link
                        try:
                            csv_path = input(
                                "\n↩️  Press 'Ctrl+C' to go back\n"
                                "⌨️  Please provide link to CSV file: "
                            )
                        except KeyboardInterrupt:
                            print("")
                            return_to_csv = True
                            break
                        # Open CSV file as dataframe
                        csv_data = open_csv(csv_path)
                        if csv_data is None:
                            continue
                        print("\n✅ CSV file opened successfully")
                        # Validate CSV columns
                        csv_cols = {*csv_data}
                        if not validate_df_cols(set(csv_cols), set(REQUIRED_COLS)):
                            continue
                        # Validate processing depthcsv
                        csv_data[PROCESSING_DEPTH_TEST_COL] = csv_data[PROCESSING_DEPTH_COL].apply(
                            lambda x: True if x in [ProcessingDepth.DIRECT_SUB, ProcessingDepth.FULL_HIERARCHY] else False
                            )
                        csv_data[FOLDER_PATH_TEST_COL] = csv_data[FOLDER_PATH_COL].apply(
                            lambda x: True if is_dir(x) else False
                            )
                        # Normalize CSV data 
                        normalized_csv_data = remove_duplicates(csv_data, column_name=FOLDER_PATH_COL)
                        print("✅ Duplicates removed successfully")
                        # Select valid entries
                        condition = (normalized_csv_data[FOLDER_PATH_TEST_COL] == True) & (normalized_csv_data[PROCESSING_DEPTH_TEST_COL] == True)
                        filtered_csv_data = filter_df(normalized_csv_data, condition)
                        if filtered_csv_data is None:
                            print("🔁 Valid folder path(s) are missing, please upload another CSV file")
                            continue
                        print("✅ Valid folder path(s) filtered successfully")
                        # Transform valid data and convert it into ditionary 
                        transformed_data = pd.pivot_table(
                            filtered_csv_data,
                            index=PROCESSING_DEPTH_COL,
                            values=FOLDER_PATH_COL,
                            aggfunc=lambda x: list(x)
                        )
                        # Update folder scope dictionary
                        path_counter = 0
                        transformed_data_idxs = list(transformed_data.index.values)
                        for idx in transformed_data_idxs:                            
                            folder_scope[idx] = transformed_data[FOLDER_PATH_COL].loc[idx]
                            # Check if dict value is not empty and provide some notification
                            if folder_scope[idx]:
                                path_count = len(folder_scope[idx])
                                path_counter += path_count
                                print(f"✅ {path_count} folder path(s) identified for {ProcessingDepth(idx).name} processing")
                    
                        # Loop control parameters check
                        if path_counter > 0:
                            exit_condition = True
                            break
                        else:
                            print("🔁 Valid folder path(s) are missing, please provide another one")
                            continue
                else:
                    print("\n🔁Invalid input provided please try again")
                    continue
                
                # Loop control parameters check
                if return_to_csv:
                    return_to_csv = False
                    continue
                elif exit_condition:
                    break
                else:
                    print("❓Unknown event")
        elif main_menu_input == 'manual':
            while True:
                try:
                    manual_menu_input = input(
                        "\n----Manual menu----\n"
                        "🛑 Print 'exit' to suspend the script\n"
                        "↩️  Press 'Ctrl+C' to go back\n"
                        "⌨️  Print 'input' to provide one or several folder path(s)\n"
                        "➜  Select your option: "
                    ).strip().lower()
                except KeyboardInterrupt:
                    print("")
                    return_to_main = True
                    break
                
                # User input handling
                if manual_menu_input == "exit":
                    terminate_main_loop = True
                    break
                elif manual_menu_input == "input":
                    while True:
                        # Request user to provide a link
                        try:
                            folder_paths = input(
                                "\n↩️  Press 'Ctrl+C' to go back\n"
                                f"⌨️  Please provide one or several folder path(s) separated with {path_separator}: "
                                )
                        except KeyboardInterrupt:
                            print("")
                            return_to_manual = True
                            break
                        
                        # Process folder paths
                        if path_separator in folder_paths:
                            folder_paths_list = folder_paths.split(path_separator)
                        else:
                            folder_paths_list = [folder_paths]
                        
                        valid_folder_paths = [folder_path for folder_path in folder_paths_list if is_dir(folder_path)]
                        corrupted_folder_paths = [folder_path for folder_path in folder_paths_list if not is_dir(folder_path)]

                        # Notify user about valid entries  
                        count_valid_paths = len(valid_folder_paths)
                        if count_valid_paths == 0:
                            print("\n🔁 Provided folder path(s) are invalid, please try another one")
                            continue
                        elif count_valid_paths == 1:
                            print("\n✅ Provided folder path is valid, please proceed with processing depth selection")
                        else:
                            print("\n✅ Provided folder paths are valid, please proceed with processing depth selection")
                        # Notify user about corrupted entries
                        count_corrupted_paths = len(corrupted_folder_paths)
                        if count_corrupted_paths == 0:
                            pass
                        elif count_corrupted_paths == 1:
                            print(f"\n⚠️  Corrupted folder path identified and won't be processed {corrupted_folder_paths}")
                        else:
                            print(f"\n⚠️  Corrupted folder paths identified and won't be processed {corrupted_folder_paths}")

                        path_counter_direct_sub = 0
                        path_counter_full_hier = 0
                        terminate_for_loop = False
                        for valid_folder_path in valid_folder_paths:
                            while True:
                                try:
                                    processing_depth_menu_input = input(
                                        "\n----Processing depth menu----\n"
                                        "↩️  Press 'Ctrl+C' to go back\n"
                                        "⌨️  Print '0' to process direct child objects only\n"
                                        "⌨️  Print '1' to process the entire nested hierarchy\n"
                                        "⌨️  Print 'skip' to skip folder path\n"
                                        f"➜  Select your option for {valid_folder_path}: "
                                    ).strip().lower()
                                except KeyboardInterrupt:
                                    print("")
                                    terminate_for_loop = True
                                    return_to_folder_path = True
                                    break
                        
                                if processing_depth_menu_input == "skip":
                                    break
                                elif int(processing_depth_menu_input) == 0:
                                    folder_scope[ProcessingDepth.DIRECT_SUB].append(valid_folder_path)
                                    path_counter_direct_sub += 1
                                    break
                                elif int(processing_depth_menu_input) == 1:
                                    folder_scope[ProcessingDepth.FULL_HIERARCHY].append(valid_folder_path)
                                    path_counter_full_hier += 1
                                    break
                                else:
                                    print("\n❌ Invalid input provided please try again")
                                    continue
                                            # Exit trigger
                            
                            # Loop control parameters check
                            if terminate_for_loop:
                                print(f"\n⚠️  Please note that folder scope dictionary has been reset")
                                folder_scope = {ProcessingDepth.DIRECT_SUB:[], ProcessingDepth.FULL_HIERARCHY:[]}
                                break

                        # User notification
                        path_counter = path_counter_direct_sub + path_counter_full_hier

                        # Loop control parameters check
                        if return_to_folder_path:
                            return_to_folder_path = False
                            continue
                        elif path_counter > 0 and terminate_for_loop == False:
                            exit_condition = True
                            print(f"\n✅ {path_counter_direct_sub} folder path(s) identified for {ProcessingDepth(0).name} processing")
                            print(f"✅ {path_counter_full_hier} folder path(s) identified for {ProcessingDepth(1).name} processing")
                            break
                        elif path_counter == 0 and terminate_for_loop == False:
                            continue
                        else:
                            print("🔁 Valid folder path(s) are missing, please provide another one")
                            continue
                else:
                    print("\n❌ Invalid input provided please try again")
                    continue
                
                # Loop control parameters check
                if return_to_manual:
                    return_to_manual = False
                    continue
                elif exit_condition:
                    break
                else:
                    print("❓Unknown event")
        else:
            print("\n❌ Invalid input provided please try again")
            continue
        
        # Main menu while loop exit handling 
        if return_to_main:
            return_to_main = False
            continue
        elif terminate_main_loop:
            print("\n❌ Script terminated\n")
            terminate_main_loop = False
            break
        elif exit_condition:
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
            print(f"➡️  Input obtained successfully {folder_scope}\n")
            break
        else:
            print("❓Unknown event")