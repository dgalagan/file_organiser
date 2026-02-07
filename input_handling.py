import os
import sys
import pandas as pd
from itertools import combinations, product

exit_flag = False
input_dict = {1:[], 2:[]}

while True:
    
    # Main loop exit condition
    if exit_flag:
        break
    # Request user to select data provision options
    print("\n----Main menu----\n")
    user_input = input(
    "Print 'csv' to load folder path(s) from CSV\n" \
    "Print 'manual' to provide folder path(s) manually\n" \
    "Print 'esc' to suspend the script\n"
    "Enter your option: "
    )

    if user_input == 'csv':
        print("\n----CSV menu----\n")
        while True:
            # Request user to provide link to csv file
            csv_input = input(
                "Provide link to csv file\n"
                "Print 'esc' to return to previous menu\n"
                "Enter your option: "
            )
            if csv_input == 'esc':
                break
            else:
                # Check whether provided link is file, otherwise continue
                if not os.path.isfile(csv_input):
                    print(f"\nProvided link {csv_input} is not a file\n")
                    continue
                # Check whether file extension == 'csv', otherwise continu
                file_basename, file_ext = os.path.splitext(csv_input)
                if not file_ext == '.csv':
                    print(f"\nProvided file has wrong extension {file_ext}\n")
                    continue
                # Open csv file as dataframe, otherwise continue
                try:
                    csv_data = pd.read_csv(csv_input)
                except:
                    print("\nProvided csv file empty or corrupted and could not be opened\n")
                    continue
                # Check whether "FolderPath", "ProcessingDepth" columns are available, otherwise continue
                required_cols = {"FolderPath", "ProcessingDepth"}
                csv_data_cols = {*csv_data}
                if not (required_cols == csv_data_cols or required_cols.issubset(set(csv_data_cols))):
                    print(f"\nRequired columns {required_cols} are missing\n")
                    continue
                # Remove duplicate values in "FolderPath"
                csv_normalized = csv_data.drop_duplicates(
                    "FolderPath", 
                    inplace=False
                    )
                # Test "ProcessingDepth" values
                processing_depth_values = {1, 2}
                csv_normalized["ProcessingDepthTest"] = csv_normalized["ProcessingDepth"].apply(
                    lambda x: True if x in processing_depth_values else False
                    )
                # Test "FolderPath" values:
                csv_normalized["FolderPathTest"] = csv_normalized["FolderPath"].apply(
                    lambda x: True if os.path.isdir(x) else False
                    )
                # Select valid entries
                condition = (csv_normalized["ProcessingDepthTest"] == True) & (csv_normalized["FolderPathTest"] == True)
                valid_data = csv_normalized[condition]
                # Check whether selected data is empty, otherwise continue
                if valid_data.empty:
                    print("Valid entries are absent\n")
                    continue
                # Transform valid data and convert it into ditionary 
                transformed_data = pd.pivot_table(
                    valid_data,
                    index="ProcessingDepth",
                    values="FolderPath",
                    aggfunc=lambda x: list(x)
                )
                # Update input dictionary
                input_dict = transformed_data.to_dict()["FolderPath"]
                # Exit loop
                exit_flag = True
                break

    elif user_input == 'manual':
        print("\n----Manual menu----\n")
        while True:
            folder_paths = input(
                "Print one or several folder path(s) separated by , without spaces\n"
                "Print 'esc' to return to previous menu\n"
                "Select your option: "
            )
            if folder_paths == 'esc':
                break
            else:
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
                        "Print 2 to process the entire nested hierarchy\n"
                        "Print 1 to process direct child files only\n"
                        "Print 'esc' to skip folder path\n"
                        f"Select option for {valid_folder_path}: "
                    )
                    if processing_depth == "esc":
                        break
                    elif int(processing_depth) == 2:
                        input_dict[2].append(valid_folder_path)
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
    elif user_input == 'esc':
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

# Function that identifies Parent-Child relationship for a given pair of folder paths
def get_child(path_pair):
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

print(pairs_key_2)
print("-----")
print(pairs_key_1_and_2)

# Identify folder path(s) that needs to be removed
remove_from_key_2 = [get_child(path_pair) for path_pair in pairs_key_2]
remove_from_key_1 = [get_child(path_pair) for path_pair in pairs_key_1_and_2]

# Normalize folder path(s) scope
input_dict[2] = list(set(input_dict[2]) - set(remove_from_key_2))
input_dict[1] = list(set(input_dict[1]) - set(remove_from_key_1))

print("Input obtained successfully")
print(input_dict)