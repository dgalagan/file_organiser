import os
import sys
import pandas as pd

exit_flag = False
input_dict = None

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
        print("----Manual menu-----")
        break
    elif user_input == 'esc':
        print("\nScript terminated\n")
        break
    else:
        print("\nInvalid input provided please try again\n")
        continue

print("Input obtained successfully")
print(input_dict)