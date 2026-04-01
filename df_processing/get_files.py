# max_branch_depth = dir_data[DirPathSchema.BranchDepth].max()
# dirs_by_depth = {depth: [] for depth in range(0, max_branch_depth + 1)}
# dir_paths = []
# file_paths = []
# total_dirs_added = 0
# total_files_added = 0
#     print(Delimiter.DASH.repeat(80))
#     print(render_cli_object(cli_objects["info"], "processing", dir_path=dir_path))
#     print(Icon.DOWNARROW.repeat(3))
#     if dir_path not in dirs_by_depth[dir_depth]:
#         depth_input, in_action = depth_loop(cli_grouped_objects, cli_objects, branch_depth_from_dir)
#         match in_action:
#             case MenuActions.SKIP:
#                 continue
#             case MenuActions.SKIP_ALL:
#                 break
#             case MenuActions.INTERUPT:
#                 reload = True
#                 break
#             case MenuActions.SUCCESS:
#                 dir_data["UserDepth"] = dir_data[DirPathSchema.DirDepth] + depth_input
#                 dirs_added = 0
#                 files_added = 0
#                 for depth, dir_path, files in iter_dir_hierarchy(dir_path, depth_input):
#                     dirs_by_depth[depth].append(dir_path)
#                     dir_paths.append(dir_path)
#                     current_files = [os.path.join(dir_path, filename) for filename in files]
#                     file_paths.extend(current_files)
#                     dirs_added += 1
#                     files_added += len(files)
#                 total_dirs_added += dirs_added
#                 total_files_added += files_added
#                 print(Icon.DOWNARROW.repeat(3))
#                 print(render_cli_object(cli_objects["info"], "added", dir_paths_count=dirs_added, file_paths_count=files_added))
#                 continue
#     else:
#         print(render_cli_object(cli_objects["info"], "skipped"))
#         # hierarchy resolution, ask whether child should be processed separately, delete all related path from parent search, add new
# print(Delimiter.DASH.repeat(80))
# print(render_cli_object(cli_objects["info"], "selected", dir_paths_count=total_dirs_added, file_paths_count=total_files_added))
# print(Icon.DOWNARROW.repeat(3))
# if total_files_added == 0:
#     return None, MenuActions.FAILED