from cli.assets import Icon, Delimiter, Emoji, Template

cli_objects = {  
    "header": {
        "template": Template.SEP_MSG_SEP,
        "defaults": {
            "start": "\n",
            "sep": Delimiter.DASH,
            "msg": "empty"
        },
        "elements":{
            "setup_env": {"sep": Delimiter.DASH.repeat(40), "msg": "Setup Env", "width": 20},
            "main": {"sep": Delimiter.DASH.repeat(40), "msg": "Input Methods", "width": 20},
            "csv_load": {"sep": Delimiter.DASH.repeat(40), "msg": "CSV load", "width": 20},
            "manual_load": {"sep": Delimiter.DASH.repeat(40), "msg": "Manual load", "width": 20},
            "depth": {"sep": Delimiter.DASH.repeat(40), "msg": "Depth", "width": 20}
        }
    },
    "menu_line": {
        "template": Template.ICON_SEP_MSG,
        "defaults": {
            "start": "",
            "icon": Emoji.KEYBOARD,
            "sep": Delimiter.SPACE.repeat(2),
            "msg": "empty"
        },
        "elements": {
            "exit": {"icon": Emoji.CROSSMARK, "sep": Delimiter.SPACE, "msg": "Press 'Ctrl+C' to suspend the script"},
            "cancel": {"icon": Emoji.LEFTWARDARROW, "msg": "Press 'Ctrl+C' to cancel"},
            "restart": {"icon": Emoji.RESTART, "sep": Delimiter.SPACE, "msg": "Press 'Ctrl+C' to cancel current input and retry"},
            "skip": {"msg": "Type 'skip' to skip current dir path"},
            "skip_all": {"msg": "Type 'skipall' to skip the rest of dir path(s)"},
            "csv_load": {"msg": "Type 'csv' to load dir path(s) from CSV"},
            "manual_load": {"msg": "Type 'manual' to provide dir path(s) directly in CLI"},
            "manual_stop": {"msg": "Type 'stop' to finish adding dir path(s)"},
            "depth": {"msg": "Select 'depth level' from {depth_range}"}
        }
    },
    "prompt": {
        "template": Template.ICON_SEP_MSG,
        "defaults": {
            "start": "",
            "icon": Emoji.RIGHTARROW,
            "sep": Delimiter.SPACE.repeat(2),
            "msg": "Provide your option: "
        },
        "elements": {
            "setup_env": {"msg": "Delete content from {target_path} permanently? (y/n): "},
            "csv": {"msg": "Enter link to CSV file: "},
            "manual": {"msg": "Enter dir path: "},
            "manual_additional": {"msg": "Add another one: "},
        }
    },
    "warning": {
        "template": Template.ICON_SEP_MSG,
        "defaults": {
            "start": "",
            "icon": Emoji.WARNINGSIGN,
            "sep": Delimiter.SPACE.repeat(2),
            "msg": "empty"
        },
        "elements": {
            "invalid_input": {"msg": "Invalid input"}, # General
            "empty_input": {"msg": "No dir path(s) to process"}, # General
            "csv_load_failed": {"msg": "CSV path load failed with the reason - {error}"}, # CSV
            "manual_load_failed": {"msg": "Manual path load failed with the reason - {error}"}, # CSV
        }
    },
    "info": {
        "template": Template.ICON_SEP_MSG,
        "defaults": {
            "start": "",
            "icon": Emoji.INFORMATION,
            "sep": Delimiter.SPACE.repeat(2),
            "msg": "empty"
        },
        "elements": {
            "exit": {"msg": "Script terminated"},
            "processing": {"icon": Emoji.HOURGLASS, "sep": Delimiter.SPACE, "msg": "[Processing] -----> {dir_path}"},
            # "added": {"msg": "[Added] -----> {dir_paths_count} dirs"},
            "skipped": {"msg": "[Skipped]    -----> already in scope"},
            "selected":{"icon": Emoji.BULLSEYE, "sep": Delimiter.SPACE.repeat(1), "msg": "[Selected]   -----> {dir_paths_count} dirs"},
            "output_ready": {"icon": Emoji.CHEQUEREDFLAG, "msg": "Files aquisition completed"},
        }
    },
    "divider": {
        "template": Template.SEP,
        "defaults": {
            "sep": Delimiter.DASH.repeat(100)
        }
    },
    "flow_marker": {
        "template": Template.ICON,
        "defaults": {
            "icon": Icon.DOWNARROW.repeat(3)
        }
    }
}

cli_grouped_objects = {
    "main_menu": [
        ("header", "main"),
        ("menu_line", "exit"),
        ("menu_line", "csv_load"),
        ("menu_line", "manual_load")
    ],
    "csv_menu": [
        ("header", "csv_load"),
        ("menu_line", "cancel")
    ],
    "manual_menu": [
        ("header", "manual_load"),
        ("menu_line", "cancel"),
        ("menu_line", "manual_stop")
    ],
    "depth_menu": [
        ("menu_line", "cancel"),
        ("menu_line", "skip_all"),
        ("menu_line", "skip"),
        ("menu_line", "depth")
    ]
}