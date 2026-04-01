from string import Formatter

def get_placeholders(text: str) -> set:
    placeholders = set()
    for _, field_name, format_spec, _ in Formatter().parse(text):
        if field_name:
            placeholders.add(field_name)
            if format_spec:
                placeholders.update(get_placeholders(format_spec))
    return placeholders

def render_cli_object(cli_object: dict, element_name: str = None, **runtime_args) -> str:
    # Validate CLI object dict
    assert isinstance(cli_object, dict), f"CLI object should be a dictionary, {type(cli_object)} provided instead"
    assert "template" in cli_object, "Template is missing"
    assert "defaults" in cli_object, "Defaults is missing"
    assert "elements" in cli_object, "Elements is missing"
    # Unpack CLI assets
    template = cli_object.get("template", {})
    elements = cli_object.get("elements", {})
    # Merge default and element configurations
    element_config = elements.get(element_name, {})
    default_config = cli_object.get("defaults", {})
    merged_config = {**default_config, **element_config}
    # Validate template placeholders
    template_args = get_placeholders(template)
    # Fill in the template placeholders
    if "msg" in merged_config:
        text_args = get_placeholders(merged_config["msg"])
        if text_args:
            missing = text_args - runtime_args.keys()
            assert not missing, f"Missing arguments for placeholders: {missing}"
            merged_config["msg"] = merged_config["msg"].format(**runtime_args)
    missing_configs = template_args - merged_config.keys()
    assert not missing_configs, f"Missing config arguments keys: {missing_configs}" 
    final_element = template.format(**merged_config)
    return final_element

def render_cli_grouped_object(cli_grouped_object: dict, cli_objects: dict, **runtime_args) -> str: # hardcoded "\n"
    rendered_objects = []
    for cli_object_config in cli_grouped_object:
        object_name, element_name = cli_object_config
        cli_object = cli_objects.get(object_name, {})
        rendered_object = render_cli_object(cli_object, element_name, **runtime_args)
        rendered_objects.append(rendered_object)
    return "\n".join(rendered_objects)
