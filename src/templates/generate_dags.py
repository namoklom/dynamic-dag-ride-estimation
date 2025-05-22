import json
import re
from pathlib import Path
import os

from jinja2 import (
    StrictUndefined,
    Template,
    UndefinedError,
)

def main():
    with open("./template.py", "r", encoding="utf-8") as file:
        unsafe_template_str = file.read()

    folder_configs = Path("./dag_configs/")
    print(f"folder_configs {folder_configs}")
    
    dags_dir = "./../dags"
    
    if not os.path.exists(dags_dir):
        os.mkdir(dags_dir)
    else:
        print(f"The directory {dags_dir} already exists.")
    
    for path_config in folder_configs.glob("config_*.json"):
        with open(path_config, "r", encoding="utf-8") as file:
            config = json.load(file)

        template_str = protect_undefineds(unsafe_template_str, config)
        
        filename = f"./../dags/{config['dag_name']}.py"
        
        content = Template(template_str).render(config)
        
        with open(filename, mode="w", encoding="utf-8") as file:
            file.write(content)
            print(f"Created {filename} from config: {path_config.name}...")
        

def protect_undefineds(template_str: str, config: dict):
    pattern = re.compile(r"(\{\{[^\{]*\}\})")
    for j2_expression in set(pattern.findall(template_str)):
        try:
            Template(j2_expression, undefined=StrictUndefined).render(config)
        except UndefinedError:
            template_str = template_str.replace(
                j2_expression, f"{{% raw %}}{j2_expression}{{% endraw %}}"
            )
    return template_str


if __name__ == "__main__":
    main()
