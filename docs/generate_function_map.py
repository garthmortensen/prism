import ast
import json
import os

import yaml


def get_function_behavior(node):
    """Extracts the first line of the docstring as behavior."""
    docstring = ast.get_docstring(node)
    if docstring:
        return docstring.strip().split('\n')[0]
    return "No description provided."

class StructureVisitor(ast.NodeVisitor):
    def __init__(self):
        """Initialize the visitor with an empty structure list."""
        self.structure = []
        self.stack = [self.structure]

    def visit_ClassDef(self, node):
        """Visit a class definition and add it to the structure."""
        item = {
            "name": node.name,
            "type": "class",
            "doc": get_function_behavior(node),
            "children": []
        }
        self.stack[-1].append(item)
        self.stack.append(item["children"])
        self.generic_visit(node)
        self.stack.pop()

    def visit_FunctionDef(self, node):
        """Visit a function definition and add it to the structure."""
        item = {
            "name": node.name,
            "type": "function",
            "doc": get_function_behavior(node),
            "children": []
        }
        self.stack[-1].append(item)
        self.stack.append(item["children"])
        self.generic_visit(node)
        self.stack.pop()

    def visit_AsyncFunctionDef(self, node):
        """Visit an async function definition and add it to the structure."""
        item = {
            "name": node.name,
            "type": "async_function",
            "doc": get_function_behavior(node),
            "children": []
        }
        self.stack[-1].append(item)
        self.stack.append(item["children"])
        self.generic_visit(node)
        self.stack.pop()

def analyze_file(filepath, root_dir):
    """Analyzes a Python file to extract structure."""
    with open(filepath, encoding="utf-8") as f:
        try:
            tree = ast.parse(f.read(), filename=filepath)
        except SyntaxError:
            return None

    visitor = StructureVisitor()
    visitor.visit(tree)
    
    if not visitor.structure:
        return None

    # Create module name from path
    rel_path = os.path.relpath(filepath, root_dir)
    module_name = os.path.splitext(rel_path)[0].replace(os.sep, ".")
    
    return {
        "name": module_name,
        "type": "module",
        "children": visitor.structure
    }

def generate_yaml_data(modules):
    """Generate a flat YAML dictionary from the hierarchical module structure."""
    yaml_data = {}

    def recurse(nodes, prefix, result_list):
        for node in nodes:
            current_name = node["name"]
            if prefix:
                current_name = f"{prefix}.{node['name']}"

            if node["type"] in ("function", "async_function"):
                result_list.append({current_name: node["doc"]})

            recurse(node.get("children", []), current_name, result_list)

    for module in modules:
        flat_list = []
        recurse(module["children"], "", flat_list)

        if flat_list:
            yaml_data[module["name"]] = flat_list
    return yaml_data
def generate_dot_data(modules):
    """Generate a DOT graph string from the hierarchical module structure."""
    lines = [
        "digraph RepoStructure {",
        "rankdir=LR;",
        "node [shape=box style=filled fillcolor=white];"
    ]
    
    def clean_id(name):
        return name.replace(".", "_").replace("-", "_")

    def process_nodes(nodes, parent_prefix):
        for node in nodes:
            if node["type"] == "class":
                cluster_name = f"cluster_{parent_prefix}_{node['name']}"
                clean_cluster_name = clean_id(cluster_name)
                lines.append(f"subgraph {clean_cluster_name} {{")
                lines.append(f'label="{node["name"]}";')
                lines.append('style=filled; fillcolor=white; color=black;')
                
                process_nodes(node["children"], f"{parent_prefix}_{node['name']}")
                
                lines.append("}")
            elif node["type"] in ("function", "async_function"):
                node_id = clean_id(f"{parent_prefix}_{node['name']}")
                lines.append(f'{node_id} [label="{node["name"]}"];')

    for module in modules:
        cluster_name = f"cluster_{module['name']}"
        clean_cluster_name = clean_id(cluster_name)
        lines.append(f"subgraph {clean_cluster_name} {{")
        lines.append(f'label="{module["name"]}";')
        lines.append('style=filled; fillcolor=lightgrey; color=black;')
        
        process_nodes(module["children"], module["name"])
        
        lines.append("}")
        
    lines.append("}")
    return "\n".join(lines)
def main():
    """Main entry point to generate the function map files."""
    # Set root_dir to the parent directory of this script (the repo root)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.dirname(script_dir)
    
    exclude_dirs = {
        '.git', '.venv', 'venv', '__pycache__', 'compute_logs', 
        'dagster_home', 'docs', 'images', 'logs', 'storage', 
        'dbt_packages', 'target', 'node_modules', 'site-packages'
    }
    
    modules = []

    for dirpath, dirnames, filenames in os.walk(root_dir):
        # Modify dirnames in-place to exclude directories
        dirnames[:] = [d for d in dirnames if d not in exclude_dirs]
        
        for filename in filenames:
            if filename.endswith(".py"):
                filepath = os.path.join(dirpath, filename)
                result = analyze_file(filepath, root_dir)
                if result:
                    modules.append(result)

    # 1. Generate YAML (Flat)
    yaml_data = generate_yaml_data(modules)
    with open(os.path.join(script_dir, "repo_functions.yaml"), "w") as f:
        yaml.dump(yaml_data, f, sort_keys=True, default_flow_style=False)
    print(f"Generated {os.path.join(script_dir, 'repo_functions.yaml')}")

    # 2. Generate JSON (Hierarchical)
    with open(os.path.join(script_dir, "repo_structure.json"), "w") as f:
        json.dump(modules, f, indent=2)
    print(f"Generated {os.path.join(script_dir, 'repo_structure.json')}")

    # 3. Generate DOT (Graph)
    dot_content = generate_dot_data(modules)
    with open(os.path.join(script_dir, "repo_structure.dot"), "w") as f:
        f.write(dot_content)
    print(f"Generated {os.path.join(script_dir, 'repo_structure.dot')}")

if __name__ == "__main__":
    main()
