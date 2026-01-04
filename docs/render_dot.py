import graphviz
import os

try:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    dot_path = os.path.join(script_dir, 'repo_structure.dot')
    output_prefix = os.path.join(script_dir, 'repo_structure')
    
    # Load the dot file
    source = graphviz.Source.from_file(dot_path)
    # Render it to png
    output_path = source.render(output_prefix, format='png', cleanup=True)
    print(f"Successfully generated diagram at: {output_path}")
except graphviz.backend.execute.ExecutableNotFound:
    print("Error: The 'dot' executable was not found.")
    print("The 'graphviz' Python package requires the Graphviz system software to be installed.")
    print("Please install it using your system package manager (e.g., 'sudo apt install graphviz').")
except Exception as e:
    print(f"An error occurred: {e}")
