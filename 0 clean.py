import subprocess
import os

# Base path of the project
base_path = os.path.dirname(os.path.abspath(__file__))

# Folders to exclude
excluded_dirs = {"venv", "nautilus_trader"}

# Walk through all directories
for root, dirs, files in os.walk(base_path):
    # Skip excluded directories
    if any(excluded in root.split(os.sep) for excluded in excluded_dirs):
        continue

    try:
        print(f"Cleaning: {root}")
        subprocess.run(["dot_clean", "-m", "-f", root], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        print(f"Error cleaning {root}:\n{e.stderr}")

# Optional: Delete .DS_Store files (excluding in excluded_dirs)
print("Deleting all .DS_Store files (except excluded dirs)...")

for root, dirs, files in os.walk(base_path):
    if any(excluded in root.split(os.sep) for excluded in excluded_dirs):
        continue
    for file in files:
        if file == ".DS_Store":
            path_to_delete = os.path.join(root, file)
            os.remove(path_to_delete)
            print(f"Deleted: {path_to_delete}")
