from env import get_project_paths

paths = get_project_paths("qwass2")

print("DB path:", paths["db"])
print("Outputs path:", paths["outputs"])
