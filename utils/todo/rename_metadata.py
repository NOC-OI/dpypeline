import json

# Open .zmetadata file to modify
with open("/home/joaomorado/git_repos/tmp/ahah/kaka.zarr/.zmetadata", "r") as f:
    data = json.load(f)

metadata = data["metadata"]


for elem in metadata:
    if "long_name" in metadata[elem]:
        long_name = metadata[elem]["long_name"]
        print(long_name, elem)
        print(type(long_name))


# Write new .zmetadata file
with open(".zmetadata", "w") as f:
    json.dump(data, f)
