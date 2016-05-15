from setup import opts

version = opts["version"]

with open("manatee/__init__.py", "r") as f:
    lines = f.readlines()

with open("manatee/__init__.py", "w") as f:
    for line in lines:
        if line.startswith("__version__"):
            f.write("__version__ = \"{}\"".format(version))
        else:
            f.write(line)
