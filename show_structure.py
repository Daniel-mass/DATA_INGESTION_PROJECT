import os

def show_structure(directory, prefix=""):
    for item in sorted(os.listdir(directory)):
        path = os.path.join(directory, item)
        if os.path.isdir(path):
            print(f"{prefix}📁 {item}/")
            show_structure(path, prefix + "    ")
        else:
            print(f"{prefix}📄 {item}")

if __name__ == "__main__":
    show_structure(".")
