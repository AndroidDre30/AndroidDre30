import os
import shutil

dir = os.path.join(os.path.expanduser("~"), "Documents")

ext = {
    ".jpg": "Images",
    ".png": "Images",
    ".gif": "Images",
    ".mp4": "Videos",
    ".nov": "Videos",
    ".doc": "Documents",
    ".pdf": "Documents",
    ".txt": "Documents",
    ".mp3": "Music",
    ".wav": "Music",
    ".py": "Python Scripts"
}

for filename in os.listdir(dir):
    file_path = os.path.join(dir, filename)

    if os.path.isfile(file_path):
        extensions = os.path.splitext(filename)[1].lower()

        if extensions in ext:
            folder_name = ext[extensions]

            folder_path = os.path.join(dir, folder_name)
            os.makedirs(folder_path, exist_ok=True)

            destination_path = os.path.join(folder_path, filename)
            shutil.move(file_path, destination_path)

            print(f"Moved {filename} to {folder_name} folder.")
        else:
            print(f"Skipped {filename}. Unknown file extension.")
    else:
        print(f"Skipped {filename}. It is a directory.")

print("File organization completed.")

