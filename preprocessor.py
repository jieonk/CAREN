
import os
import json
import uuid
import getpass
from pathlib import Path
from datetime import datetime
from file_parser import parse_file_by_extension

def extract_file_metadata(file_path, folder_id):
    parsed_result = parse_file_by_extension(file_path)

    return {
        "fileID": str(uuid.uuid4()),
        "fileName": os.path.basename(file_path),
        "filePath": str(file_path),
        "fileOwner": getpass.getuser(),
        "createdTime": datetime.fromtimestamp(os.path.getctime(file_path)).isoformat(),
        "modifiedTime": datetime.fromtimestamp(os.path.getmtime(file_path)).isoformat(),
        "relation": "isPartOf",
        "parentFolder": folder_id,
        "actions": parsed_result.get("actions", []),
        "metadata": parsed_result.get("metadata", {})
    }

def process_folder(folder_path):
    folder_id = str(uuid.uuid4())
    folder_data = {
        "folderID": folder_id,
        "folderPath": str(folder_path),
        "folderOwner": getpass.getuser(),
        "files": []
    }

    for root, dirs, files in os.walk(folder_path):
        for file_name in files:
            file_path = Path(root) / file_name
            file_metadata = extract_file_metadata(file_path, folder_id)
            folder_data["files"].append(file_metadata)

    return folder_data


def save_output(data, output_path):
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

