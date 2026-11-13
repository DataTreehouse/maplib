from typing import Any, Dict, List
import os
import pathlib

PATH_HERE = pathlib.Path(__file__).parent

STORAGE_FOLDER = "disk"


def is_maplib_ee() -> bool:
    import dotenv
    dotenv.load_dotenv(PATH_HERE.parent.parent/".env")
    return bool(os.environ.get("MAPLIB_EE", False))


def disk_params() -> List[str]:
    """Storage modes to parameterize tests over. Hybrid and disk storage need maplib_ee."""
    if is_maplib_ee():
        return ["memory", "hybrid", "disk"]
    else:
        return ["memory"]


def storage_kwargs(storage: str) -> Dict[str, Any]:
    """Model / Model.deserialize keyword arguments for a storage mode from disk_params()."""
    if storage == "memory":
        return {"storage": "memory"}
    return {"storage": storage, "storage_folder": STORAGE_FOLDER}
