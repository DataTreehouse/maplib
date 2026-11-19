from typing import Any, Dict, List
import os
import pathlib

PATH_HERE = pathlib.Path(__file__).parent

STORAGE_FOLDER = str(PATH_HERE / "disk")


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


_SUITE_FOLDER = None


def suite_storage_kwargs() -> Dict[str, Any]:
    """Model keyword arguments for compliance suites, in the storage mode of the environment
    variable MAPLIB_TEST_STORAGE (memory, the default, hybrid or disk), each model with a storage
    folder of its own that is removed when the tests end."""
    global _SUITE_FOLDER
    storage = os.environ.get("MAPLIB_TEST_STORAGE", "memory")
    if storage == "memory":
        return {}
    import atexit
    import shutil
    import tempfile

    if _SUITE_FOLDER is None:
        _SUITE_FOLDER = tempfile.mkdtemp(prefix="maplib_suite_storage_")
        atexit.register(shutil.rmtree, _SUITE_FOLDER, True)
    return {"storage": storage, "storage_folder": tempfile.mkdtemp(dir=_SUITE_FOLDER)}
