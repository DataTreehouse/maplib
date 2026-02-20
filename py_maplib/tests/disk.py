from typing import List, Optional
import os
import pathlib

PATH_HERE = pathlib.Path(__file__).parent

def disk_params() -> List[Optional[str]]:
    import dotenv
    dotenv.load_dotenv(PATH_HERE.parent.parent/".env")
    if "MAPLIB_EE" in os.environ:
        is_maplib_ee = os.environ["MAPLIB_EE"]
    else:
        is_maplib_ee = False
    if is_maplib_ee:
        return [None, "disk"]
    else:
        return [None]