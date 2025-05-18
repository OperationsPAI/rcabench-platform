from pathlib import Path
from pprint import pprint

PROJECT = Path(__file__).resolve().parent.parent.parent.parent

TEMP = PROJECT / "temp"

if __name__ == "__main__":
    pprint(
        {
            "PROJECT": PROJECT,
            "TEMP": TEMP,
        }
    )
