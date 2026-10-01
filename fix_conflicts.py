"""
Remove Git merge-conflict markers from JSON files IN PLACE (file names stay the same).

Usage (run from your repo folder):
    python fix_conflicts.py            # fixes every .json file under the current folder
    python fix_conflicts.py samples    # or only a specific folder

For each broken file it keeps the first ("HEAD") copy, deletes the markers and the duplicate
copy, and only saves the file if the result is valid JSON.
"""
import json
import os
import re
import sys

root = sys.argv[1] if len(sys.argv) > 1 else "."
PATTERN = re.compile(r"<<<<<<< [^\n]*\n(.*?)\n=======\n(.*?)\n>>>>>>> [^\n]*\n(.*)$", re.S)

fixed = skipped = 0
for folder, dirs, files in os.walk(root):
    dirs[:] = [d for d in dirs if d != ".git"]
    for name in files:
        path = os.path.join(folder, name)
        if not name.endswith(".json"):
            continue
        with open(path, encoding="utf-8") as f:
            text = f.read()
        if "<<<<<<<" not in text:
            continue
        m = PATTERN.match(text)
        if not m:
            print("COULD NOT FIX (unusual layout):", path)
            skipped += 1
            continue
        ours, theirs, tail = m.groups()
        result = ours + "\n" + tail
        if ours != theirs:
            print("WARNING: the two sides differ in", path, "- kept the first (HEAD) side")
        try:
            json.loads(result)
        except ValueError as e:
            print("COULD NOT FIX (result is not valid JSON):", path, e)
            skipped += 1
            continue
        with open(path, "w", encoding="utf-8") as f:
            f.write(result)
        print("fixed:", path)
        fixed += 1

print(f"\nDone. Fixed {fixed} file(s), {skipped} problem(s).")
