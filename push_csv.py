#!/usr/bin/env python3
import os, sys
from git import Repo, GitCommandError

repo_path = os.path.dirname(os.path.abspath(__file__))
try:
    repo = Repo(repo_path)
except:
    print(f"❌ {repo_path} is not a Git repo.")
    sys.exit(1)

csv_filename = "panama_rent_averages_july2025.csv"
csv_path = os.path.join(repo_path, csv_filename)
if not os.path.isfile(csv_path):
    print(f"❌ {csv_filename} not found.")
    sys.exit(1)

try:
    repo.index.add([csv_filename])
    repo.index.commit(f"Add/update {csv_filename}")
    origin = repo.remote('origin')
    branch = repo.active_branch.name
    info = origin.push(f"{branch}:{branch}")[0]
    if info.flags & info.ERROR:
        print("❌ Push failed:", info.summary)
    else:
        print("✅ Pushed:", info.summary)
except GitCommandError as e:
    print("❌ Git error:", e)
    sys.exit(1)


# Display the dataframe to the user
import ace_tools as tools; tools.display_dataframe_to_user(name="Panama Rent Averages July 2025", dataframe=df)
