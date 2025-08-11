#!/usr/bin/env python3
import os
import sys
import argparse
from git import Repo, GitCommandError, InvalidGitRepositoryError

def main():
    p = argparse.ArgumentParser(description="Commit and push a CSV to the repo.")
    p.add_argument("--file", "-f", default="panama_rent_averages.csv",
                   help="Path to the CSV to commit (default: panama_rent_averages.csv)")
    p.add_argument("--message", "-m", default=None,
                   help="Commit message (default auto-generated)")
    p.add_argument("--branch", "-b", default=None,
                   help="Branch to push (default: current active branch)")
    p.add_argument("--remote", "-r", default="origin",
                   help="Remote name (default: origin)")
    args = p.parse_args()

    # Locate repo root
    script_dir = os.path.dirname(os.path.abspath(__file__))
    try:
        repo = Repo(script_dir, search_parent_directories=True)
    except InvalidGitRepositoryError:
        print(f"❌ {script_dir} is not inside a Git repository.")
        sys.exit(1)

    repo_root = repo.working_tree_dir

    # Resolve file path and validate
    file_path = args.file if os.path.isabs(args.file) else os.path.join(repo_root, args.file)
    if not os.path.isfile(file_path):
        print(f"❌ File not found: {file_path}")
        sys.exit(1)

    # Path relative to repo root for git index
    rel_path = os.path.relpath(file_path, repo_root)

    # Stage
    repo.index.add([rel_path])

    # If nothing changed, bail out nicely
    has_changes = repo.is_dirty(index=True, working_tree=False, untracked_files=True)
    if not has_changes:
        print("ℹ️ No changes to commit.")
        sys.exit(0)

    # Commit
    commit_msg = args.message or f"Add/update {os.path.basename(rel_path)}"
    commit = repo.index.commit(commit_msg)
    print(f"✅ Committed {rel_path} as {commit.hexsha[:10]}")

    # Determine branch
    branch_name = args.branch
    if not branch_name:
        try:
            branch_name = repo.active_branch.name
        except TypeError:
            print("❌ Detached HEAD. Use --branch to specify a branch.")
            sys.exit(1)

    # Push
    try:
        remote = repo.remote(args.remote)
    except ValueError:
        print(f"❌ Remote not found: {args.remote}")
        sys.exit(1)

    try:
        results = remote.push(f"{branch_name}:{branch_name}")
    except GitCommandError as e:
        print("❌ Push failed:", e)
        sys.exit(1)

    info = results[0]
    if info.flags & info.ERROR:
        print("❌ Push error:", info.summary)
        sys.exit(1)
    else:
        print(f"✅ Pushed to {args.remote}/{branch_name}: {info.summary}")

if __name__ == "__main__":
    main()

