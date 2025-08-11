#!/usr/bin/env python3
import os
import sys
import argparse
from git import Repo, GitCommandError, InvalidGitRepositoryError

def main():
    p = argparse.ArgumentParser(description="Commit and push a CSV to GitHub.")
    p.add_argument("--file", "-f", default="panama_rent_averages.csv",
                   help="Path to CSV to commit (default: panama_rent_averages.csv)")
    p.add_argument("--message", "-m", default=None,
                   help="Commit message")
    p.add_argument("--remote", "-r", default="origin",
                   help="Remote name (default: origin)")
    p.add_argument("--branch", "-b", default=None,
                   help="Branch to push (default: current)")
    p.add_argument("--rename-old", default=None,
                   help="If the repo still has an old filename, provide it here to remove. Example: panama_rent_averages_july2025.csv")
    args = p.parse_args()

    # Open repo
    here = os.path.dirname(os.path.abspath(__file__))
    try:
        repo = Repo(here, search_parent_directories=True)
    except InvalidGitRepositoryError:
        print(f"❌ {here} is not inside a Git repo.")
        sys.exit(1)

    repo_root = repo.working_tree_dir

    # Resolve file path
    target_path = args.file if os.path.isabs(args.file) else os.path.join(repo_root, args.file)
    if not os.path.isfile(target_path):
        print(f"❌ File not found: {target_path}")
        sys.exit(1)

    rel_target = os.path.relpath(target_path, repo_root)

    # Pick branch
    branch = args.branch or getattr(repo.active_branch, "name", None)
    if not branch:
        print("❌ Detached HEAD. Use --branch to specify a branch.")
        sys.exit(1)

    # Fetch and rebase to avoid non-fast-forward errors
    remote = repo.remote(args.remote)
    try:
        remote.fetch()
        repo.git.rebase(f"{args.remote}/{branch}")
    except GitCommandError as e:
        print("ℹ️ Rebase attempt failed or not needed:", e)

    # Optional: remove old file name from repo so you do not keep both
    if args.rename_old:
        old_path = os.path.join(repo_root, args.rename_old)
        if os.path.exists(old_path):
            repo.index.remove([os.path.relpath(old_path, repo_root)], working_tree=True)
        else:
            # If it only exists in repo but not in working tree, try removing by path anyway
            try:
                repo.index.remove([args.rename_old])
            except Exception:
                pass

    # Stage new file
    repo.index.add([rel_target])

    # Skip empty commit
    if not repo.is_dirty(index=True, working_tree=False, untracked_files=True):
        print("ℹ️ No changes to commit.")
        sys.exit(0)

    msg = args.message or f"Update {os.path.basename(rel_target)}"
    commit = repo.index.commit(msg)
    print(f"✅ Committed {rel_target} as {commit.hexsha[:10]} on {branch}")

    # Push
    try:
        results = remote.push(f"{branch}:{branch}")
        info = results[0]
        if info.flags & info.ERROR:
            print("❌ Push error:", info.summary)
            sys.exit(1)
        print(f"✅ Pushed to {args.remote}/{branch}: {info.summary}")
    except GitCommandError as e:
        print("❌ Push failed:", e)
        sys.exit(1)

if __name__ == "__main__":
    main()

if __name__ == "__main__":
    main()

