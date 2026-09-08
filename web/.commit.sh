#!/bin/sh
# usage: .commit.sh "message"
set -e
cd "$(dirname "$0")/.."
git add web
if git diff --cached --quiet; then echo "nothing to commit"; exit 0; fi
git commit -q -m "$1"
/opt/homebrew/bin/git push -q origin main 2>/dev/null || { git pull -q --rebase origin main && /opt/homebrew/bin/git push -q origin main; }
git log --oneline -1
