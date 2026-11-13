#!/usr/bin/env bash
# SessionStart hook: tells the assistant which lesson branch is checked out and
# where its contract is. Output on stdout is added to the session context.
branch="$(git branch --show-current 2>/dev/null)"
[ -z "$branch" ] && exit 0

echo "Tadka repo. Current branch: ${branch}."
if [[ "$branch" =~ ^lesson-([0-9]+) ]]; then
  contract="docs/lessons/lesson-${BASH_REMATCH[1]}-contract.md"
  if [ -f "$contract" ]; then
    echo "Lesson contract: ${contract}. Read it before building, porting or running anything."
  else
    echo "No contract file found at ${contract}."
  fi
else
  echo "Not a lesson branch. Check out a lesson-NN-* branch to work on a lab."
fi
ports="$(ls -d ports/*/ 2>/dev/null | tr '\n' ' ')"
[ -n "$ports" ] && echo "Existing ports: ${ports}"
exit 0
