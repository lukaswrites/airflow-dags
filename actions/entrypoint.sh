#!/bin/sh -l

echo "Hello $1"
time=$(date)
echo "::set-output name=time::$time"
git fetch origin
git checkout -b $GITHUB_HEAD_REF origin/$GITHUB_HEAD_REF
git --git-dir="$GITHUB_WORKSPACE/.git" log --left-right --name-only --cherry-pick --pretty=format: remotes/origin/$GITHUB_BASE_REF...remotes/origin/$GITHUB_HEAD_REF > commit_list.txt
cat commit_list.txt
python3 sql_check.py commit_list.txt