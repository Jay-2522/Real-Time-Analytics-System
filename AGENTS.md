# Repository instructions

These instructions apply to every task in the Real-Time Threat Detection &
Analytics System repository. Keep the repository clean, minimal, secure, and
production-focused. Every committed file must have a defensible project purpose.

## Branch and release policy

- Work exclusively on `dev`. Verify the repository, branch, upstream, remote,
  and working-tree status before making changes.
- Commit and push completed, verified changes to `origin/dev`.
- Never modify, push, or merge into `main`. Keep it as the protected
  baseline until the agreed production-readiness requirements are satisfied.
- Promotion requires explicit approval and a reviewed pull request from `dev`
  to `main`; do not merge it automatically.
- Never deploy `dev` as the approved production release.
- Never force-push, rebase shared history, or otherwise rewrite existing history.
- If branch state conflicts with this policy, inspect and report it; do not reset,
  overwrite, or discard existing work to proceed.

## Commit scope

Keep changes minimal and directly related to the assigned task. Commit only
explicitly authorized, task-related paths, such as required application source,
backend/frontend components, configuration, intentionally changed dependency
manifests and lockfiles, required migrations, essential production/deployment
configuration, explicitly requested documentation, and essential regression tests.

Do not commit or push unnecessary development artifacts:

- Test execution reports, temporary results, `.pytest_cache`, `.coverage`, or
  HTML coverage reports.
- Python `__pycache__`, virtual environments, `node_modules`, build caches,
  temporary build outputs, or temporary testing scripts.
- Debug logs, temporary screenshots/recordings, generated sample datasets,
  local Docker data, or database volumes.
- Local environment files such as `.env`, API keys, passwords, tokens,
  certificates, or other secrets.
- Unnecessary Markdown reports, unrelated formatting changes, or files unrelated
  to the requested feature or fix.

Do not delete legitimately tracked project files merely because they match one
of these categories. Inspect their purpose and report cleanup recommendations
separately. Preserve pre-existing changes; never perform destructive cleanup
without authorization.

## Verification and regression tests

- Run all tests relevant to correctness, security, compatibility, and reliability
  of the change before committing. Record actual commands and outcomes.
- Do not commit temporary testing files or execution artifacts.
- Do not add permanent tests merely to increase test counts.
- When a focused regression test is essential to prevent a critical defect from
  recurring, include it as important project source and explain why it must be
  version-controlled. Do not sacrifice long-term correctness to minimize files.
- If critical tests fail, report the blockers. Do not commit/push broken work as
  completed or claim verification that did not occur. Distinguish pre-existing
  failures, change-related failures, skipped checks, and blocked checks.

## Mandatory review before every commit

1. Run `git status` and inspect `git diff`, including relevant untracked files.
2. Identify every modified file and separate required changes from unrelated work.
3. Preserve all unrelated existing work without overwriting or discarding it.
4. Stage only explicitly approved, task-related paths with `git add -- <paths>`.
   Never blindly use `git add .` or `git add -A`.
5. Inspect `git diff --cached` and the staged path list. Check for credentials,
   secrets, temporary artifacts, unnecessary files, and unrelated changes.
6. Check the staged diff for whitespace errors and confirm relevant tests passed.
7. Create one focused commit with a clear, descriptive message, then push to
   `origin/dev` without force. Verify the remote commit and upstream afterward.
8. If there are no legitimate changes, do not create an empty commit.

## Required final report for every task

Report all of the following, using "none" or "not applicable" when appropriate:

1. Source files modified.
2. Essential files committed.
3. Files intentionally excluded from the commit.
4. Tests/checks executed and their actual results.
5. Remaining issues, failures, skipped checks, or blockers.
6. Git commit hash, or why no commit was created.
7. Whether the commit was successfully pushed to `origin/dev`.
8. Confirmation that `main` remains untouched.
