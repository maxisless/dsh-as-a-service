# dsh-as-a-service Agent Instructions

## Delivery model

- The local Mac checkout is the only development and test environment. Make and verify all public-source changes here first.
- A server deployment may contain only a commit that has been tested locally and pushed to GitHub `origin/main`.
- The server is a deployment target, not an editing environment. Do not hot-edit tracked application code in the server checkout or copy individual public source files there with SCP.
- After a successful local test run: inspect `git diff --check`, commit the focused change, push `main`, then let the server deployment timer pull the fast-forward GitHub commit and rebuild the Worker.
- Verify a release from the server after pushing: deployment journal, checked-out commit, and `GET /health`. Do not treat a successful `git push` alone as a completed deployment.
- An emergency server fix must be reproduced in this local checkout, tested, committed, and pushed immediately; do not leave it only on the server.

## Local verification before push

Run the checks that cover the changed surface:

- Python Worker: `cd implementations/python && python3 -m unittest discover -s tests -v`.
- TypeScript Gateway: `cd implementations/typescript && npm test`.
- Docker/runtime changes: build or bring up the local Compose stack when practical, then check `GET /health`.
- Always run `git diff --check` before committing.

## Public versus private boundary

This repository is public source only. Never commit or copy into it:

- API keys, app secrets, tokens, credentials, user data, conversation memory, generated media, or runtime logs;
- model endpoint IDs, server addresses, server-local paths, or deployment-only configuration;
- instance-specific Feishu configuration, private Skills, or private Docker Compose overrides.

The repository may contain a generic, configuration-driven channel integration
under `integrations/`. Such code must not contain a bot name, tenant or user
information, credentials, server-local paths, installed Skills, endpoint IDs,
or runtime state. Every concrete channel profile and secrets file remains
outside this Git worktree.

On the server, private configuration and extension state remain outside this Git worktree. The public Compose file is combined at runtime with an untracked private override; the deployment timer updates only the tracked GitHub checkout and preserves that private layer.
Private source code that needs deployment (such as a bot bridge or account-specific extension) belongs in a separate **private** GitHub repository and follows the same local-test → push → server-pull process. Only its secrets, runtime state, and deployment-only configuration remain server-local.

## Server deployment contract

- Only `origin/main` is deployable.
- Deployment uses `git fetch` plus fast-forward-only integration; a dirty tracked server worktree blocks deployment instead of overwriting local changes.
- The deployment rebuilds the Docker Worker and waits for its loopback health endpoint. If the new revision fails to become healthy, it rolls the tracked public checkout back to the previously running commit and rebuilds that revision.
- Private state and private mounts are never reset or pulled from GitHub.

## Scope boundaries

- Keep the Python Worker as the stable execution plane and TypeScript Gateway experimental unless the change explicitly promotes it.
- Preserve the public HTTP/SSE protocol in `protocol/http-contract.json` when changing either implementation.
- Keep session memory per HTTP session and preserve the existing model-binding semantics.
