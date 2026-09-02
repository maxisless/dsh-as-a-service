# Generic Feishu channel deployment

`integrations/feishu/` is an optional transport for a DSH Worker. It receives
Feishu messages, maps a conversation to one server-issued session, safely stages
per-Run media inputs, renders a streaming Card 2.0 response, and delivers
registered Worker artifacts. It has no dependency on a particular bot, tenant,
Skill, model endpoint, or server path.

## Instance layout

Keep every concrete instance outside the public Git checkout:

```text
/secure/dsh-instances/<instance-id>/
  profile.json       # portable, non-secret behaviour and Agent binding
  secrets.env        # mode 0600: app secret and bridge token
  state/             # deduplication DB and temporary attachments
```

`profile.json` uses [the example schema](../integrations/feishu/profile.example.json).
It selects the display name, Worker URL/container, Agent ID/version, mention
policy, contact enrichment, and limits. Credential fields only name environment
variables; they never contain values. Replace the example `worker.container`
placeholder with the actual local Worker container name before enabling media
input support.

`secrets.env` follows [the example](../integrations/feishu/secrets.env.example)
but is not copied into the checkout or committed. Set mode `0600`. A profile
can move between machines; recreate or inject the secret file from a secret
manager. Generate a distinct bridge token for every Worker deployment.

## Fresh-machine bootstrap

1. Clone the tested public revision to an installation root and start the
   Worker with its deployment-only model configuration. Set the same
   `DSH_INTERNAL_BRIDGE_TOKEN` in the Worker environment and the channel
   instance's `secrets.env`.
2. Create a non-secret profile from the example and a mode-0600 secrets file
   in a secure configuration root. The `worker.container` field is required
   for inbound media because the current trusted single-node adapter uses
   `docker cp` into the Run-specific inbox.
3. Run the generic installer as root:

   ```bash
   sudo ./deploy/feishu/install-channel.sh \
     --profile /secure/dsh-instances/example/profile.json \
     --secrets-file /secure/dsh-instances/example/secrets.env \
     --install-root /srv/dsh-as-a-service \
     --config-root /etc/dsh/feishu/example \
     --state-root /var/lib/dsh-feishu-channel/example \
     --service-user dshbridge
   ```

   It validates the profile, installs the optional Feishu SDK into an isolated
   virtual environment, creates fresh channel state, writes a hardened systemd
   unit, and enables `dsh-feishu-channel@dshbridge.service`. Pass `--no-enable`
   to inspect the installed unit before it connects.

For a brand-new host, use the top-level bootstrap script after preparing the
three private Worker assets (`worker.env`, `models.json`, and `dsh-home`) and
the optional channel profile/secrets outside the checkout:

```bash
sudo ./deploy/bootstrap-machine.sh \
  --repo-url https://github.com/owner/dsh-as-a-service.git \
  --install-root /srv/dsh-as-a-service \
  --worker-env /secure/dsh/worker.env \
  --models-file /secure/dsh/models.json \
  --dsh-home /secure/dsh/dsh-home \
  --channel-profile /secure/dsh-instances/example/profile.json \
  --channel-secrets /secure/dsh-instances/example/secrets.env
```

The bootstrap only accepts a fresh installation root. It clones the requested
public revision, mounts private Worker configuration from its original secure
location, checks the local health endpoint, then delegates the optional channel
installation to `install-channel.sh`.

Do not run the old and new bridge for the same Feishu app concurrently. The
current single-node deduplication database is local to one bridge host. Stop
the previous service first, then enable the new one. Do not copy the old bridge
SQLite state or attachment directories; the Worker owns durable conversation and
Run state, while the bridge starts with a clean delivery/deduplication store.

## Operational boundaries

- The generic bridge only passes trusted channel metadata and workspace-relative
  media paths to the Agent. It never exposes Feishu resource keys, hashes, or
  server/container paths in prompts.
- The bridge accepts generated artifacts only by opaque Worker artifact IDs and
  downloads them through the internal authenticated Worker endpoint.
- When `agent.version` is present, the profile pins that Agent version during
  external-conversation binding. Leaving it `null` explicitly opts into the
  Worker-managed current published version for that Agent ID.
- The inbound-media Docker adapter is a trusted, same-host transitional path. A
  future multi-node deployment should replace it with signed object storage and
  executor-side materialization; the public HTTP control-plane protocol stays
  unchanged.
- The bridge service needs Docker-socket access for this transitional media
  adapter. The installer adds its dedicated service account to the socket's
  group; treat that group as privileged host access and restrict who can edit
  its profile and secrets.
