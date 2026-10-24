# Cowrie honeypot VPS

Run this deployment only on a separate, disposable VPS. A honeypot is an
internet-facing target: do not colocate it with the analytics stack, personal
data, production workloads, cloud credentials, or trusted networks. Never put
real credentials in Cowrie. The container runs without Linux capabilities and
with a read-only root filesystem, but the VPS must still be treated as hostile.

## Safe setup

1. Patch the VPS, install Docker with Compose, and create `/opt/cowrie`.
2. Before exposing Cowrie, move the real administration SSH daemon off port 22,
   require SSH keys, disable password and root login, and verify a second admin
   session works on the new port. Do not close the first session until verified.
3. Copy this directory to `/opt/cowrie`, then prepare the bind mount for the
   image's unprivileged UID:

   ```sh
   sudo mkdir -p /opt/cowrie/var
   sudo chown -R 999:999 /opt/cowrie/var
   sudo chmod 750 /opt/cowrie/var
   ```

4. Default-deny the host and provider firewalls. Permit the admin SSH port only
   from a trusted administrator address, permit TCP/22 from the internet for
   Cowrie, and deny every other inbound port. Replace both placeholders below;
   they are not example credentials or addresses:

   ```sh
   sudo ufw default deny incoming
   sudo ufw default allow outgoing
   sudo ufw allow from <TRUSTED_ADMIN_ADDRESS> to any port <ADMIN_SSH_PORT> proto tcp
   sudo ufw allow 22/tcp
   sudo ufw enable
   ```

   Docker-published ports can bypass some host-firewall paths. Enforce the same
   allowlist in the VPS provider firewall and, where applicable, the Docker
   `DOCKER-USER` chain. Confirm from another host that only the intended ports
   are reachable.
5. Start Cowrie and check that the JSON audit log is being written:

   ```sh
   cd /opt/cowrie
   docker compose -f compose.yml up -d
   sudo test -f var/log/cowrie/cowrie.json
   ```

Cowrie listens inside the container on 2222; Compose publishes it as VPS port
22. Telnet is disabled. Do not enable Cowrie proxy backends, mount the Docker
socket, add host networking, or give the container access to trusted services.
Downloaded artifacts and session data under `var/` are untrusted; never execute
them, and keep the directory out of version control.

## Rotation and collection

Cowrie uses a plain JSON log so host `logrotate` owns retention. The included
policy rotates daily or at 25 MiB, retains 14 compressed rotations, and uses
`copytruncate` so Cowrie keeps writing without a restart:

```sh
sudo cp /opt/cowrie/logrotate.conf /etc/logrotate.d/cowrie
sudo logrotate -d /etc/logrotate.d/cowrie
```

Copy only logs (not keys, downloaded files, or the `etc` volume) to the machine
that runs the analytics pipeline. Use a dedicated SSH key with minimum access:

```sh
rsync -av --partial -e 'ssh -i <DEDICATED_KEY> -p <ADMIN_SSH_PORT>' \
  <VPS_USER>@<VPS_HOST>:/opt/cowrie/var/log/cowrie/cowrie.json* ./cowrie-logs/
```

For a one-off copy, use:

```sh
scp -i <DEDICATED_KEY> -P <ADMIN_SSH_PORT> \
  <VPS_USER>@<VPS_HOST>:/opt/cowrie/var/log/cowrie/cowrie.json ./cowrie.json
```

Restrict the local copies because login attempts can contain attacker-supplied
usernames and passwords. The ingestion adapter deliberately discards passwords
before publishing events, but the original Cowrie log still contains them.
