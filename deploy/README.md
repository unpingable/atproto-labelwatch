# Deploy

Minimal deploy for a single box (Linode, VPS, etc). Two pieces: a systemd
service that polls + scans + generates reports, and nginx serving the static
output.

## Setup

```bash
# Create user and directories
sudo useradd -r -s /bin/false labelwatch
sudo mkdir -p /opt/labelwatch /var/lib/labelwatch /var/www/labelwatch
sudo chown labelwatch:labelwatch /var/lib/labelwatch /var/www/labelwatch

# Install
cd /opt/labelwatch
sudo -u labelwatch python3 -m venv .venv
sudo -u labelwatch .venv/bin/pip install /path/to/labelwatch
sudo cp /path/to/config.toml /opt/labelwatch/config.toml
sudo chown labelwatch:labelwatch /opt/labelwatch/config.toml

# Systemd
sudo cp labelwatch.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now labelwatch

# Nginx
sudo cp labelwatch.nginx.conf /etc/nginx/sites-available/labelwatch
sudo ln -s /etc/nginx/sites-available/labelwatch /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl reload nginx
```

## Verify

```bash
sudo systemctl status labelwatch
ls /var/www/labelwatch/index.html
curl -s http://localhost/overview.json | python3 -m json.tool
```

## Known rough edges

- No log rotation configured; add `StandardOutput=journal` or pipe to a file
  with logrotate
- No HTTPS in the nginx config; add certbot or your own certs
- SQLite file grows unbounded; consider periodic `VACUUM` or archival
- No alerting/notification; check reports manually or add a cron health check
- Report regeneration is atomic (directory swap) but not zero-downtime for
  concurrent readers during the swap
