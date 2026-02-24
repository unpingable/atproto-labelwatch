# Next

Incremental work, in order. Core loop and receipts are done.

1. **Cursor persistence** — save/restore polling cursor across restarts. Small DB table, wire into ingest.

2. **Richer metrics** — target concentration, novel-target ratio, churn index, silence-then-surge. All SQL over label_events + new rules following existing pattern.

3. **Report UI pass** — health cards, sparklines, behavioral badges, anomaly highlighting. Static HTML, small JS charting lib. SRE dashboard feel, not accusation UI. Describe geometry, not intent.

4. **Deploy** — systemd unit or docker-compose entry on Linode, NGINX vhost for static reports, resource limits. One box, one SQLite, one generated site.
