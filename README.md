# Splunk Enterprise + Cisco Meraki AI Observability Platform

An end-to-end observability stack that polls Cisco Meraki APIs (environmental sensors, switches, cameras), streams data into Splunk Enterprise via HEC, and surfaces an AI-powered chatbot interface for natural-language queries against live building data.

```
Meraki API → Python Agent → Splunk HEC → Splunk Dashboards
                   ↓
           OpenRouter LLM
                   ↓
         FastAPI Chatbot UI (port 5000)
```

---

## Hardware Requirements

| Device | Purpose |
|---|---|
| Meraki MT10/MT sensors | Environmental: temperature, humidity, door state |
| Meraki MS switch (e.g. MS355) | Network: per-port traffic, PoE, WAP client counts |
| Meraki MV cameras (MV12W etc.) | People counting via MV Sense zone analytics |
| Ubuntu 22.04 server | Runs Splunk Enterprise + the observability agent |

> The agent works with whatever subset of Meraki hardware you have. Sensors, switch, and cameras are all independently optional — comment out any polling function you don't need.

---

## Software Stack

| Component | Version / Notes |
|---|---|
| Splunk Enterprise | 10.2.x (Developer License: 10 GB/day free) |
| Splunk TA for Cisco Meraki | Splunkbase: `Splunk_TA_cisco_meraki` |
| Splunk AI Toolkit | Splunkbase: `Splunk_ML_Toolkit` |
| Python for Scientific Computing | Splunkbase dependency for AI Toolkit |
| Splunk MCP Server | Splunkbase: `Splunk_MCP_Server` (optional) |
| Python 3.10+ | Agent runtime |
| FastAPI + Uvicorn | Chatbot web server |
| OpenRouter | LLM API gateway (free tier available) |

---

## Part 1 — Splunk Enterprise Installation

### 1.1 Install the .deb Package
```bash
sudo dpkg -i splunk-<version>-linux-amd64.deb
```

Download from: https://www.splunk.com/en_us/download/splunk-enterprise.html

### 1.2 First Start
```bash
sudo /opt/splunk/bin/splunk start --accept-license --run-as-root
```
Splunk prompts for admin username and password on first run.

> Running as root triggers a deprecation warning but works for lab/dev deployments.

### 1.3 Enable systemd Boot Start
```bash
sudo /opt/splunk/bin/splunk enable boot-start \
  -systemd-managed 1 \
  --run-as-root
sudo systemctl daemon-reload
sudo systemctl enable Splunkd
sudo systemctl start Splunkd
```

### 1.4 Open Firewall Ports
```bash
sudo ufw allow 8088/tcp   # Splunk HEC (HTTP Event Collector)
sudo ufw allow 8089/tcp   # Splunk management API
sudo ufw allow 5000/tcp   # Observability Agent web UI
# Adjust web UI port as needed (default Splunk web is 8000; example below uses 8008)
sudo ufw allow 8008/tcp
sudo ufw reload
```

### 1.5 Configure web.conf
```bash
sudo nano /opt/splunk/etc/system/local/web.conf
```
```ini
[settings]
httpport = 8008
server.socket_host = 0.0.0.0
enableSplunkWebSSL = false
max_upload_size = 500
response.timeout = 60
verifyCookiesWorkDuringLogin = false
root_endpoint = /
```

> **Avoid these deprecated keys** in Splunk 10.2.x — they cause startup warnings:
> `SplunkdConnectionTimeout`, `allowEmbedTokenAuth`, `hostnameAndPort`

### 1.6 Configure server.conf
```bash
sudo nano /opt/splunk/etc/system/local/server.conf
```
```ini
[general]
serverName = <your-hostname>
hostnameOption = fullyqualifiedname
Pass4SymmKey = <keep existing encrypted value>

[httpServer]
acceptFrom = *

[sslConfig]
sslPassword = <keep existing encrypted value>
```

> **Critical:** Never duplicate `[general]` stanzas. Merge all keys into one. Removing `Pass4SymmKey` breaks internal auth.

### 1.7 Apply Developer License
The Splunk Developer Personal License provides 10 GB/day ingest (vs. 500 MB trial).
Get it at: https://dev.splunk.com/enterprise/dev_license/

```bash
scp Splunk.License <user>@<server-ip>:/home/<user>/

sudo /opt/splunk/bin/splunk add licenses /home/<user>/Splunk.License \
  --run-as-root -auth <admin-user>:<admin-password>

sudo /opt/splunk/bin/splunk restart --run-as-root
```

Verify:
```bash
sudo /opt/splunk/bin/splunk list licenses --run-as-root \
  -auth <admin-user>:<admin-password> 2>/dev/null \
  | grep -E "label|quota|stack_id|status"
```
Expected: `label: Splunk Developer Personal License` | `quota: 10737418240` | `stack_id: enterprise`

### 1.8 Remote Access via SSH Tunnel
If accessing Splunk through a VPN or corporate network with DNS restrictions, an SSH tunnel is the most reliable method:

```bash
# Tunnel Splunk web UI and agent UI together
ssh -L 8008:127.0.0.1:8008 -L 5000:127.0.0.1:5000 <user>@<server-ip>
```

Then open:
- Splunk: `http://127.0.0.1:8008`
- Agent UI: `http://127.0.0.1:5000`

### 1.9 Post-Reboot Login Issue (CSRF Token)
After reboot, Splunk web may show `Server Error` on login due to stale session tokens:

```bash
sudo /opt/splunk/bin/splunk stop --run-as-root
sudo rm -rf /opt/splunk/var/run/splunk/sessions/*
sudo rm -f /opt/splunk/var/run/splunk/csrf_token
sudo /opt/splunk/bin/splunk start --run-as-root
```

Wait 60 seconds, then use incognito browser or the SSH tunnel method.

> **Root cause:** Splunk running as root causes session ownership conflicts on restart.

---

## Part 2 — Cisco Meraki Add-on

### 2.1 Install via Splunkbase
```
Apps → Find More Apps → Search "Cisco Meraki" → Install
```
Installs to: `/opt/splunk/etc/apps/Splunk_TA_cisco_meraki/`

### 2.2 Configure Your Meraki Organisation
```
Apps → Cisco Meraki Add-on → Configuration → Add
```
| Field | Value |
|---|---|
| Account Name | `<your-org-name>` |
| API Key | Your Meraki Dashboard API key |
| Organisation ID | Your Meraki org ID |

API key: `Meraki Dashboard → My Profile → API Access → Generate API Key`
Org ID: visible in the Meraki Dashboard URL or via API: `GET /organizations`

---

## Part 3 — Sensor Inputs (MT10)

### 3.1 Configure inputs.conf
```bash
sudo nano /opt/splunk/etc/apps/Splunk_TA_cisco_meraki/local/inputs.conf
```
```ini
[cisco_meraki_sensor_readings_history://<INPUT-NAME>]
index = main
interval = 1800
organization_name = <your-org-name>
start_from_days_ago = 7

[cisco_meraki_devices_availabilities://<AVAILABILITY-INPUT>]
index = main
interval = 3600
organization_name = <your-org-name>

[cisco_meraki_summary_switch_power_history://<POWER-INPUT>]
index = main
interval = 3600
start_from_days_ago = 7
organization_name = <your-org-name>

[cisco_meraki_power_modules_statuses_by_device://<PSU-INPUT>]
index = main
interval = 3600
organization_name = <your-org-name>
```

> Meraki API retains only 7 days of sensor history. Create inputs promptly after setup.

### 3.2 Key SPL Notes
- Sourcetype uses **single colon**: `meraki:sensorreadingshistory`
- Dotted field names require **single quotes** in eval:
  ```spl
  | eval display=round('temperature.celsius',1)
  ```
- Do **not** concatenate units in eval when also using the panel `unit` option — causes double units (`% %`)

---

## Part 4 — Splunk HEC Setup

### 4.1 Enable HEC
```bash
sudo /opt/splunk/bin/splunk http-event-collector enable \
  -uri https://localhost:8089 \
  -auth <admin-user>:<admin-password> --run-as-root

sudo /opt/splunk/bin/splunk restart --run-as-root
```

### 4.2 Create HEC Token
```
Settings → Data Inputs → HTTP Event Collector → New Token
→ Name: meraki-agent → Submit → Copy Token
```

Store this token in your `.env` as `SPLUNK_HEC_TOKEN`.

### 4.3 Test HEC
```bash
curl -k https://127.0.0.1:8088/services/collector \
  -H "Authorization: Splunk <your-token>" \
  -d '{"event":"HEC test","sourcetype":"test"}'
```
Expected: `{"text":"Success","code":0}`

> HEC requires HTTPS (`https://`) with `-k` to skip cert validation for self-signed certs.

---

## Part 5 — AI Apps (Splunkbase)

Install in this order (Python Scientific Computing must precede AI Toolkit):

| App | Splunkbase Name | Purpose |
|---|---|---|
| Python for Scientific Computing | `python-for-scientific-computing-for-linux-x86_64` | numpy/scipy/sklearn dependency |
| Splunk AI Toolkit | `Splunk_ML_Toolkit` | `predict`, `anomalydetection`, IsolationForest |
| Splunk MCP Server | `Splunk_MCP_Server` | MCP endpoint for LLM agent integration (optional) |

---

## Part 6 — Observability Agent Setup

### 6.1 Project Structure
```
<project-dir>/
├── observability_agent.py    # Main agent
├── .env                      # Your config (never commit this)
├── .env.example              # Template
├── observability-agent.service  # systemd unit file template
├── sensor_dashboard.xml      # Splunk Classic Dashboard — sensors
└── switch_dashboard.xml      # Splunk Classic Dashboard — switch
```

### 6.2 Python Environment
```bash
cd <project-dir>
python3 -m venv .venv
source .venv/bin/activate
pip install requests fastapi uvicorn httpx python-dotenv
```

### 6.3 Environment Configuration
Copy `.env.example` to `.env` and fill in all values:

```bash
cp .env.example .env
nano .env
```

```ini
# Meraki
MERAKI_API_KEY=<your-meraki-api-key>
MERAKI_ORG_ID=<your-meraki-org-id>
MERAKI_NETWORK_ID=<your-meraki-network-id>

# Splunk HEC
SPLUNK_HEC_URL=https://127.0.0.1:8088/services/collector
SPLUNK_HEC_TOKEN=<your-hec-token>
SPLUNK_HOST=<your-splunk-hostname>

# OpenRouter AI (https://openrouter.ai)
OPENROUTER_API_KEY=sk-or-v1-...
OPENROUTER_BASE_URL=https://openrouter.ai/api/v1
OPENROUTER_REFERER=https://<your-org-url>

# Models — free tier options shown; upgrade as needed
MODEL_ANALYST=openai/gpt-oss-120b:free
MODEL_COMPOSER=meta-llama/llama-3.3-70b-instruct:free
MODEL_FALLBACK=openai/gpt-oss-20b:free

# Polling interval (seconds)
POLL_INTERVAL=120
```

> **OpenRouter rate limiting:** Free tier models have shared rate limits. Add credits at https://openrouter.ai/settings/credits to avoid 429 errors during demos.

### 6.4 Configure Your Devices
Edit `observability_agent.py` and update the device registry at the top of the file:

```python
# ── Sensor serials ────────────────────────────────────────────────
SENSORS = [
    {"serial": "XXXX-XXXX-XXXX", "name": "Sensor 1 (Temp/Humidity)",
     "metrics": ["temperature", "humidity", "battery"]},
    {"serial": "XXXX-XXXX-XXXX", "name": "Door Sensor",
     "metrics": ["door", "battery"]},
]

# ── Switch ────────────────────────────────────────────────────────
SWITCH = {
    "serial": "XXXX-XXXX-XXXX",
    "name":   "SW01",
    "model":  "MS355-48X2",    # update to your model
    "ports": {
        "1":  "Camera 1",
        "43": "WAP 1",
        "48": "Uplink",
    }
}

# ── Cameras ───────────────────────────────────────────────────────
CAMERAS = [
    {"serial": "XXXX-XXXX-XXXX", "name": "Camera 1",
     "zones": [{"id": "0", "label": "Full Frame"}]},
]
```

Device serials are visible in: `Meraki Dashboard → Network → Devices`

### 6.5 Enable MV Sense on Cameras
People-counting requires MV Sense to be enabled per-camera:

```bash
curl -s -X PUT \
  -H "X-Cisco-Meraki-API-Key: $MERAKI_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"senseEnabled": true}' \
  "https://api.meraki.com/api/v1/devices/<CAMERA_SERIAL>/camera/sense"
```

> Allow 1–2 hours after enabling MV Sense before the first analytics bucket appears.

### 6.6 Run Manually (Test)
```bash
source .venv/bin/activate
python observability_agent.py
```

Open http://localhost:5000 to verify the chatbot UI loads.

### 6.7 Run as systemd Service
Edit `observability-agent.service` — replace `<user>` and `<project-dir>` with your values:

```bash
sudo cp observability-agent.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable observability-agent
sudo systemctl start observability-agent
sudo systemctl status observability-agent
```

Monitor logs:
```bash
sudo journalctl -u observability-agent -f
```

> **Important:** The service uses `.venv/bin/python3` — the full path to the venv Python. Do not use `/usr/bin/python3` or dependencies will be missing.

---

## Part 7 — Splunk Dashboards

Two Classic Dashboards are included. Import them via:

```
Dashboards → Create New Dashboard → Classic Dashboard
→ Edit → Source (</>) → Paste XML → Save
```

### sensor_dashboard.xml
Panels: current temperature/humidity/door/battery stats, 7-day time series, anomaly detection (`anomalydetection`), 24h forecast (`predict` with `LLP5`), threshold breach events.

**Before importing:** replace all occurrences of `SENSOR-SERIAL-1` and `SENSOR-SERIAL-2` with your actual MT sensor serials.

### switch_dashboard.xml
Panels: connected port count, total traffic, per-port traffic breakdown, PoE consumption, WAP client count.

**Before importing:** replace `SWITCH-SERIAL` with your MS switch serial.

### Forecasting Note
```spl
| predict temperature future_timespan=24 algorithm=LLP5
```
`LLP5` (Local Level with Period 5) uses exponential smoothing suited for HVAC daily cycles.

---

## Part 8 — Data Index Summary

All data lands in `index=main` by default.

| Sourcetype | Origin | Data | Interval |
|---|---|---|---|
| `meraki:sensorreadingshistory` | Meraki TA | MT sensor temp/humidity/door/battery | 30 min |
| `meraki:cameras` | Meraki TA | MV camera events | 24 hrs |
| `sensor:latest` | Agent | Latest sensor readings | 2 min |
| `switch:port_status` | Agent | Switch per-port traffic, PoE, clients | 2 min |
| `camera:analytics` | Agent | MV people count per zone | 2 min |
| `building:anomaly` | Agent | Detected anomaly events | On detection |

---

## Part 9 — Services Reference

Three processes run on the server:

| Service | Unit File | Purpose |
|---|---|---|
| Splunk Enterprise | `Splunkd` | SIEM and analytics |
| Observability Agent | `observability-agent.service` | Meraki polling + AI chatbot |

Check status:
```bash
sudo systemctl status Splunkd observability-agent
```

---

## Part 10 — Diagnostic Commands

### Splunk
```bash
# Restart
sudo /opt/splunk/bin/splunk restart --run-as-root

# Check config errors
sudo /opt/splunk/bin/splunk btool check --debug --run-as-root 2>&1 | grep -i error

# TA input logs
sudo tail -50 /opt/splunk/var/log/splunk/splunk_ta_cisco_meraki_<input_name>.log
```

### SPL Verification Queries
```spl
-- All sourcetypes and event counts
index=main | stats count by sourcetype | sort - count

-- Raw sensor data from TA
index=main sourcetype="meraki:sensorreadingshistory"
| stats count avg(value) min(value) max(value) by serial metric

-- Agent sensor data
index=main sourcetype="sensor:latest"
| stats count by sensor_name metric | sort sensor_name

-- Switch port data
index=main sourcetype="switch:port_status"
| stats count by portId status | sort portId

-- Recent anomalies
index=main sourcetype="building:anomaly"
| table _time type signal message severity
| sort - _time
```

### Agent
```bash
# Service status
sudo systemctl status observability-agent

# Live logs
sudo journalctl -u observability-agent -f

# REST API status
curl -s http://127.0.0.1:5000/api/status | python3 -m json.tool

# Trigger AI narrative
curl -s http://127.0.0.1:5000/api/narrative \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['narrative'])"

# Chat endpoint
curl -s -X POST http://127.0.0.1:5000/api/chat \
  -H "Content-Type: application/json" \
  -d '{"message": "What is the current building status?"}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['response'])"
```

---

## Part 11 — Known Issues & Resolutions

| Issue | Cause | Resolution |
|---|---|---|
| `Server Error` on Splunk login after reboot | Stale CSRF token / session ownership conflict (root-run Splunk) | Clear sessions (Part 1.9) or use SSH tunnel |
| Corporate DNS cannot resolve server hostname | DNS doesn't know your VPN/Tailscale domain | Add hosts file entry or use IP directly |
| Double units in stat panels (`% %`) | Unit concatenated in eval AND set in panel `unit` option | Remove unit suffix from eval string |
| Zero search results with correct SPL | Sourcetype uses single colon, not double | `meraki:sensorreadingshistory` not `meraki::` |
| `switch_ports_by_switch` returns 404 | Requires higher Meraki license tier | Use the Python agent HEC poller instead |
| `cisco_meraki_switches` returns count=0 | TA inventory endpoint incompatible | Use `devices_availabilities` input instead |
| HEC `Connection refused` | HEC not enabled, or using HTTP instead of HTTPS | Enable via CLI; always use `https://` with `-k` |
| `round()` eval error on dotted field names | Splunk interprets dots as nested field accessors | Wrap in single quotes: `round('temperature.celsius',1)` |
| OpenRouter 404 on chat | Model ID unavailable on free tier | Check available models at openrouter.ai; update `.env` |
| OpenRouter 429 rate limit | Free tier shared rate limit hit | Add credits at https://openrouter.ai/settings/credits |
| Camera people count shows zeros | MV Sense analytics bucket not yet populated | Use 1h+ lookback; allow 1–2h after enabling MV Sense |
| Agent `.env` not loading in systemd | Working directory mismatch | Use absolute path in `load_dotenv()`: `load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))` |
| Wrong Python in systemd service | Service points to system Python, not venv | Use `.venv/bin/python3` as `ExecStart` binary |

---

## Architecture Notes

- The Meraki TA handles bulk historical data (30-min sensor history, daily camera inventory). The Python agent handles near-real-time data (2-min polling) and AI narrative generation.
- All data lands in `index=main`. Separate indexes can be used by changing the `index` field in `send_to_splunk()` and `inputs.conf`.
- OpenRouter provides access to multiple free and paid LLMs through a single API. The agent uses three model slots: `MODEL_ANALYST` (scheduled reports), `MODEL_COMPOSER` (interactive chat), `MODEL_FALLBACK` (timeout fallback).
- The FastAPI chatbot runs on port 5000. The `/api/status` endpoint returns all current sensor state as JSON, making it easy to integrate with other systems.
- MV Sense Full Frame zone (`id: "0"`) is used for people counting. Named area zones use a different API endpoint and are not implemented here.

---

*Built at Innovation Central Perth — Curtin University*
