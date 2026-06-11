#!/usr/bin/env python3
"""
Meraki + Splunk Observability Agent
=====================================
Multi-signal AI-powered observability for a Cisco Meraki-instrumented building.

Data sources:
  - Meraki MV cameras    (people count via MV Sense zone analytics)
  - Meraki MT sensors    (temperature, humidity, door state)
  - Meraki MS switch     (port traffic, PoE, WAP client count)

Pipeline:
  Poll Meraki APIs → POST to Splunk HEC → OpenRouter AI generates narratives
  FastAPI web server exposes chatbot UI and REST endpoints

Requirements:
  pip install requests fastapi uvicorn httpx python-dotenv

Environment variables (.env):
  MERAKI_API_KEY        - Meraki Dashboard API key
  MERAKI_ORG_ID         - Meraki organisation ID
  MERAKI_NETWORK_ID     - Meraki network ID
  SPLUNK_HEC_URL        - e.g. https://127.0.0.1:8088/services/collector
  SPLUNK_HEC_TOKEN      - Splunk HEC token
  SPLUNK_HOST           - hostname tag written to Splunk events
  OPENROUTER_API_KEY    - OpenRouter API key
  OPENROUTER_BASE_URL   - https://openrouter.ai/api/v1
  OPENROUTER_REFERER    - your site/project URL
  MODEL_ANALYST         - model for scheduled analysis
  MODEL_COMPOSER        - model for interactive chat
  MODEL_FALLBACK        - fallback model on timeout
  POLL_INTERVAL         - seconds between polls (default 120)

Usage:
  python observability_agent.py
  Then open http://localhost:5000
"""

import os
import json
import time
import threading
import urllib3
from datetime import datetime, timezone

import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import uvicorn
from dotenv import load_dotenv

# Suppress SSL warnings for local Splunk HEC (self-signed cert)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Explicit path so load_dotenv works correctly when run as a systemd service
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

MERAKI_API_KEY   = os.getenv("MERAKI_API_KEY",   "")
SPLUNK_HEC_URL   = os.getenv("SPLUNK_HEC_URL",   "https://127.0.0.1:8088/services/collector")
SPLUNK_HEC_TOKEN = os.getenv("SPLUNK_HEC_TOKEN", "")
SPLUNK_HOST      = os.getenv("SPLUNK_HOST",       "splunk-server")
POLL_INTERVAL    = int(os.getenv("POLL_INTERVAL", "120"))
ORG_ID           = os.getenv("MERAKI_ORG_ID",     "")
NETWORK_ID       = os.getenv("MERAKI_NETWORK_ID", "")

MERAKI_BASE_URL = "https://api.meraki.com/api/v1"
MERAKI_HEADERS  = {
    "X-Cisco-Meraki-API-Key": MERAKI_API_KEY,
    "Content-Type": "application/json",
    "Accept":       "application/json",
}

# ── Device registry — CONFIGURE FOR YOUR DEPLOYMENT ──────────────────────────
# Find serials in: Meraki Dashboard → Network → Devices
SENSORS = [
    {"serial": "XXXX-XXXX-XXXX", "name": "Sensor 1 (Temp/Humidity)",
     "metrics": ["temperature", "humidity", "battery"]},
    {"serial": "XXXX-XXXX-XXXX", "name": "Door Sensor",
     "metrics": ["door", "battery"]},
]

SWITCH = {
    "serial": "XXXX-XXXX-XXXX",
    "name":   "SW01",
    "model":  "MS355-48X2",
    "ports": {
        "1":  "Camera 1",
        "2":  "Camera 2",
        "13": "Camera 3",
        "43": "WAP 1",
        "45": "WAP 2",
        "48": "Uplink",
    }
}

# Full Frame zone (id "0") for all cameras — named area zones use a different endpoint
CAMERAS = [
    {"serial": "XXXX-XXXX-XXXX", "name": "Camera 1",
     "zones": [{"id": "0", "label": "Full Frame"}]},
    {"serial": "XXXX-XXXX-XXXX", "name": "Camera 2",
     "zones": [{"id": "0", "label": "Full Frame"}]},
]

PRIMARY_SENSOR_SERIAL = SENSORS[0]["serial"] if SENSORS else ""
WAP_PORT_IDS = [pid for pid, name in SWITCH["ports"].items() if "WAP" in name]
UPLINK_PORT  = list(SWITCH["ports"].keys())[-1] if SWITCH["ports"] else "48"

# ── In-memory state ───────────────────────────────────────────────────────────
state = {
    "last_poll":      None,
    "temperature":    None,
    "humidity":       None,
    "door_open":      None,
    "wap_clients":    {},
    "port_traffic":   {},
    "port_poe":       {},
    "people_count":   {},
    "anomalies":      [],
    "last_narrative": None,
    "poll_count":     0,
    "errors":         [],
}

# ── Meraki API helper ─────────────────────────────────────────────────────────
def meraki_get(path: str, params=None):
    url = f"{MERAKI_BASE_URL}{path}"
    r = requests.get(url, headers=MERAKI_HEADERS, params=params, timeout=15)
    if not r.ok:
        raise requests.HTTPError(
            f"{r.status_code} {r.reason} {r.text[:200]}", response=r)
    return r.json()

# ── Splunk HEC sender ─────────────────────────────────────────────────────────
def send_to_splunk(events: list, sourcetype: str):
    if not SPLUNK_HEC_TOKEN:
        return
    headers = {
        "Authorization": f"Splunk {SPLUNK_HEC_TOKEN}",
        "Content-Type":  "application/json",
    }
    ts      = datetime.now(timezone.utc).timestamp()
    payload = ""
    for event in events:
        record = {
            "time":       ts,
            "sourcetype": sourcetype,
            "index":      "main",
            "host":       SPLUNK_HOST,
            "event":      event,
        }
        payload += json.dumps(record) + "\n"
    try:
        r = requests.post(SPLUNK_HEC_URL, headers=headers,
                          data=payload, timeout=15, verify=False)
        return r.status_code
    except Exception as e:
        state["errors"].append(f"HEC error: {e}")

# ── Poll: MT sensors ──────────────────────────────────────────────────────────
def poll_sensors():
    try:
        data = meraki_get(
            f"/organizations/{ORG_ID}/sensor/readings/latest",
            params={"serials[]": [s["serial"] for s in SENSORS]},
        )
        events = []
        for sensor_data in data:
            serial      = sensor_data.get("serial")
            sensor_name = next(
                (s["name"] for s in SENSORS if s["serial"] == serial), serial)
            for reading in sensor_data.get("readings", []):
                metric = reading.get("metric")
                event  = {
                    "serial":      serial,
                    "sensor_name": sensor_name,
                    "metric":      metric,
                    "ts":          reading.get("ts"),
                }
                if metric == "temperature":
                    val = reading["temperature"]["celsius"]
                    event["temperature_celsius"] = val
                    event["value"] = val
                    if serial == PRIMARY_SENSOR_SERIAL:
                        state["temperature"] = val
                elif metric == "humidity":
                    val = reading["humidity"]["relativePercentage"]
                    event["humidity_pct"] = val
                    event["value"] = val
                    if serial == PRIMARY_SENSOR_SERIAL:
                        state["humidity"] = val
                elif metric == "door":
                    val = reading["door"]["open"]
                    event["door_open"] = val
                    event["value"]     = 1 if val else 0
                    state["door_open"] = val
                elif metric == "battery":
                    val = reading["battery"]["percentage"]
                    event["battery_pct"] = val
                    event["value"]       = val
                events.append(event)
        send_to_splunk(events, "sensor:latest")
        return len(events)
    except Exception as e:
        state["errors"].append(f"Sensor poll error: {e}")
        return 0

# ── Poll: MS switch ports ─────────────────────────────────────────────────────
def poll_switch():
    try:
        ports  = meraki_get(f"/devices/{SWITCH['serial']}/switch/ports/statuses")
        events = []
        for port in ports:
            port_id   = str(port.get("portId", ""))
            port_name = SWITCH["ports"].get(port_id, f"Port {port_id}")
            event = {
                "serial":              SWITCH["serial"],
                "device_name":         SWITCH["name"],
                "model":               SWITCH["model"],
                "portId":              port_id,
                "port_name":           port_name,
                "status":              port.get("status"),
                "speed":               port.get("speed", ""),
                "clientCount":         port.get("clientCount", 0),
                "powerUsageInWh":      port.get("powerUsageInWh", 0.0),
                "trafficInKbps_total": port.get("trafficInKbps", {}).get("total", 0.0),
                "trafficInKbps_sent":  port.get("trafficInKbps", {}).get("sent",  0.0),
                "trafficInKbps_recv":  port.get("trafficInKbps", {}).get("recv",  0.0),
                "poe_allocated":       port.get("poe", {}).get("isAllocated", False),
                "lldp_name":           port.get("lldp", {}).get("systemName", ""),
            }
            if port_id in WAP_PORT_IDS:
                state["wap_clients"][port_id] = port.get("clientCount", 0)
            if port.get("status") == "Connected":
                state["port_traffic"][port_id] = \
                    port.get("trafficInKbps", {}).get("total", 0.0)
                state["port_poe"][port_id] = port.get("powerUsageInWh", 0.0)
            events.append(event)
        send_to_splunk(events, "switch:port_status")
        return len(events)
    except Exception as e:
        state["errors"].append(f"Switch poll error: {e}")
        return 0

# ── Poll: MV camera zone history (last 1 hour) ───────────────────────────────
def poll_cameras():
    total = 0
    now   = int(datetime.now(timezone.utc).timestamp())
    t0    = now - 3600

    for cam in CAMERAS:
        for zone in cam["zones"]:
            try:
                history = meraki_get(
                    f"/devices/{cam['serial']}/camera/analytics/zones"
                    f"/{zone['id']}/history",
                    params={"t0": t0, "t1": now},
                )
                initialising = not history

                if history:
                    non_zero = [h for h in history
                                if h.get("averageCount", 0) > 0 or h.get("entrances", 0) > 0]
                    latest    = non_zero[-1] if non_zero else history[-1]
                    people    = latest.get("averageCount", 0.0)
                    entrances = latest.get("entrances",    0)
                    start_ts  = latest.get("startTs", "")
                    end_ts    = latest.get("endTs",   "")
                else:
                    people = entrances = 0
                    start_ts = end_ts = ""

                state["people_count"][f"{cam['name']}_{zone['label']}"] = {
                    "count":        people,
                    "entrances":    entrances,
                    "camera":       cam["name"],
                    "zone":         zone["label"],
                    "initialising": initialising,
                }

                if not initialising:
                    send_to_splunk([{
                        "camera_serial": cam["serial"],
                        "camera_name":   cam["name"],
                        "zone_id":       zone["id"],
                        "zone_label":    zone["label"],
                        "averageCount":  people,
                        "entrances":     entrances,
                        "startTs":       start_ts,
                        "endTs":         end_ts,
                    }], "camera:analytics")
                total += 1

            except Exception as e:
                state["errors"].append(f"Camera {cam['name']} zone {zone['label']}: {e}")
    return total

# ── Anomaly detection ─────────────────────────────────────────────────────────
def detect_anomalies():
    anomalies = []
    temp   = state.get("temperature")
    hum    = state.get("humidity")
    uplink = state["port_traffic"].get(UPLINK_PORT, 0)

    if temp is not None and temp > 24.0:
        anomalies.append({
            "type":      "THRESHOLD_BREACH",
            "signal":    "temperature",
            "value":     temp,
            "threshold": 24.0,
            "message":   f"Temperature {temp:.1f}°C exceeds 24°C threshold",
            "severity":  "HIGH" if temp > 28 else "MEDIUM",
        })

    if hum is not None and hum > 70.0:
        anomalies.append({
            "type":      "THRESHOLD_BREACH",
            "signal":    "humidity",
            "value":     hum,
            "threshold": 70.0,
            "message":   f"Humidity {hum:.0f}% exceeds 70% threshold",
            "severity":  "MEDIUM",
        })

    if state.get("door_open"):
        door_name = next((s["name"] for s in SENSORS
                          if "door" in s["name"].lower()), "Door sensor")
        anomalies.append({
            "type":     "ACCESS_EVENT",
            "signal":   "door",
            "value":    1,
            "message":  f"{door_name} is currently open",
            "severity": "INFO",
        })

    total_wap = sum(state["wap_clients"].values())
    if total_wap > 10:
        anomalies.append({
            "type":     "OCCUPANCY_HIGH",
            "signal":   "wap_clients",
            "value":    total_wap,
            "message":  f"High occupancy: {total_wap} WiFi clients on WAPs",
            "severity": "INFO",
        })

    if uplink > 5000:
        anomalies.append({
            "type":     "TRAFFIC_HIGH",
            "signal":   "uplink_traffic",
            "value":    uplink,
            "message":  f"High uplink traffic: {uplink:.0f} Kbps",
            "severity": "MEDIUM",
        })

    state["anomalies"] = anomalies
    if anomalies:
        send_to_splunk(anomalies, "building:anomaly")
    return anomalies

# ── Build context snapshot for AI ────────────────────────────────────────────
def build_context() -> str:
    ts        = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    temp      = state.get("temperature")
    hum       = state.get("humidity")
    door      = state.get("door_open")
    wap       = sum(state["wap_clients"].values())
    uplink    = state["port_traffic"].get(UPLINK_PORT, 0)
    total_poe = sum(state["port_poe"].values())

    cam_lines = []
    for val in state["people_count"].values():
        if val.get("initialising"):
            cam_lines.append(f"  {val['camera']} / {val['zone']}: Initialising MV Sense")
        else:
            cam_lines.append(
                f"  {val['camera']} / {val['zone']}: "
                f"{val['count']:.1f} avg occupancy, {val['entrances']} entrances")
    cam_summary = "\n".join(cam_lines) if cam_lines else "  No camera data available"

    anom_lines   = [f"  [{a['severity']}] {a['message']}" for a in state["anomalies"]]
    anom_summary = "\n".join(anom_lines) if anom_lines else "  None detected"

    port_lines = []
    for pid, traffic in state["port_traffic"].items():
        name       = SWITCH["ports"].get(pid, f"Port {pid}")
        poe        = state["port_poe"].get(pid, 0)
        clients    = state["wap_clients"].get(pid, "")
        client_str = f", {clients} clients" if clients != "" else ""
        port_lines.append(
            f"  Port {pid} ({name}): {traffic:.1f} Kbps{client_str}, PoE {poe:.1f} Wh")
    port_summary = "\n".join(port_lines) if port_lines else "  No active ports"

    return f"""Building Observability Status — {ts}

ENVIRONMENTAL:
  Temperature : {f"{temp:.1f}°C" if temp is not None else "N/A"} (threshold: 24°C)
  Humidity    : {f"{hum:.0f}%" if hum is not None else "N/A"} (threshold: 70%)
  Door        : {"OPEN" if door else "CLOSED" if door is not None else "N/A"}

NETWORK ({SWITCH['name']} {SWITCH['model']}):
  Uplink Traffic : {uplink:.1f} Kbps
  Total PoE      : {total_poe:.1f} Wh
  WAP Clients    : {wap} total
  Active Ports:
{port_summary}

CAMERA ANALYTICS:
{cam_summary}

ANOMALIES DETECTED:
{anom_summary}

SYSTEM: Poll #{state['poll_count']} | Interval: {POLL_INTERVAL}s
"""

# ── OpenRouter AI narrative generator ────────────────────────────────────────
def generate_narrative(context: str, question: str = None) -> str:
    api_key  = os.getenv("OPENROUTER_API_KEY",  "")
    base_url = os.getenv("OPENROUTER_BASE_URL",  "https://openrouter.ai/api/v1")
    referer  = os.getenv("OPENROUTER_REFERER",   "https://example.com")

    model = (os.getenv("MODEL_COMPOSER", "meta-llama/llama-3.3-70b-instruct:free")
             if question
             else os.getenv("MODEL_ANALYST", "openai/gpt-oss-120b:free"))

    if not api_key:
        return "OPENROUTER_API_KEY not configured. Add it to .env to enable AI narratives."

    system = """You are a building observability agent. You monitor environmental sensors,
network infrastructure, and camera analytics in real time.

Your role:
- Analyse multi-signal data (temperature, humidity, door state, network traffic, PoE, people count)
- Detect patterns and correlations across signals
- Generate clear, concise operational narratives for facilities and engineering teams
- Flag anomalies with actionable recommendations
- Keep responses focused and technical but accessible

Ground your response in the actual data provided. Be specific with numbers."""

    user_msg = (
        f"Current building data:\n\n{context}\n\nQuestion: {question}"
        if question else
        f"""Analyse the current building status and provide:
1. A one-paragraph operational summary
2. Any anomalies or concerns with recommended actions
3. Notable patterns or correlations across signals

Current data:\n\n{context}"""
    )

    headers = {
        "Authorization": f"Bearer {api_key}",
        "HTTP-Referer":  referer,
        "X-Title":       "Building Observability Agent",
        "Content-Type":  "application/json",
    }
    payload = {
        "model":      model,
        "max_tokens": 600,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user",   "content": user_msg},
        ],
    }

    try:
        r = requests.post(f"{base_url}/chat/completions",
                          headers=headers, json=payload, timeout=30)
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"]

    except requests.exceptions.Timeout:
        fallback = os.getenv("MODEL_FALLBACK", "openai/gpt-oss-20b:free")
        try:
            payload["model"] = fallback
            r = requests.post(f"{base_url}/chat/completions",
                              headers=headers, json=payload, timeout=20)
            r.raise_for_status()
            return r.json()["choices"][0]["message"]["content"]
        except Exception as e:
            return f"AI narrative unavailable (fallback also failed): {e}"

    except Exception as e:
        return f"AI narrative unavailable: {e}"


# ── Background polling loop ───────────────────────────────────────────────────
def polling_loop():
    print(f"[Poller] Starting — interval {POLL_INTERVAL}s")
    while True:
        try:
            state["poll_count"] += 1
            t0 = time.time()
            s  = poll_sensors()
            sw = poll_switch()
            c  = poll_cameras()
            detect_anomalies()
            state["last_poll"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Generate narrative every 5 polls or whenever anomalies are present
            if state["poll_count"] % 5 == 0 or state["anomalies"]:
                ctx = build_context()
                state["last_narrative"] = generate_narrative(ctx)

            elapsed = time.time() - t0
            print(f"[Poller] #{state['poll_count']} — "
                  f"sensors:{s} switch:{sw} camera:{c} "
                  f"anomalies:{len(state['anomalies'])} ({elapsed:.1f}s)")

            state["errors"] = state["errors"][-10:]

        except Exception as e:
            print(f"[Poller] ERROR: {e}")
            state["errors"].append(str(e))

        time.sleep(POLL_INTERVAL)

# ── FastAPI app ───────────────────────────────────────────────────────────────
app = FastAPI(title="Building Observability Agent", version="2.0")

class ChatRequest(BaseModel):
    message: str

@app.get("/", response_class=HTMLResponse)
async def root():
    return HTML_UI

@app.get("/api/status")
async def get_status():
    return {
        "last_poll":      state["last_poll"],
        "poll_count":     state["poll_count"],
        "temperature":    state["temperature"],
        "humidity":       state["humidity"],
        "door_open":      state["door_open"],
        "wap_clients":    sum(state["wap_clients"].values()),
        "uplink_kbps":    state["port_traffic"].get(UPLINK_PORT, 0),
        "total_poe_wh":   sum(state["port_poe"].values()),
        "people_count":   state["people_count"],
        "anomaly_count":  len(state["anomalies"]),
        "anomalies":      state["anomalies"],
        "last_narrative": state["last_narrative"],
        "errors":         state["errors"][-3:],
    }

@app.get("/api/context")
async def get_context():
    return {"context": build_context()}

@app.post("/api/chat")
async def chat(req: ChatRequest):
    if not req.message.strip():
        raise HTTPException(status_code=400, detail="Empty message")
    ctx      = build_context()
    response = generate_narrative(ctx, req.message)
    return {"response": response, "context_snapshot": ctx}

@app.get("/api/narrative")
async def get_narrative():
    ctx = build_context()
    narrative = generate_narrative(ctx)
    state["last_narrative"] = narrative
    return {"narrative": narrative}

# ── Chatbot HTML UI ───────────────────────────────────────────────────────────
HTML_UI = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Building Observability Agent</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: 'Segoe UI', sans-serif; background: #0d1117; color: #e6edf3; min-height: 100vh; }
  header { background: #161b22; border-bottom: 1px solid #30363d; padding: 16px 24px;
           display: flex; align-items: center; gap: 12px; }
  .logo { width: 36px; height: 36px; background: #1f6feb; border-radius: 6px;
          display: flex; align-items: center; justify-content: center;
          font-weight: 800; font-size: 14px; color: #fff; }
  header h1 { font-size: 18px; font-weight: 600; }
  header span { font-size: 12px; color: #8b949e; margin-left: auto; }
  .main { display: grid; grid-template-columns: 340px 1fr; gap: 0; height: calc(100vh - 61px); }
  .sidebar { background: #161b22; border-right: 1px solid #30363d; padding: 16px; overflow-y: auto; }
  .sidebar h2 { font-size: 12px; text-transform: uppercase; letter-spacing: 1px;
                color: #8b949e; margin-bottom: 12px; }
  .stat-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-bottom: 16px; }
  .stat { background: #0d1117; border: 1px solid #30363d; border-radius: 8px; padding: 10px; }
  .stat .label { font-size: 10px; color: #8b949e; text-transform: uppercase; letter-spacing: 0.5px; }
  .stat .value { font-size: 22px; font-weight: 700; margin-top: 2px; }
  .stat .value.ok   { color: #3fb950; }
  .stat .value.warn { color: #d29922; }
  .stat .value.crit { color: #f85149; }
  .stat .value.info { color: #58a6ff; }
  .anomaly-list { margin-bottom: 16px; }
  .anomaly { background: #0d1117; border-left: 3px solid #d29922; border-radius: 4px;
             padding: 8px 10px; margin-bottom: 6px; font-size: 12px; }
  .anomaly.HIGH { border-color: #f85149; }
  .anomaly.INFO { border-color: #58a6ff; }
  .anomaly.INIT { border-color: #6e7681; }
  .anomaly .sev { font-size: 10px; font-weight: 700; margin-bottom: 2px; }
  .narrative { background: #0d1117; border: 1px solid #30363d; border-radius: 8px;
               padding: 12px; font-size: 12px; line-height: 1.6; color: #c9d1d9; }
  .chat-area { display: flex; flex-direction: column; }
  .messages { flex: 1; overflow-y: auto; padding: 20px; display: flex; flex-direction: column; gap: 12px; }
  .msg { max-width: 80%; padding: 12px 16px; border-radius: 12px; font-size: 14px; line-height: 1.5; }
  .msg.user  { background: #1f6feb; align-self: flex-end; border-radius: 12px 12px 2px 12px; }
  .msg.agent { background: #161b22; border: 1px solid #30363d; align-self: flex-start;
               border-radius: 12px 12px 12px 2px; white-space: pre-wrap; }
  .msg.system { background: transparent; border: 1px solid #30363d; align-self: center;
                font-size: 12px; color: #8b949e; text-align: center;
                border-radius: 20px; padding: 6px 16px; }
  .input-row { padding: 16px 20px; border-top: 1px solid #30363d; background: #161b22;
               display: flex; gap: 10px; }
  .input-row input { flex: 1; background: #0d1117; border: 1px solid #30363d; border-radius: 8px;
                     padding: 10px 14px; color: #e6edf3; font-size: 14px; outline: none; }
  .input-row input:focus { border-color: #58a6ff; }
  .input-row button { background: #1f6feb; color: #fff; border: none; border-radius: 8px;
                      padding: 10px 20px; font-weight: 600; cursor: pointer; font-size: 14px; }
  .input-row button:hover { background: #388bfd; }
  .input-row button:disabled { opacity: 0.5; cursor: not-allowed; }
  .suggestions { padding: 0 20px 12px; display: flex; gap: 8px; flex-wrap: wrap; }
  .sug { background: #161b22; border: 1px solid #30363d; border-radius: 16px;
         padding: 6px 12px; font-size: 12px; cursor: pointer; color: #8b949e; }
  .sug:hover { border-color: #58a6ff; color: #58a6ff; }
  .pulse { display: inline-block; width: 8px; height: 8px; border-radius: 50%;
           background: #3fb950; margin-right: 6px; animation: pulse 2s infinite; }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.3} }
</style>
</head>
<body>
<header>
  <div class="logo">OA</div>
  <h1>Building Observability Agent</h1>
  <span id="poll-status"><span class="pulse"></span>Initialising...</span>
</header>
<div class="main">
  <div class="sidebar">
    <h2>Live Sensor Data</h2>
    <div class="stat-grid">
      <div class="stat"><div class="label">Temperature</div><div class="value" id="temp">--</div></div>
      <div class="stat"><div class="label">Humidity</div><div class="value" id="hum">--</div></div>
      <div class="stat"><div class="label">Door</div><div class="value" id="door">--</div></div>
      <div class="stat"><div class="label">WAP Clients</div><div class="value info" id="wap">--</div></div>
      <div class="stat"><div class="label">Uplink Kbps</div><div class="value info" id="uplink">--</div></div>
      <div class="stat"><div class="label">Total PoE Wh</div><div class="value info" id="poe">--</div></div>
    </div>
    <h2>People Count</h2>
    <div id="people-counts" class="anomaly-list">
      <div class="anomaly INFO"><div class="sev">CAMERA</div>Awaiting data...</div>
    </div>
    <h2>Active Anomalies</h2>
    <div id="anomaly-list" class="anomaly-list">
      <div class="anomaly INFO"><div class="sev">STATUS</div>Polling...</div>
    </div>
    <h2>AI Narrative</h2>
    <div id="narrative" class="narrative">Waiting for first AI analysis...</div>
  </div>
  <div class="chat-area">
    <div class="messages" id="messages">
      <div class="msg system">Building Observability Agent ready — ask anything about your building</div>
    </div>
    <div class="suggestions">
      <div class="sug" onclick="ask('What is the current building status?')">Building status</div>
      <div class="sug" onclick="ask('Are there any anomalies right now?')">Any anomalies?</div>
      <div class="sug" onclick="ask('How many people are in the building?')">Occupancy</div>
      <div class="sug" onclick="ask('What is the network traffic like?')">Network traffic</div>
      <div class="sug" onclick="ask('Which devices are consuming the most power?')">PoE usage</div>
      <div class="sug" onclick="ask('Is the temperature within safe limits?')">Temp check</div>
      <div class="sug" onclick="ask('Summarise all sensor readings in one sentence')">Quick summary</div>
      <div class="sug" onclick="ask('What would you recommend to improve energy efficiency?')">Energy tips</div>
    </div>
    <div class="input-row">
      <input type="text" id="user-input" placeholder="Ask about the building..."
             onkeydown="if(event.key==='Enter') sendMessage()">
      <button id="send-btn" onclick="sendMessage()">Ask</button>
    </div>
  </div>
</div>
<script>
async function updateStatus() {
  try {
    const r = await fetch('/api/status');
    const d = await r.json();
    const temp = d.temperature, hum = d.humidity, door = d.door_open;
    const te = document.getElementById('temp');
    te.textContent = temp !== null ? temp.toFixed(1)+'°C' : '--';
    te.className = 'value '+(temp===null?'': temp>28?'crit': temp>24?'warn':'ok');
    const he = document.getElementById('hum');
    he.textContent = hum !== null ? hum.toFixed(0)+'%' : '--';
    he.className = 'value '+(hum===null?'': hum>70?'crit': hum>60?'warn':'ok');
    const de = document.getElementById('door');
    de.textContent = door===null ? '--' : door ? 'OPEN' : 'CLOSED';
    de.className = 'value '+(door ? 'warn' : 'ok');
    document.getElementById('wap').textContent    = d.wap_clients ?? '--';
    document.getElementById('uplink').textContent = d.uplink_kbps ? d.uplink_kbps.toFixed(0) : '--';
    document.getElementById('poe').textContent    = d.total_poe_wh ? d.total_poe_wh.toFixed(0) : '--';
    const pc = document.getElementById('people-counts');
    const people = d.people_count;
    if (people && Object.keys(people).length > 0) {
      pc.innerHTML = Object.entries(people).map(([k,v]) => v.initialising
        ? `<div class="anomaly INIT"><div class="sev">${v.camera.toUpperCase()} / ${v.zone}</div>MV Sense initialising...</div>`
        : `<div class="anomaly INFO"><div class="sev">${v.camera.toUpperCase()} / ${v.zone}</div>${v.count.toFixed(1)} avg &nbsp;|&nbsp; ${v.entrances} entrances</div>`
      ).join('');
    } else {
      pc.innerHTML = '<div class="anomaly INFO"><div class="sev">CAMERA</div>No data yet</div>';
    }
    const al = document.getElementById('anomaly-list');
    al.innerHTML = d.anomalies && d.anomalies.length > 0
      ? d.anomalies.map(a => `<div class="anomaly ${a.severity}"><div class="sev">${a.severity} — ${a.signal}</div>${a.message}</div>`).join('')
      : '<div class="anomaly INFO"><div class="sev">STATUS</div>All systems normal</div>';
    if (d.last_narrative) document.getElementById('narrative').textContent = d.last_narrative;
    document.getElementById('poll-status').innerHTML =
      `<span class="pulse"></span>Poll #${d.poll_count} — ${d.last_poll || 'pending'}`;
  } catch(e) { console.error('Status update failed:', e); }
}
async function sendMessage() {
  const input = document.getElementById('user-input');
  const btn   = document.getElementById('send-btn');
  const msg   = input.value.trim();
  if (!msg) return;
  addMessage(msg, 'user');
  input.value = ''; btn.disabled = true;
  addMessage('Analysing building data...', 'agent', 'thinking');
  try {
    const r = await fetch('/api/chat', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({message: msg})
    });
    const d = await r.json();
    removeThinking(); addMessage(d.response, 'agent');
  } catch(e) { removeThinking(); addMessage('Connection error.', 'agent'); }
  btn.disabled = false;
}
function ask(q) { document.getElementById('user-input').value = q; sendMessage(); }
function addMessage(text, role, id) {
  const div = document.createElement('div');
  div.className = `msg ${role}`; if (id) div.id = id; div.textContent = text;
  const msgs = document.getElementById('messages');
  msgs.appendChild(div); msgs.scrollTop = msgs.scrollHeight;
}
function removeThinking() { const t = document.getElementById('thinking'); if (t) t.remove(); }
updateStatus();
setInterval(updateStatus, 15000);
</script>
</body>
</html>"""

# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if not MERAKI_API_KEY:
        print("WARNING: MERAKI_API_KEY not set — polling will fail")
    if not SPLUNK_HEC_TOKEN:
        print("WARNING: SPLUNK_HEC_TOKEN not set — Splunk ingestion disabled")
    if not ORG_ID:
        print("WARNING: MERAKI_ORG_ID not set")

    threading.Thread(target=polling_loop, daemon=True).start()
    uvicorn.run(app, host="0.0.0.0", port=5000, log_level="warning")
