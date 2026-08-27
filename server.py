#!/usr/bin/env python3
"""
NRM Packet Sender - Local Bridge Server
----------------------------------------
Browsers cannot open raw TCP sockets, so this small local server does the
real networking on your behalf:

  1. Serves the NRM Packet Sender web UI (nrm_packet_sender.html)
  2. Exposes a JSON API that the web UI calls to actually open a real TCP
     socket to your target device/server and send/receive packets.

Supports TWO independent TCP connections:
  - "primary"   : normal NRM/ALT/LGN/HLM traffic
  - "emergency" : dedicated connection just for $EPB (EMR/SEM) packets

USAGE
-----
    python server.py [port]

Then open in your browser:   http://127.0.0.1:8765   (or your chosen port)

Requires ONLY the Python standard library - no pip install needed.
Both files (server.py and nrm_packet_sender.html) must be in the same folder.
"""

import json
import os
import socket
import sys
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HTML_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nrm_packet_sender.html")

# ---------------------------------------------------------------------------
# Shared state (guarded by state_lock — accessed from multiple threads:
# the HTTP request threads and the background socket-receiver threads)
# ---------------------------------------------------------------------------
state_lock = threading.Lock()

def _new_conn():
    return {
        "sock": None,
        "connected": False,
        "host": "",
        "port": 0,
        "connected_at": None,
        "stop_recv": False,
    }

state = {
    "conns": {
        "primary": _new_conn(),
        "emergency": _new_conn(),
    },
    "logs": [],           # list of {id, time, type, direction, message}
    "next_log_id": 1,
    "stats": {"sent": 0, "recv": 0, "errors": 0, "last_sent": None},
}


def _conn_name(target):
    return "emergency" if str(target).lower() == "emergency" else "primary"


def ts():
    d = datetime.now()
    return d.strftime("%d-%m-%Y %H:%M:%S")


def add_log(type_, direction, message):
    with state_lock:
        entry = {
            "id": state["next_log_id"],
            "time": ts(),
            "type": type_,
            "direction": direction,
            "message": message,
        }
        state["next_log_id"] += 1
        state["logs"].append(entry)
        if len(state["logs"]) > 1000:
            state["logs"] = state["logs"][-1000:]
        return entry


def receiver_loop(name):
    """Background thread: continuously reads from a real socket and logs
    whatever the target server sends back (ACKs, replies, unsolicited data)."""
    while True:
        with state_lock:
            conn = state["conns"][name]
            if conn["stop_recv"] or not conn["sock"]:
                return
            sock = conn["sock"]
        try:
            sock.settimeout(1.0)
            data = sock.recv(4096)
        except socket.timeout:
            continue
        except OSError:
            break

        if not data:
            break  # remote closed the connection

        message = data.decode(errors="replace").strip()
        if message:
            tag = "EMERGENCY" if name == "emergency" else "PRIMARY"
            add_log("RX", "RECEIVE", f"[{tag}] {message}")
            with state_lock:
                state["stats"]["recv"] += 1

    with state_lock:
        conn = state["conns"][name]
        was_connected = conn["connected"]
        conn["connected"] = False
    if was_connected:
        add_log("INFO", "SYSTEM", f"Connection lost ({name})")


def do_connect(target, host, port, timeout):
    name = _conn_name(target)
    with state_lock:
        if state["conns"][name]["connected"]:
            return {"ok": False, "error": "Already connected. Disconnect first."}

    try:
        sock = socket.create_connection((host, int(port)), timeout=float(timeout))
    except Exception as e:
        add_log("INFO", "SYSTEM", f"[{name}] Connection failed to {host}:{port} - {e}")
        with state_lock:
            state["stats"]["errors"] += 1
        return {"ok": False, "error": str(e)}

    with state_lock:
        conn = state["conns"][name]
        conn["sock"] = sock
        conn["connected"] = True
        conn["host"] = host
        conn["port"] = port
        conn["connected_at"] = time.time()
        conn["stop_recv"] = False

    add_log("INFO", "SYSTEM", f"[{name}] Connected to {host}:{port}")
    add_log("INFO", "SYSTEM", f"[{name}] Connection established successfully")

    t = threading.Thread(target=receiver_loop, args=(name,), daemon=True)
    t.start()
    return {"ok": True}


def do_disconnect(target):
    name = _conn_name(target)
    with state_lock:
        conn = state["conns"][name]
        sock = conn["sock"]
        conn["stop_recv"] = True
        conn["connected"] = False
        conn["sock"] = None
    if sock:
        try:
            sock.close()
        except Exception:
            pass
    add_log("INFO", "SYSTEM", f"[{name}] Disconnected from server")
    return {"ok": True}


def do_test(host, port, timeout):
    try:
        s = socket.create_connection((host, int(port)), timeout=float(timeout))
        s.close()
        add_log("INFO", "SYSTEM", f"Test connection to {host}:{port} - OK")
        return {"ok": True}
    except Exception as e:
        add_log("INFO", "SYSTEM", f"Test connection to {host}:{port} - FAILED ({e})")
        return {"ok": False, "error": str(e)}


def do_send(packet, target="primary"):
    name = _conn_name(target)
    with state_lock:
        conn = state["conns"][name]
        connected = conn["connected"]
        sock = conn["sock"]

    if not connected or not sock:
        with state_lock:
            state["stats"]["errors"] += 1
        add_log("INFO", "SYSTEM", f"[{name}] Send failed: not connected")
        return {"ok": False, "error": f"Not connected ({name})"}

    try:
        sock.sendall(packet.encode())
    except Exception as e:
        with state_lock:
            state["stats"]["errors"] += 1
            conn["connected"] = False
        add_log("INFO", "SYSTEM", f"[{name}] Send failed: {e}")
        return {"ok": False, "error": str(e)}

    tag = "EMERGENCY" if name == "emergency" else "PRIMARY"
    add_log("TX", "SEND", f"[{tag}] {packet}")
    with state_lock:
        state["stats"]["sent"] += 1
        state["stats"]["last_sent"] = time.strftime("%H:%M:%S")

    return {"ok": True}


def do_clear_stats():
    with state_lock:
        state["stats"] = {"sent": 0, "recv": 0, "errors": 0, "last_sent": None}
        # Reset the uptime clock too (restart it from now for any active connection)
        for name in ("primary", "emergency"):
            conn = state["conns"][name]
            conn["connected_at"] = time.time() if conn["connected"] else None
    return {"ok": True}


def get_state(since):
    with state_lock:
        new_logs = [l for l in state["logs"] if l["id"] > since]

        def conn_view(name):
            c = state["conns"][name]
            uptime = (
                int(time.time() - c["connected_at"])
                if c["connected"] and c["connected_at"]
                else 0
            )
            return {
                "connected": c["connected"],
                "host": c["host"],
                "port": c["port"],
                "uptime_seconds": uptime,
            }

        primary = conn_view("primary")
        return {
            # Top-level keys kept for backward compatibility (primary connection)
            "connected": primary["connected"],
            "host": primary["host"],
            "port": primary["port"],
            "uptime_seconds": primary["uptime_seconds"],
            "primary": primary,
            "emergency": conn_view("emergency"),
            "logs": new_logs,
            "stats": state["stats"],
        }


# ---------------------------------------------------------------------------
# HTTP handler
# ---------------------------------------------------------------------------
class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # keep console clean

    def _send_json(self, obj, code=200):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self):
        length = int(self.headers.get("Content-Length", 0))
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            return json.loads(raw)
        except Exception:
            return {}

    def do_GET(self):
        if self.path == "/" or self.path.startswith("/index"):
            try:
                with open(HTML_FILE, "rb") as f:
                    body = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except FileNotFoundError:
                self._send_json(
                    {"error": "nrm_packet_sender.html not found next to server.py"}, 404
                )
            return

        if self.path.startswith("/api/state"):
            since = 0
            if "since=" in self.path:
                try:
                    since = int(self.path.split("since=")[1].split("&")[0])
                except Exception:
                    since = 0
            self._send_json(get_state(since))
            return

        self._send_json({"error": "not found"}, 404)

    def do_POST(self):
        body = self._read_json()

        if self.path == "/api/connect":
            result = do_connect(
                body.get("target", "primary"),
                body.get("host", "0.0.0.0"),
                body.get("port", 9999),
                body.get("timeout", 15),
            )
            self._send_json(result)
            return

        if self.path == "/api/disconnect":
            self._send_json(do_disconnect(body.get("target", "primary")))
            return

        if self.path == "/api/test":
            result = do_test(
                body.get("host", "127.0.0.1"), body.get("port", 9999), body.get("timeout", 5)
            )
            self._send_json(result)
            return

        if self.path == "/api/send":
            result = do_send(body.get("packet", ""), body.get("target", "primary"))
            self._send_json(result)
            return

        if self.path == "/api/clear_stats":
            self._send_json(do_clear_stats())
            return

        self._send_json({"error": "not found"}, 404)


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8765
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"NRM Packet Sender bridge running at http://127.0.0.1:{port}")
    print("Open that URL in your browser. Press Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.shutdown()


if __name__ == "__main__":
    main()
