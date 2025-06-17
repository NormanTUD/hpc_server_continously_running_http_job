#!/usr/bin/env python3
"""
Very small HTTP server that shows the machine’s hostname and greets the user.

Start the server with:
    python3 hpc.py
Then visit the printed URL or check ~/hpc_server_host_and_file.
"""

import socket
import os
from flask import Flask, request
from werkzeug.serving import make_server

app = Flask(__name__)
HOSTNAME = socket.gethostname()


@app.route("/", methods=["GET", "POST"])
def index():
    """Root page — shows hostname and an optional personalised greeting."""
    name = request.form.get("name")
    if name:
        greeting = f"Hi {name}. This script runs on {HOSTNAME}."
    else:
        greeting = f"This script runs on {HOSTNAME}."

    return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><title>HPC Server</title></head>
<body style="font-family:sans-serif">
  <h1>{greeting}</h1>
  <form method="post">
    <label for="name">Your name:</label>
    <input id="name" name="name" type="text" placeholder="Enter your name" required>
    <button type="submit">Send</button>
  </form>
</body>
</html>"""


def find_free_port():
    """Find a free port by binding to port 0 and letting the OS choose."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as temp_sock:
        temp_sock.bind(('', 0))
        return temp_sock.getsockname()[1]


def write_host_and_port_file(host, port):
    """Write hostname:port to ~/hpc_server_host_and_file."""
    filepath = os.path.expanduser("~/hpc_server_host_and_file")
    try:
        with open(filepath, "w") as f:
            f.write(f"{host}:{port}\n")
    except OSError as e:
        print(f"Failed to write {filepath}: {e}")


if __name__ == "__main__":
    port = find_free_port()
    write_host_and_port_file(HOSTNAME, port)
    print(f"Server running at http://{HOSTNAME}:{port}/")
    try:
        app.run(host="0.0.0.0", port=port)
    except Exception as e:
        print(f"Server failed to start: {e}")
