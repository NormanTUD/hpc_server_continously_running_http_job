#!/usr/bin/env python3
"""
Very small HTTP server that shows the machine’s hostname and greets the user.

Start the server with:
    python3 hpc.py
Then visit http://localhost:8000/
"""

import socket
from flask import Flask, request

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


if __name__ == "__main__":
    # Listen on all interfaces so the real hostname resolves from outside localhost
    app.run(host="0.0.0.0", port=8000)
