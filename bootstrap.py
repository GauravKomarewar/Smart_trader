#!/usr/bin/env python3
"""
Smart Trader — Automated Bootstrap Setup
==========================================
One-command setup: clone the repo, then run `python3 bootstrap.py`.
Handles: system deps, PostgreSQL, Python venv, geckodriver, Node.js,
         frontend build, nginx, systemd services, .env generation,
         DB initialization, and health checks.

Usage:
    python3 bootstrap.py            # Interactive setup
    python3 bootstrap.py --yes      # Accept all defaults (non-interactive)
"""

import os
import sys
import platform
import subprocess
import secrets
import string
import shutil
import time
import urllib.request
from pathlib import Path

# ── Paths ───────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
BACKEND_DIR = BASE_DIR / "backend"
FRONTEND_DIR = BASE_DIR / "frontend"
VENV_DIR = BACKEND_DIR / "venv"

BACKEND_PORT = 8001
FRONTEND_PORT = 5173
NGINX_PORT = 3000

AUTO_YES = "--yes" in sys.argv or "-y" in sys.argv


# ── Helpers ─────────────────────────────────────────────────────────────
def banner(text):
    print(f"\n{'='*60}\n  {text}\n{'='*60}")


def run(cmd, **kwargs):
    """Run a command, print it, and return the CompletedProcess."""
    if isinstance(cmd, str):
        print(f"  $ {cmd}")
    else:
        print(f"  $ {' '.join(cmd)}")
    return subprocess.run(cmd, shell=isinstance(cmd, str), check=True, **kwargs)


def run_quiet(cmd, **kwargs):
    """Run a command silently, return CompletedProcess or None on error."""
    try:
        return subprocess.run(
            cmd, shell=isinstance(cmd, str), capture_output=True, text=True, **kwargs
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def ask(prompt, default=""):
    if AUTO_YES:
        return default
    val = input(f"{prompt} [{default}]: ").strip()
    return val if val else default


def generate_password(length=32):
    chars = string.ascii_letters + string.digits
    return "".join(secrets.choice(chars) for _ in range(length))


def generate_secret_key():
    return secrets.token_hex(32)


def _sudo():
    """Return 'sudo ' prefix if not root."""
    if os.geteuid() == 0:
        return ""
    if shutil.which("sudo"):
        return "sudo "
    return ""


def _arch():
    """Return machine architecture normalised for download URLs."""
    m = platform.machine().lower()
    if m in ("aarch64", "arm64"):
        return "aarch64"
    if m in ("x86_64", "amd64"):
        return "x86_64"
    return m


# ── Step 1: System Dependencies ─────────────────────────────────────────
def install_system_deps():
    banner("Step 1: System Dependencies")
    sudo = _sudo()

    if not shutil.which("apt-get"):
        print("  apt-get not found. Ensure these are installed manually:")
        print("  python3, python3-venv, postgresql, nginx, nodejs, npm, firefox")
        return

    print("Updating package lists...")
    run(f"{sudo}apt-get update -qq")

    packages = [
        "python3", "python3-venv", "python3-pip", "python3-dev",
        "postgresql", "postgresql-contrib",
        "nginx",
        "nodejs", "npm",
        "build-essential", "libpq-dev", "curl",
        "firefox",
    ]
    print(f"Installing: {', '.join(packages)}")
    run(f"{sudo}apt-get install -y -qq {' '.join(packages)}")

    run(f"{sudo}systemctl enable postgresql")
    run(f"{sudo}systemctl start postgresql")

    print("  System dependencies installed")


# ── Step 2: geckodriver (for Shoonya OAuth headless login) ──────────────
def install_geckodriver():
    banner("Step 2: geckodriver (Selenium)")

    if shutil.which("geckodriver"):
        ver = run_quiet("geckodriver --version")
        if ver and ver.stdout:
            print(f"  geckodriver already installed: {ver.stdout.splitlines()[0]}")
            return

    arch = _arch()
    if arch == "aarch64":
        arch_suffix = "linux-aarch64"
    elif arch == "x86_64":
        arch_suffix = "linux64"
    else:
        print(f"  Unsupported architecture '{arch}' for geckodriver — install manually")
        return

    version = "0.36.0"
    url = (
        f"https://github.com/mozilla/geckodriver/releases/download/"
        f"v{version}/geckodriver-v{version}-{arch_suffix}.tar.gz"
    )
    sudo = _sudo()
    print(f"Downloading geckodriver v{version} for {arch_suffix}...")
    run(f"curl -sL {url} | {sudo}tar -xz -C /usr/local/bin")
    run(f"{sudo}chmod +x /usr/local/bin/geckodriver")
    print("  geckodriver installed at /usr/local/bin/geckodriver")


# ── Step 3: PostgreSQL Setup ────────────────────────────────────────────
def setup_postgresql(db_user, db_password, db_name):
    banner("Step 3: PostgreSQL Database")

    check = run_quiet(
        f"sudo -u postgres psql -tAc \"SELECT 1 FROM pg_roles WHERE rolname='{db_user}'\""
    )
    if check and check.stdout.strip() == "1":
        print(f"  PostgreSQL user '{db_user}' already exists")
    else:
        print(f"Creating PostgreSQL user '{db_user}'...")
        run(f"sudo -u postgres psql -c \"CREATE USER {db_user} WITH PASSWORD '{db_password}'\"")

    check = run_quiet(
        f"sudo -u postgres psql -tAc \"SELECT 1 FROM pg_database WHERE datname='{db_name}'\""
    )
    if check and check.stdout.strip() == "1":
        print(f"  Database '{db_name}' already exists")
    else:
        print(f"Creating database '{db_name}'...")
        run(f"sudo -u postgres psql -c \"CREATE DATABASE {db_name} OWNER {db_user}\"")

    run(f"sudo -u postgres psql -c \"GRANT ALL PRIVILEGES ON DATABASE {db_name} TO {db_user}\"")
    print("  PostgreSQL setup complete")


# ── Step 4: Python Virtual Environment ──────────────────────────────────
def setup_python_venv():
    banner("Step 4: Python Virtual Environment")

    if VENV_DIR.exists():
        print(f"  venv already exists at {VENV_DIR}")
    else:
        print("Creating virtual environment...")
        run(f"{sys.executable} -m venv {VENV_DIR}")

    pip = VENV_DIR / "bin" / "pip"
    print("Upgrading pip...")
    run(f"{pip} install --upgrade pip -q")

    print("Installing backend dependencies...")
    run(f"{pip} install -r {BACKEND_DIR / 'requirements.txt'} -q")

    # Extra packages needed at runtime but not always in requirements.txt
    extras = [
        "psycopg2-binary",   # PostgreSQL adapter for trading_db
        "numpy",             # Greeks / option pricing
        "scipy",             # Greeks / option pricing
        "fyers-apiv3",       # Fyers broker API
        "bcrypt",            # passlib[bcrypt] backend
    ]
    print(f"Installing extras: {', '.join(extras)}")
    run(f"{pip} install {' '.join(extras)} -q")

    print("  Python dependencies installed")


# ── Step 5: Backend .env ────────────────────────────────────────────────
def setup_backend_env(db_user, db_password, db_name, admin_email, admin_password):
    banner("Step 5: Backend Configuration (.env)")

    env_file = BACKEND_DIR / ".env"
    if env_file.exists():
        print(f"  {env_file} already exists — skipping")
        print("  To regenerate, delete the file and re-run bootstrap.py")
        return

    jwt_secret = generate_secret_key()
    encryption_key = generate_secret_key()
    trading_dsn = (
        f"dbname={db_name} user={db_user} password={db_password} "
        f"host=localhost port=5432"
    )

    content = f"""# Smart Trader — Backend Environment Variables
# Auto-generated by bootstrap.py — edit as needed

# ── Server ──────────────────────────────────────────
BACKEND_HOST=0.0.0.0
BACKEND_PORT={BACKEND_PORT}
DEBUG=false
CORS_ORIGINS=http://localhost:{FRONTEND_PORT},http://localhost:{NGINX_PORT}

# ── JWT & Encryption ───────────────────────────────
JWT_SECRET={jwt_secret}
JWT_EXPIRE_HOURS=24
ENCRYPTION_KEY={encryption_key}

# ── Admin Defaults ──────────────────────────────────
ADMIN_EMAIL={admin_email}
ADMIN_PASSWORD={admin_password}
ADMIN_NAME=Admin

# ── Database ────────────────────────────────────────
# SQLite auth DB (auto-created at data/smarttrader.db)
# DATABASE_URL=sqlite:///data/smarttrader.db

# PostgreSQL trading DB
TRADING_DB_URL={trading_dsn}

# ── Logging ─────────────────────────────────────────
LOG_DIR=logs
LOG_LEVEL=INFO

# ── Broker Credentials (fill in after first login via UI) ──
# SHOONYA_USER_ID=
# SHOONYA_PASSWORD=
# SHOONYA_TOTP_KEY=
# SHOONYA_VENDOR_CODE=
# SHOONYA_OAUTH_SECRET=
# FYERS_APP_ID=
# FYERS_CLIENT_ID=
# FYERS_SECRET_ID=
# FYERS_TOTP_KEY=
# FYERS_PIN=
"""
    env_file.write_text(content)
    os.chmod(env_file, 0o600)
    print(f"  Created {env_file}")


# ── Step 6: Create Required Directories ─────────────────────────────────
def create_directories():
    banner("Step 6: Creating Directories")
    dirs = [
        BACKEND_DIR / "data",
        BACKEND_DIR / "data" / "option_chain",
        BACKEND_DIR / "logs",
    ]
    for d in dirs:
        if not d.exists():
            d.mkdir(parents=True)
            print(f"  Created: {d}")
    print("  Directories ready")


# ── Step 7: Initialize Trading Database ─────────────────────────────────
def init_trading_db():
    banner("Step 7: Initialize Trading Database")
    python = VENV_DIR / "bin" / "python"
    # Run Python snippet that imports and calls init_trading_db()
    cmd = (
        f'{python} -c "'
        "import sys; sys.path.insert(0, str('{backend}')); "
        "from db.trading_db import init_trading_db; init_trading_db(); "
        "print('  Trading DB tables created')"
        '"'
    ).replace("{backend}", str(BACKEND_DIR))
    try:
        run(cmd)
    except subprocess.CalledProcessError:
        print("  Could not auto-init trading DB (may need manual setup)")
        print(f"  Run: cd {BACKEND_DIR} && {python} -c 'from db.trading_db import init_trading_db; init_trading_db()'")


# ── Step 8: Frontend Build ──────────────────────────────────────────────
def setup_frontend():
    banner("Step 8: Frontend Build (React + Vite)")

    if not shutil.which("npm"):
        print("  npm not found — skipping frontend build")
        print("  Install Node.js >=18 and re-run bootstrap")
        return

    # Ensure Node >= 18
    node_ver = run_quiet("node --version")
    if node_ver and node_ver.stdout:
        print(f"  Node.js version: {node_ver.stdout.strip()}")

    # Create .env for frontend API proxy
    fe_env = FRONTEND_DIR / ".env"
    if not fe_env.exists():
        fe_env.write_text(
            f"VITE_API_URL=http://localhost:{BACKEND_PORT}\n"
        )
        print(f"  Created {fe_env}")

    print("Installing frontend dependencies...")
    run("npm install --legacy-peer-deps", cwd=str(FRONTEND_DIR))

    print("Building production bundle...")
    run("npm run build", cwd=str(FRONTEND_DIR))

    print("  Frontend built successfully")


# ── Step 9: Nginx Configuration ─────────────────────────────────────────
def setup_nginx():
    banner("Step 9: Nginx Configuration")
    sudo = _sudo()

    # Detect frontend build output dir
    dist_dir = FRONTEND_DIR / "dist"   # Vite default
    if not dist_dir.exists():
        dist_dir = FRONTEND_DIR / "build"
    if not dist_dir.exists():
        print("  Frontend build directory not found — skipping nginx")
        print("  Build frontend first, then re-run bootstrap")
        return

    nginx_conf = f"""# Smart Trader — auto-generated by bootstrap.py
server {{
    listen {NGINX_PORT};
    server_name _;

    client_max_body_size 10M;

    # Serve React/Vite production build
    root {dist_dir};
    index index.html;

    # Proxy API requests to FastAPI backend
    location /api/ {{
        proxy_pass http://127.0.0.1:{BACKEND_PORT};
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_read_timeout 120s;
        proxy_send_timeout 60s;
    }}

    # Proxy WebSocket connections
    location /ws {{
        proxy_pass http://127.0.0.1:{BACKEND_PORT};
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_read_timeout 86400s;
    }}

    # Proxy /docs and /openapi.json to backend
    location ~ ^/(docs|redoc|openapi.json) {{
        proxy_pass http://127.0.0.1:{BACKEND_PORT};
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }}

    # Static assets — long cache
    location ~* \\.(js|css|ico|svg|woff|woff2|ttf|eot|png|jpg|jpeg|gif|webp)$ {{
        expires 1y;
        add_header Cache-Control "public, immutable";
        try_files $uri =404;
    }}

    # SPA fallback
    location / {{
        add_header Cache-Control "no-cache, must-revalidate";
        try_files $uri $uri/ /index.html;
    }}
}}
"""

    conf_path = Path("/etc/nginx/sites-available/smart_trader")
    link_path = Path("/etc/nginx/sites-enabled/smart_trader")

    tmp = Path("/tmp/smart_trader_nginx.conf")
    tmp.write_text(nginx_conf)
    run(f"{sudo}cp {tmp} {conf_path}")
    tmp.unlink()

    if not link_path.exists():
        run(f"{sudo}ln -sf {conf_path} {link_path}")

    # Remove default site if it conflicts
    default_site = Path("/etc/nginx/sites-enabled/default")
    if default_site.exists():
        run(f"{sudo}rm -f {default_site}")

    run(f"{sudo}nginx -t")
    run(f"{sudo}systemctl enable nginx")
    run(f"{sudo}systemctl reload nginx")

    print(f"  Nginx configured on port {NGINX_PORT}")


# ── Step 10: Systemd Service ────────────────────────────────────────────
def setup_systemd():
    banner("Step 10: Systemd Service")
    sudo = _sudo()
    user = os.environ.get("USER", "ubuntu")
    python = VENV_DIR / "bin" / "python"

    service = f"""[Unit]
Description=Smart Trader Backend (FastAPI)
After=network-online.target postgresql.service
Wants=network-online.target
StartLimitIntervalSec=3600
StartLimitBurst=10

[Service]
Type=simple
User={user}
Group={user}
WorkingDirectory={BACKEND_DIR}

Environment="PATH={VENV_DIR / 'bin'}:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin"
Environment="PYTHONUNBUFFERED=1"
Environment="PYTHONPATH={BACKEND_DIR}"

ExecStart={python} {BACKEND_DIR / 'main.py'}

Restart=always
RestartSec=15

MemoryMax=2G
CPUQuota=80%

PrivateTmp=yes
NoNewPrivileges=yes
ProtectSystem=no
ProtectHome=no
ProtectKernelTunables=yes

TimeoutStopSec=30
KillSignal=SIGTERM
SendSIGKILL=yes

OOMScoreAdjust=-500
LimitNOFILE=65536
LimitNPROC=8192

StandardOutput=journal
StandardError=journal
SyslogIdentifier=smart_trader

[Install]
WantedBy=multi-user.target
"""

    svc_path = Path("/etc/systemd/system/smart_trader.service")
    tmp = Path("/tmp/smart_trader.service")
    tmp.write_text(service)
    run(f"{sudo}cp {tmp} {svc_path}")
    tmp.unlink()

    run(f"{sudo}systemctl daemon-reload")
    run(f"{sudo}systemctl enable smart_trader")
    run(f"{sudo}systemctl restart smart_trader")

    print("  smart_trader.service enabled and started")


# ── Step 11: Health Check ───────────────────────────────────────────────
def health_check():
    banner("Step 11: Health Check")
    time.sleep(4)

    # Backend
    try:
        resp = urllib.request.urlopen(
            f"http://localhost:{BACKEND_PORT}/docs", timeout=5
        )
        if resp.status == 200:
            print(f"  Backend API is running on port {BACKEND_PORT}")
        else:
            print(f"  Backend returned status {resp.status}")
    except Exception as e:
        print(f"  Backend health check failed: {e}")
        print(f"  Check logs: sudo journalctl -u smart_trader -n 30")

    # Nginx
    try:
        resp = urllib.request.urlopen(
            f"http://localhost:{NGINX_PORT}/", timeout=5
        )
        if resp.status == 200:
            print(f"  Nginx serving frontend on port {NGINX_PORT}")
        else:
            print(f"  Nginx returned status {resp.status}")
    except Exception as e:
        print(f"  Nginx check failed: {e}")
        print(f"  Check: sudo nginx -t && sudo systemctl status nginx")


# ── Main ────────────────────────────────────────────────────────────────
def main():
    print(r"""
  ╔══════════════════════════════════════════════════════════╗
  ║         Smart Trader — Automated Bootstrap Setup        ║
  ║   Multi-Broker Trading Platform (Shoonya + Fyers)       ║
  ╚══════════════════════════════════════════════════════════╝
    """)

    banner("Configuration")
    db_user = ask("PostgreSQL username", "smarttrader")
    db_password = ask(
        "PostgreSQL password (auto-generated if blank)", generate_password()
    )
    db_name = ask("Database name", "smart_trader_trading")
    admin_email = ask("Admin email", "admin@smarttrader.local")
    admin_password = ask("Admin password", "Admin@1234")

    print(f"\n  Database:  {db_name} (user: {db_user})")
    print(f"  Admin:     {admin_email}")
    print(f"  Backend:   port {BACKEND_PORT}")
    print(f"  Frontend:  port {NGINX_PORT} (nginx)")

    if not AUTO_YES:
        confirm = input("\nProceed with setup? (y/n): ").strip().lower()
        if confirm != "y":
            print("Aborted.")
            sys.exit(0)

    try:
        install_system_deps()
        install_geckodriver()
        setup_postgresql(db_user, db_password, db_name)
        setup_python_venv()
        setup_backend_env(db_user, db_password, db_name, admin_email, admin_password)
        create_directories()
        init_trading_db()
        setup_frontend()
        setup_nginx()
        setup_systemd()
        health_check()

        banner("Setup Complete!")
        print(f"""
  Smart Trader is ready!

  App URL:        http://<your-server-ip>:{NGINX_PORT}
  API Docs:       http://<your-server-ip>:{BACKEND_PORT}/docs
  Admin Login:    {admin_email} / {admin_password}

  Service management:
    sudo systemctl status smart_trader
    sudo systemctl restart smart_trader
    sudo journalctl -u smart_trader -f

  Broker credentials:
    Log in to the web UI and add Shoonya/Fyers credentials
    via Settings > Broker Accounts.

  IMPORTANT: Change default admin credentials after first login!
""")

    except subprocess.CalledProcessError as e:
        print(f"\n  Command failed: {e}")
        print("  Fix the issue above and re-run: python3 bootstrap.py")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n\n  Setup interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n  Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
