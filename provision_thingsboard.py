"""Provision ThingsBoard over its REST API (run once before the demo).

1. logs in as the tenant administrator (TB_URL, TB_USERNAME, TB_PASSWORD)
2. imports thingsboard/device_profile_water_node.json (alarm rules)
3. creates BDR-01..03 with that profile and generated access tokens
4. sets the server attribute ``inactivityTimeout`` on every device
   (ThingsBoard derives the ``active`` flag - used by the Node Inactive
   alarm - from this value)
5. writes the tokens into .env (TB_TOKEN_BDR01 ...)

Usage:  python provision_thingsboard.py [--inactivity-s 30]
The script is idempotent: existing profile/devices are reused.
"""

from __future__ import annotations

import argparse
import json
import secrets
import string

import requests

from config import BASE_DIR, load_dotenv, load_nodes, required_env

PROFILE_FILE = BASE_DIR / "thingsboard" / "device_profile_water_node.json"


class TB:
    def __init__(self, url: str, user: str, password: str) -> None:
        self.url = url.rstrip("/")
        r = requests.post(f"{self.url}/api/auth/login",
                          json={"username": user, "password": password}, timeout=15)
        r.raise_for_status()
        self.h = {"X-Authorization": f"Bearer {r.json()['token']}"}

    def get(self, path: str, **params):
        r = requests.get(self.url + path, headers=self.h, params=params, timeout=15)
        r.raise_for_status()
        return r.json() if r.text else None

    def post(self, path: str, body, **params):
        r = requests.post(self.url + path, headers=self.h, json=body, params=params, timeout=15)
        if not r.ok:
            raise RuntimeError(f"POST {path} -> {r.status_code}: {r.text[:300]}")
        return r.json() if r.text else None


def new_token(n: int = 20) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(n))


def ensure_profile(tb: TB) -> dict:
    profile = json.loads(PROFILE_FILE.read_text(encoding="utf-8"))
    page = tb.get("/api/deviceProfiles", pageSize=100, page=0, textSearch=profile["name"])
    for item in page["data"]:
        if item["name"] == profile["name"]:
            print(f"profile exists: {item['name']} - updating alarm rules")
            item["profileData"]["alarms"] = profile["profileData"]["alarms"]
            return tb.post("/api/deviceProfile", item)
    print(f"creating profile: {profile['name']}")
    return tb.post("/api/deviceProfile", profile)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--inactivity-s", type=int, default=30,
                        help="node-loss detection timeout (demo 30 s; production 2700 s = 3 x 15 min)")
    args = parser.parse_args()
    load_dotenv()
    tb = TB(required_env("TB_URL"), required_env("TB_USERNAME"), required_env("TB_PASSWORD"))
    profile = ensure_profile(tb)

    tokens: dict[str, str] = {}
    for spec in load_nodes():
        found = tb.get("/api/tenant/devices", pageSize=10, page=0, textSearch=spec.node_id)
        device = next((d for d in found["data"] if d["name"] == spec.node_id), None)
        if device is None:
            token = new_token()
            device = tb.post("/api/device", {
                "name": spec.node_id, "label": spec.name, "type": profile["name"],
                "deviceProfileId": profile["id"],
                "additionalInfo": {"description": f"{spec.site_type}, Bidur, Nuwakot"}},
                accessToken=token)
            print(f"created {spec.node_id}  token={token}")
        else:
            token = tb.get(f"/api/device/{device['id']['id']}/credentials")["credentialsId"]
            print(f"exists  {spec.node_id}  token={token}")
        tokens[spec.token_env] = token
        tb.post(f"/api/plugins/telemetry/DEVICE/{device['id']['id']}/attributes/SERVER_SCOPE",
                {"inactivityTimeout": args.inactivity_s * 1000,
                 "latitude": spec.latitude, "longitude": spec.longitude,
                 "site_type": spec.site_type})

    env = BASE_DIR / ".env"
    lines = env.read_text(encoding="utf-8").splitlines() if env.exists() else []
    lines = [ln for ln in lines if ln.split("=", 1)[0].strip() not in tokens]
    lines += [f"{k}={v}" for k, v in tokens.items()]
    env.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"tokens written to {env}")


if __name__ == "__main__":
    main()
