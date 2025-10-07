import json
import os
from typing import Any, Dict

_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")


def load_config() -> Dict[str, Any]:
    with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    # Environment overrides for WMS
    wms = cfg.get("wms", {})
    wms["base_url"] = os.getenv("BASE_URL", wms.get("base_url", "")).rstrip("/")
    wms["username"] = os.getenv("WMS_USERNAME", wms.get("username", ""))
    wms["password"] = os.getenv("WMS_PASSWORD", wms.get("password", ""))
    wms["verify_ssl"] = str(os.getenv("WMS_VERIFY_SSL", str(wms.get("verify_ssl", True)))).lower() != "false"

    cfg["wms"] = wms
    return cfg
