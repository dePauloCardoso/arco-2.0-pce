from typing import Any, Dict, List, Tuple

from wms_client import WMSClient
from utils import csv_bytes_from_dicts_fixed


def _normalize_order_status(status: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": status.get("id"),
        "description": status.get("description"),
    }


def _fieldnames() -> List[str]:
    return ["id", "description"]


async def extract_order_status_csv_bytes(client: WMSClient) -> Tuple[str, bytes]:
    items: List[Dict[str, Any]] = await client.fetch_all("order_status")
    normalized = [_normalize_order_status(x) for x in items]
    csv_bytes = csv_bytes_from_dicts_fixed(normalized, _fieldnames())
    return "order_status.csv", csv_bytes
