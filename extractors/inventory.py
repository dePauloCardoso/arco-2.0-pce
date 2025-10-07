from typing import Any, Dict, List, Tuple

from wms_client import WMSClient
from utils import flatten_one_level, to_scalar, csv_bytes_from_dicts_dynamic


def _flatten_inventory_record(inv: Dict[str, Any]) -> Dict[str, Any]:
    flat = flatten_one_level(inv)
    qty_val = inv.get("curr_qty")
    try:
        flat["curr_qty"] = int(qty_val) if qty_val is not None else 0
    except (TypeError, ValueError):
        flat["curr_qty"] = 0
    return flat


async def extract_inventory_csv_bytes(client: WMSClient) -> Tuple[str, bytes]:
    items: List[Dict[str, Any]] = await client.fetch_all("inventory")
    flattened = [_flatten_inventory_record(x) for x in items]
    _, csv_bytes = csv_bytes_from_dicts_dynamic(flattened)
    return "inventory.csv", csv_bytes
