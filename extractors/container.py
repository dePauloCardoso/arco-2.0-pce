from typing import Any, Dict, List, Tuple

from wms_client import WMSClient
from utils import flatten_one_level, csv_bytes_from_dicts_dynamic


async def extract_container_csv_bytes(client: WMSClient) -> Tuple[str, bytes]:
    items: List[Dict[str, Any]] = await client.fetch_all("container")
    flattened = [flatten_one_level(x) for x in items]
    _, csv_bytes = csv_bytes_from_dicts_dynamic(flattened)
    return "container.csv", csv_bytes
