from typing import Any, Dict, List, Tuple

from wms_client import WMSClient
from utils import csv_bytes_from_dicts_fixed


def _normalize_order_dtl(order: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": order.get("id"),
        "create_user": order.get("create_user"),
        "create_ts": order.get("create_ts"),
        "mod_user": order.get("mod_user"),
        "mod_ts": order.get("mod_ts"),
        "order_id_id": order.get("order_id", {}).get("id"),
        "order_id_key": order.get("order_id", {}).get("key"),
        "item_id_id": order.get("item_id", {}).get("id"),
        "item_id_key": order.get("item_id", {}).get("key"),
        "ord_qty": order.get("ord_qty"),
        "orig_ord_qty": order.get("orig_ord_qty"),
        "alloc_qty": order.get("alloc_qty"),
        "req_cntr_nbr": order.get("req_cntr_nbr"),
        "po_nbr": order.get("po_nbr"),
        "shipment_nbr": order.get("shipment_nbr"),
        "dest_facility_attr_a": order.get("dest_facility_attr_a"),
        "dest_facility_attr_b": order.get("dest_facility_attr_b"),
        "dest_facility_attr_c": order.get("dest_facility_attr_c"),
        "ref_nbr_1": order.get("ref_nbr_1"),
        "vas_activity_code": order.get("vas_activity_code"),
        "cost": order.get("cost"),
        "sale_price": order.get("sale_price"),
        "host_ob_lpn_nbr": order.get("host_ob_lpn_nbr"),
        "spl_instr": order.get("spl_instr"),
        "batch_number_id": order.get("batch_number_id"),
        "voucher_nbr": order.get("voucher_nbr"),
        "voucher_amount": order.get("voucher_amount"),
        "voucher_exp_date": order.get("voucher_exp_date"),
        "req_pallet_nbr": order.get("req_pallet_nbr"),
        "lock_code": order.get("lock_code"),
        "serial_nbr": order.get("serial_nbr"),
        "voucher_print_count": order.get("voucher_print_count"),
        "ship_request_line": order.get("ship_request_line"),
        "unit_declared_value": order.get("unit_declared_value"),
        "externally_planned_load_nbr": order.get("externally_planned_load_nbr"),
        "invn_attr_id_id": order.get("invn_attr_id", {}).get("id"),
        "invn_attr_id_key": order.get("invn_attr_id", {}).get("key"),
        "invn_attr_id_url": order.get("invn_attr_id", {}).get("url"),
        "internal_text_field_1": order.get("internal_text_field_1"),
        "orig_item_code": order.get("orig_item_code"),
        "erp_source_line_ref": order.get("erp_source_line_ref"),
        "erp_source_shipment_ref": order.get("erp_source_shipment_ref"),
        "erp_fulfillment_line_ref": order.get("erp_fulfillment_line_ref"),
        "min_shipping_tolerance_percentage": order.get("min_shipping_tolerance_percentage"),
        "max_shipping_tolerance_percentage": order.get("max_shipping_tolerance_percentage"),
        "status_id": order.get("status_id"),
        "order_dtl_original_seq_nbr": order.get("order_dtl_original_seq_nbr"),
    }


def _fieldnames() -> List[str]:
    return [
        "id", "create_user", "create_ts", "mod_user", "mod_ts",
        "order_id_id", "order_id_key", "item_id_id", "item_id_key", "ord_qty", "orig_ord_qty", "alloc_qty",
        "req_cntr_nbr", "po_nbr", "shipment_nbr", "dest_facility_attr_a", "dest_facility_attr_b", "dest_facility_attr_c",
        "ref_nbr_1", "vas_activity_code", "cost", "sale_price", "host_ob_lpn_nbr", "spl_instr",
        "batch_number_id", "voucher_nbr", "voucher_amount", "voucher_exp_date", "req_pallet_nbr", "lock_code",
        "serial_nbr", "voucher_print_count", "ship_request_line", "unit_declared_value", "externally_planned_load_nbr",
        "invn_attr_id_id", "invn_attr_id_key", "invn_attr_id_url", "internal_text_field_1", "orig_item_code",
        "erp_source_line_ref", "erp_source_shipment_ref", "erp_fulfillment_line_ref",
        "min_shipping_tolerance_percentage", "max_shipping_tolerance_percentage", "status_id", "order_dtl_original_seq_nbr"
    ]


async def extract_order_dtl_csv_bytes(client: WMSClient) -> Tuple[str, bytes]:
    items: List[Dict[str, Any]] = await client.fetch_all("order_dtl")
    normalized = [_normalize_order_dtl(x) for x in items]
    csv_bytes = csv_bytes_from_dicts_fixed(normalized, _fieldnames())
    return "order_dtl.csv", csv_bytes
