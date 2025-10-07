import asyncio
import os
import io
import logging
from typing import List, Tuple
import pandas as pd
import duckdb as ddb

from config import load_config
from wms_client import WMSClient
from drive_client import authenticate_google_drive, upload_or_update_bytes
from extractors.order_hdr import extract_order_hdr_csv_bytes
from extractors.order_dtl import extract_order_dtl_csv_bytes
from extractors.order_status import extract_order_status_csv_bytes


async def _extract_all(client: WMSClient) -> List[Tuple[str, bytes]]:
    results: List[Tuple[str, bytes]] = []

    hdr_name, hdr_bytes = await extract_order_hdr_csv_bytes(client)
    results.append((hdr_name, hdr_bytes))

    dtl_name, dtl_bytes = await extract_order_dtl_csv_bytes(client)
    results.append((dtl_name, dtl_bytes))

    st_name, st_bytes = await extract_order_status_csv_bytes(client)
    results.append((st_name, st_bytes))

    return results


def run() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = load_config()

    wms = cfg["wms"]
    drive_cfg = cfg["drive"]

    client = WMSClient(
        base_url=wms["base_url"],
        username=wms["username"],
        password=wms["password"],
        verify_ssl=wms.get("verify_ssl", True),
        concurrency=int(wms.get("default_concurrency", 10)),
        timeout_seconds=float(wms.get("default_timeout", 30.0)),
        retries=int(wms.get("default_retries", 3)),
        backoff_base=float(wms.get("default_backoff_base", 0.5)),
    )

    results = asyncio.run(_extract_all(client))

    name_to_bytes = {name: content for name, content in results}

    # Carrega CSVs de orders em memória como DataFrames
    dtl_df = None
    hdr_df = None
    st_df = None
    if "order_dtl.csv" in name_to_bytes and "order_hdr.csv" in name_to_bytes and "order_status.csv" in name_to_bytes:
        dtl_df = pd.read_csv(io.BytesIO(name_to_bytes["order_dtl.csv"]))
        hdr_df = pd.read_csv(io.BytesIO(name_to_bytes["order_hdr.csv"]))
        st_df = pd.read_csv(io.BytesIO(name_to_bytes["order_status.csv"]))

    combined_csv_bytes: bytes | None = None
    if dtl_df is not None and hdr_df is not None and st_df is not None:
        con = ddb.connect()
        con.register("dtl", dtl_df)
        con.register("hdr", hdr_df)
        con.register("st", st_df)
        combined_df = con.execute(
            """
            SELECT 
                h.facility_id_key AS filial,
                CAST(d.create_ts AS DATE) AS dt_criacao,
                CAST(d.create_ts AS TIME) AS hr_criacao,
                CAST(d.mod_ts AS DATE) AS dt_modificacao,
                CAST(d.mod_ts AS TIME) AS hr_modificacao,
                h.cust_short_text_1 AS orderm_frete,
                h.order_nbr AS remessa,
                d.item_id_key AS item,
                d.ord_qty AS qtd_pedido,
                d.orig_ord_qty AS qtd_pedido_original,
                d.alloc_qty AS qtd_alocada,
                h.order_type_id_key AS tipo_pedido,
                h.ord_date AS dt_ordem,
                h.req_ship_date AS dt_embarque_obrigatoria,
                 CASE
                     WHEN h.status_id = 0  THEN 'Criado'
                     WHEN h.status_id = 10 THEN 'Parcialmente alocado'
                     WHEN h.status_id = 20 THEN 'Alocado'
                     WHEN h.status_id = 25 THEN 'Em Separação'
                     WHEN h.status_id = 27 THEN 'Separado'
                     WHEN h.status_id = 30 THEN 'Em Conferência'
                     WHEN h.status_id = 40 AND h.cust_field_2 <> '' THEN 'Faturado'
                     WHEN h.status_id = 40 THEN 'Conferido'
                     WHEN h.status_id = 50 THEN 'Carregado'
                     WHEN h.status_id = 90 THEN 'Expedido'
                     WHEN h.status_id = 99 THEN 'Cancelado'
                     ELSE 'Desconhecido'
                 END AS status_remessa,
                h.cust_name AS nome_cliente,
                h.cust_addr AS endereco_cliente,	
                h.cust_addr2 AS numero_end_cliente,
                h.cust_city AS cidade_cliente,
                h.cust_state AS estado_cliente,
                h.cust_zip AS cep_cliente,
                h.cust_nbr AS cod_cliente,	 
                h.shipto_name AS cliente_entrega,
                h.shipto_addr AS endereco_entrega,
                h.shipto_addr2 AS numero_entrega, 	
                h.shipto_city AS cidade_cliente_entrega,	
                h.shipto_state AS estado_cliente_entrega,	
                h.shipto_zip AS cep_cliente_entrega,
                h.priority AS prioridade,
                CAST(h.order_shipped_ts AS DATE) AS data_expedicao,
                h.cust_field_2 AS nota_fiscal,
                h.cust_date_1 AS dt_faturamento,
                h.cust_short_text_2 AS erro_zero,
                h.cust_long_text_1 AS transportadora,
                h.cust_long_text_2 AS tipo_pedido_extra
            FROM dtl d
            LEFT JOIN hdr h ON d.order_id_id = h.id
            LEFT JOIN st  s ON h.status_id = s.id
            WHERE h.order_type_id_key <> '91'
            """
        ).df()
        combined_csv_bytes = combined_df.to_csv(index=False, sep=',').encode("utf-8")

    base_dir = os.path.dirname(__file__)
    client_secret_path = os.path.join(base_dir, drive_cfg["client_secret_file"])
    token_path = os.path.join(base_dir, drive_cfg.get("token_file", "token.json"))

    service = authenticate_google_drive(
        client_secret_file=client_secret_path,
        scopes=drive_cfg.get("scopes", ["https://www.googleapis.com/auth/drive"]),
        token_file=token_path,
    )
    folder_id = drive_cfg["folder_id"]
    shared_drive_id = drive_cfg.get("shared_drive_id")

    # Primeiro envia as extrações que não são orders
    for file_name, content_bytes in results:
        if file_name in ("order_dtl.csv", "order_hdr.csv", "order_status.csv"):
            continue
        upload_or_update_bytes(
            service=service,
            folder_id=folder_id,
            shared_drive_id=shared_drive_id,
            file_name=file_name,
            content_bytes=content_bytes,
            mime_type="text/csv",
        )
        logging.info("Uploaded %s to Drive folder %s", file_name, folder_id)

    # Depois envia o resultado combinado das orders
    if combined_csv_bytes is not None:
        upload_or_update_bytes(
            service=service,
            folder_id=folder_id,
            shared_drive_id=shared_drive_id,
            file_name="base_status_pedidos_wms_sae.csv",
            content_bytes=combined_csv_bytes,
            mime_type="text/csv",
        )
        logging.info("Uploaded %s to Drive folder %s", "base_status_pedidos_wms_sae.csv", folder_id)


if __name__ == "__main__":
    run()
