"""
main_db.py

Executa múltiplas queries SQL (base_status_pedidos_wms_sae.sql e produtividade_sae.sql)
no Postgres local, gera CSVs e envia para o Google Drive usando drive_client.py.

Dependências:
  pip install pandas psycopg[binary] google-api-python-client google-auth

Conexão ao Postgres via variáveis de ambiente:
  PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE

Configurações do Google Drive via config.json.
"""

import os
import logging
from typing import Optional

import pandas as pd

from config import load_config
from drive_client import authenticate_google_drive, upload_or_update_bytes

try:
    import psycopg
    _PSYCOPG_V3 = True
except Exception:
    try:
        import psycopg2 as psycopg2
        from psycopg2.extras import RealDictCursor
        _PSYCOPG_V3 = False
    except Exception:
        raise ImportError(
            "psycopg (v3) or psycopg2 is required. Install with: pip install psycopg[binary] or psycopg2-binary"
        )


# --------------------------------------------------------------------------
# Funções utilitárias
# --------------------------------------------------------------------------

def _read_sql_file(path: str) -> list[str]:
    """Lê um arquivo SQL e separa em múltiplos statements (split por ';')."""
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    return [stmt.strip() for stmt in content.split(";") if stmt.strip()]


def _connect() -> object:
    """Retorna conexão com Postgres usando psycopg3 ou psycopg2."""
    cfg = load_config()
    db_cfg = cfg.get("database", {}) if isinstance(cfg, dict) else {}

    host = db_cfg.get("host") or os.getenv("PGHOST", "localhost")
    port = db_cfg.get("port") or os.getenv("PGPORT", "5432")
    user = db_cfg.get("user") or os.getenv("PGUSER", os.getenv("USER", ""))
    password = db_cfg.get("password") or os.getenv("PGPASSWORD", "")
    dbname = db_cfg.get("database") or os.getenv("PGDATABASE", "postgres")

    try:
        port_int = int(port)
    except Exception:
        port_int = port

    if _PSYCOPG_V3:
        return psycopg.connect(host=host, port=port_int, user=user, password=password, dbname=dbname)
    else:
        return psycopg2.connect(host=host, port=port_int, user=user, password=password, dbname=dbname)


def _run_query_to_dataframe(conn, sql_path: str) -> pd.DataFrame:
    """Executa a query SQL e retorna um DataFrame."""
    if not os.path.exists(sql_path):
        raise FileNotFoundError(f"Arquivo SQL não encontrado: {sql_path}")

    statements = _read_sql_file(sql_path)
    if not statements:
        raise ValueError(f"Nenhum comando SQL encontrado em {sql_path}")

    logging.info("Executando query de: %s", sql_path)

    # Executa statements intermediários (DDL/DML) e o último SELECT
    if len(statements) > 1:
        for stmt in statements[:-1]:
            with conn.cursor() as cur:
                cur.execute(stmt)
                conn.commit()

    select_query = statements[-1]
    if _PSYCOPG_V3:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(select_query)
            rows = cur.fetchall()
            df = pd.DataFrame(rows)
    else:
        from psycopg2.extras import RealDictCursor
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(select_query)
            rows = cur.fetchall()
            df = pd.DataFrame(rows)

    if df.empty:
        logging.warning("A query em %s não retornou resultados.", sql_path)
    return df


def _upload_dataframe_to_drive(df: pd.DataFrame, drive_cfg: dict, file_name: str):
    """Envia um DataFrame ao Google Drive."""
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

    csv_bytes = df.to_csv(index=False, sep=",").encode("utf-8")

    logging.info("Fazendo upload de %s para o Google Drive...", file_name)
    upload_or_update_bytes(
        service=service,
        folder_id=folder_id,
        shared_drive_id=shared_drive_id,
        file_name=file_name,
        content_bytes=csv_bytes,
        mime_type="text/csv",
    )
    logging.info("Upload concluído: %s", file_name)


# --------------------------------------------------------------------------
# Execução principal
# --------------------------------------------------------------------------

def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    cfg = load_config()

    base_dir = os.path.dirname(__file__)
    conn = _connect()

    try:
        queries = [
            {
                "sql_file": os.path.join(base_dir, "sql", "base_status_pedidos_wms_sae.sql"),
                "output_csv": "pce.csv",
            },
            {
                "sql_file": os.path.join(base_dir, "sql", "produtividade_sae.sql"),
                "output_csv": "produtividade_sae.csv",
            },
        ]

        for q in queries:
            sql_path = q["sql_file"]
            csv_name = q["output_csv"]
            logging.info("Executando extração para %s", csv_name)
            df = _run_query_to_dataframe(conn, sql_path)
            if df is not None:
                _upload_dataframe_to_drive(df, cfg["drive"], csv_name)

        logging.info("✅ Todas as consultas foram executadas e enviadas com sucesso para o Drive.")

    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
