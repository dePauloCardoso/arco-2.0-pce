-- Query to build base_status_pedidos_wms_sae from raw tables
CREATE SCHEMA IF NOT EXISTS public;

SELECT
    h.facility_id_key AS filial,
    CAST(d.create_ts AS DATE) AS dt_criacao,
    TO_CHAR(d.create_ts, 'HH24:MI') AS hr_criacao,
    CAST(d.mod_ts AS DATE) AS dt_modificacao,
	TO_CHAR(d.mod_ts, 'HH24:MI') AS hr_modificacao,
    h.cust_short_text_1 AS orderm_frete,
    h.order_nbr AS remessa,
    d.item_id_key AS item,
    CAST(d.ord_qty AS INTEGER) AS qtd_pedido,
    CAST(d.orig_ord_qty AS INTEGER) AS qtd_pedido_original,
    CAST(d.alloc_qty AS INTEGER) AS qtd_alocada,
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
        WHEN h.status_id = 40 AND COALESCE(h.cust_field_2,'') <> '' THEN 'Faturado'
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
FROM public.raw_order_dtl d
LEFT JOIN public.raw_order_hdr h ON d.order_id_id = h.id
LEFT JOIN public.raw_order_status s ON h.status_id = s.id
WHERE h.order_type_id_key <> '91'
ORDER BY  CAST(d.create_ts AS DATE), TO_CHAR(d.create_ts, 'HH24:MI');