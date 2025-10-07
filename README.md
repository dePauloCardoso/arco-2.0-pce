### arco-2.0-pce

Ferramenta de extração do WMS (Oracle Cloud) que:
- **busca dados** das entidades `order_hdr`, `order_dtl` e `order_status` via API;
- **gera CSVs** padronizados para cada entidade;
- **cruza os dados** com DuckDB para montar a base consolidada de status de pedidos;
- **envia os arquivos** para uma pasta no Google Drive (pasta comum ou Shared Drive).

---

### Arquitetura (alto nível)
- **`wms_client.py`**: cliente assíncrono (aiohttp) para paginação e robustez (retry/backoff).
- **`extractors/`**: normalização e geração de CSV em memória para cada entidade.
- **`main.py`**: orquestra extração, join com DuckDB e upload ao Drive.
- **`drive_client.py`**: autenticação e upload/update no Google Drive.
- **`config.py` / `config.json`**: configuração do WMS e do Drive (com overrides por variáveis de ambiente).

---

### Pré‑requisitos
- Python 3.12+
- Poetry (opcional, mas recomendado)
- Acesso à API do WMS (URL, usuário e senha)
- Credenciais válidas do Google (service account ou OAuth) com acesso de escrita na pasta de destino

---

### Instalação
Com Poetry:

```bash
poetry install
poetry run python -m pip install --upgrade pip
```

Sem Poetry (virtualenv padrão):

```bash
python -m venv .venv
.venv\Scripts\activate  # Windows
pip install -U pip
pip install -r requirements.txt  # se você mantiver um requirements.txt gerado do Poetry
```

---

### Configuração
Edite `config.json` (valores de exemplo):

```json
{
  "wms": {
    "base_url": "https://<tenant>.wms.ocs.oraclecloud.com/<org>",
    "username": "<usuario>",
    "password": "<senha>",
    "verify_ssl": true,
    "default_concurrency": 10,
    "default_timeout": 30.0,
    "default_retries": 3,
    "default_backoff_base": 0.5
  },
  "drive": {
    "client_secret_file": "client_secret.json",
    "token_file": "token.json",
    "folder_id": "<pasta_de_destino_no_drive>",
    "shared_drive_id": "<id_do_shared_drive_ou_remova>",
    "scopes": ["https://www.googleapis.com/auth/drive"]
  },
  "outputs": {
    "order_dtl": "order_dtl.csv",
    "order_hdr": "order_hdr.csv",
    "order_status": "order_status.csv"
  }
}
```

Overrides por variáveis de ambiente (opcional), conforme `config.py`:
- **`BASE_URL`**: substitui `wms.base_url`
- **`WMS_USERNAME`**: substitui `wms.username`
- **`WMS_PASSWORD`**: substitui `wms.password`
- **`WMS_VERIFY_SSL`**: quando "false" desabilita verificação SSL (não recomendado)

Coloque os arquivos de credenciais do Google na raiz do projeto (ou ajuste os caminhos em `config.json`):
- `client_secret.json`
- `token.json` (se usar OAuth de usuário)

---

### Autenticação com Google Drive
O projeto suporta dois modos, escolhidos automaticamente:
- **Service Account**: quando `client_secret.json` contém `"type": "service_account"` (ou quando `token.json` é uma chave de serviço). Garanta que a conta de serviço tenha acesso à pasta/Shared Drive.
- **OAuth de Usuário**: quando há `token.json` válido. Se o token expirar e houver `refresh_token`, ele será renovado.

Defina em `drive.folder_id` a pasta destino. Se usar Shared Drive, informe também `drive.shared_drive_id`.

---

### Como executar
Com Poetry:

```bash
poetry run python main.py
```

Python direto (ambiente ativado):

```bash
python main.py
```

O script:
1) Extrai `order_hdr`, `order_dtl` e `order_status` do WMS (com paginação). 
2) Gera CSVs individuais em memória. 
3) Usa DuckDB para cruzar as tabelas e gerar `base_status_pedidos_wms_sae.csv`. 
4) Faz upload dos CSVs para o Google Drive na pasta configurada; se o arquivo existir, faz update.

---

### Saídas geradas
- `order_hdr.csv`: cabeçalho de pedidos (campos normalizados, incluindo `order_nbr`, `status_id`, datas, dados de cliente e transporte etc.).
- `order_dtl.csv`: itens de pedidos (quantidades, itens, atributos e relacionamentos com o header via `order_id_id`).
- `order_status.csv`: dicionário de status (`id`, `description`).
- `base_status_pedidos_wms_sae.csv`: base consolidada com join e seleção de colunas chave (filial, datas/hora, remessa, item, quantidades, tipo, status descritivo, dados de cliente, NF, transportadora, etc.).

Observação: os CSVs individuais são enviados primeiro; o consolidado é enviado em seguida.

---

### Logs
Os logs são exibidos no console (nível INFO). Erros de rede/servidor no WMS fazem retry com backoff exponencial.

---

### Problemas comuns
- Credenciais do Google inválidas: confira `client_secret.json`/`token.json` e permissões da pasta.
- Acesso ao WMS negado: valide `BASE_URL`, usuário/senha e permissões na API.
- Sem dados retornados: verifique filtros do WMS e se há páginas a serem percorridas.

---

### Agendamento (opcional)
Em Windows, use o Agendador de Tarefas para rodar `python main.py` periodicamente no diretório do projeto, com o ambiente virtual ativado.

---

### Licença
Projeto interno. Se necessário, adicione uma licença apropriada.
