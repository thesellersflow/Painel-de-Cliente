# Painel SellersFlow

Dashboard executivo que consolida dados das planilhas (Mapa Geral + Clientes Estratégicos) e do ClickUp em uma visão unificada. Atualiza automaticamente todo dia às 8:00 BRT via GitHub Actions.

## 📁 Arquivos

```
sellersflow-painel/
├── index.html          # O painel — abre direto no navegador
├── data.json           # Dados (gerados pelo sync.py)
├── sync.py             # Script de sincronização
├── requirements.txt    # Dependências Python
└── .github/workflows/sync.yml   # Automação diária
```

## 🚀 Setup (uma vez só)

### 1. Criar repositório no GitHub

Crie um repo novo e faça upload desta pasta inteira.

### 2. Ativar GitHub Pages

`Settings` → `Pages` → Source: `Deploy from branch` → Branch: `main` → `/root` → Save.

O painel fica em `https://SEU-USUARIO.github.io/NOME-DO-REPO/`

### 3. Criar as credenciais

**ClickUp Token:**
1. Vai em https://app.clickup.com/settings/apps
2. `Generate` em API Token
3. Copia o token (começa com `pk_...`)

**Google Service Account:**
1. Acessa https://console.cloud.google.com
2. Cria projeto novo (ou usa existente)
3. Ativa `Google Sheets API`
4. `IAM & Admin` → `Service Accounts` → `Create Service Account`
5. Baixa a chave JSON
6. **Compartilha as duas planilhas** com o email do service account (ex: `painel@projeto.iam.gserviceaccount.com`) como **Leitor**

### 4. Adicionar Secrets no GitHub

No repo: `Settings` → `Secrets and variables` → `Actions` → `New repository secret`

Adicione dois secrets:

| Nome | Valor |
|---|---|
| `CLICKUP_TOKEN` | O token do ClickUp (pk_...) |
| `GOOGLE_CREDENTIALS_JSON` | O conteúdo INTEIRO do JSON da service account (copia e cola tudo) |

### 5. Rodar o primeiro sync

`Actions` → `Sync Painel SellersFlow` → `Run workflow` → `Run workflow`.

Em ~1 minuto o `data.json` é atualizado e commitado. O painel já fica vivo.

## 🔄 Como funciona a atualização

- **Automática**: Todo dia às 8:00 BRT, o GitHub Actions roda o `sync.py`, que lê as planilhas e o ClickUp, gera o `data.json` novo e commita.
- **Manual**: A qualquer hora, vai em `Actions` → `Run workflow` pra forçar.

## 🧪 Rodar localmente (dev)

```bash
pip install -r requirements.txt

export CLICKUP_TOKEN="pk_..."
export GOOGLE_CREDENTIALS_JSON='{"type":"service_account",...}'

python sync.py

# Abre o painel
open index.html
```

Se abrir direto no browser (`file://`), o fetch do `data.json` pode falhar por CORS. Roda um servidor local:

```bash
python -m http.server 8000
# Abre http://localhost:8000
```

## 📊 Estrutura de dados

`data.json` tem:
- `clients`: 114 clientes (Ativo/Setup/Stand By/México/Outros) com canais, score, logística, ADS
- `tasks`: Tarefas por cliente (ClickUp)
- `tasks_by_team`: Mesmas tarefas agrupadas por time (Listing, Copy, Design, SEO, Logística)
- `expansoes`: Expansões estratégicas (Onda 1, 2, 3, 4)
- `atencao`: Pontos de atenção da planilha
- `penetration`: Penetração de canal por categoria (usado nos insights)
- `calendar`: Eventos promocionais dos marketplaces

## 🔧 Ajustes futuros

- **Adicionar cliente**: Adiciona na planilha Mapa Geral. No próximo sync ele aparece.
- **Adicionar ponto de atenção**: Preenche colunas G/H/I na aba "Pontos de Atenção" do Mapa Geral.
- **Ajustar eventos do calendário**: Edita a lista `CALENDAR_EVENTS` no topo do `sync.py`.
- **Adicionar nova fonte**: Adiciona função nova no `sync.py` e inclui no bundle.
