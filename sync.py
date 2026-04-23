#!/usr/bin/env python3
"""
sync.py — Atualiza data.json com dados das planilhas do Google Sheets e ClickUp.

Uso local:
  export CLICKUP_TOKEN="pk_..."
  export GOOGLE_CREDENTIALS_JSON='{"type":"service_account",...}'
  python sync.py

No GitHub Actions, essas variáveis vêm de Secrets.

O que ele faz:
  1. Lê "Mapa Geral de Operação" (clientes, canais, score, logística, ADS)
  2. Lê planilha de Clientes Estratégicos (expansões, pontos de atenção)
  3. Busca tarefas do ClickUp (GESTÃO de cada cliente + times)
  4. Calcula penetração por categoria (insights)
  5. Grava data.json na mesma pasta
"""

import os
import json
import datetime
import requests
from typing import Dict, List, Any

# ─── CONFIG ───
MAPA_GERAL_ID = "1cHK_RUzbwEbqStv3umBBCQLTs3-IugTYTZbLnreEzPE"
ESTRATEGICOS_ID = "1Knf6Fp2yMDcPtOdFG_kLM1N3An1Q44nlFCMIGw6tGog"
CLICKUP_WORKSPACE_ID = "9007167493"
CLICKUP_CLIENT_SPACE_ID = "90136482129"  # CLIENTE (NOVO MODELO)

CLICKUP_TOKEN = os.environ.get("CLICKUP_TOKEN")
GOOGLE_CREDS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON")

CALENDAR_EVENTS = [
    {"date":"2026-05-05","name":"Datas Duplas Mercado Livre (05/05)","channels":["Mercado Livre"],"type":"datas-duplas"},
    {"date":"2026-05-05","name":"Datas Duplas Shopee (05/05)","channels":["Shopee"],"type":"datas-duplas"},
    {"date":"2026-05-11","name":"Dia das Mães","channels":["Amazon BR","Mercado Livre","Shopee","Magalu"],"type":"data-comemorativa"},
    {"date":"2026-06-06","name":"Datas Duplas Shopee (06/06)","channels":["Shopee"],"type":"datas-duplas"},
    {"date":"2026-06-06","name":"Datas Duplas Mercado Livre (06/06)","channels":["Mercado Livre"],"type":"datas-duplas"},
    {"date":"2026-06-12","name":"Dia dos Namorados","channels":["Amazon BR","Mercado Livre","Shopee","Magalu"],"type":"data-comemorativa"},
    {"date":"2026-07-07","name":"Datas Duplas Shopee (07/07)","channels":["Shopee"],"type":"datas-duplas"},
    {"date":"2026-07-15","name":"Prime Day (estimado)","channels":["Amazon US","Amazon BR","Amazon Vendor"],"type":"amazon-event"},
    {"date":"2026-08-08","name":"Datas Duplas Shopee (08/08)","channels":["Shopee"],"type":"datas-duplas"},
    {"date":"2026-08-11","name":"Dia dos Pais","channels":["Amazon BR","Mercado Livre","Shopee","Magalu"],"type":"data-comemorativa"},
    {"date":"2026-09-09","name":"Datas Duplas Shopee (09/09)","channels":["Shopee"],"type":"datas-duplas"},
    {"date":"2026-10-10","name":"Datas Duplas Shopee (10/10)","channels":["Shopee"],"type":"datas-duplas"},
    {"date":"2026-10-15","name":"Dia das Crianças","channels":["Amazon BR","Mercado Livre","Shopee","Magalu"],"type":"data-comemorativa"},
    {"date":"2026-10-20","name":"Aniversário Magalu","channels":["Magalu"],"type":"marketplace-event"},
    {"date":"2026-11-11","name":"Datas Duplas Shopee (11/11)","channels":["Shopee"],"type":"datas-duplas"},
    {"date":"2026-11-27","name":"Black Friday","channels":["Amazon US","Amazon BR","Amazon Vendor","Mercado Livre","Shopee","Magalu","Walmart US"],"type":"black-friday"},
    {"date":"2026-11-30","name":"Cyber Monday","channels":["Amazon US","Amazon BR","Amazon Vendor","Walmart US"],"type":"black-friday"},
    {"date":"2026-12-12","name":"Datas Duplas Shopee (12/12)","channels":["Shopee"],"type":"datas-duplas"},
    {"date":"2026-12-25","name":"Natal","channels":["Amazon BR","Mercado Livre","Shopee","Magalu"],"type":"data-comemorativa"},
]

CANAL_COLS = {
    'Amazon US': 7, 'Amazon MX': 10, 'Amazon CA': 13, 'Amazon BR': 16, 'Amazon Vendor': 19,
    'TikTok US': 22, 'Site US': 25, 'Ebay US': 28, 'Walmart US': 31, 'Wayfair US': 34, 'Temu US': 37,
    'Mercado Livre': 40, 'Mercado Livre 1P': 43, 'Shopee': 46, 'Magalu': 49, 'Privalia': 52,
    'Netshoes': 55, 'RD': 58, 'Centauro': 61, 'MadeiraMadeira': 64, 'Olist': 67,
    'Decathlon': 70, 'Shein': 73, 'TikTok BR': 76, 'Site BR': 79
}

ABBREV = {
    'Amz US':'Amazon US','Amz MX':'Amazon MX','Amz CA':'Amazon CA','Amz BR':'Amazon BR','Amz Ven':'Amazon Vendor',
    'TikUS':'TikTok US','SiteUS':'Site US','Ebay':'Ebay US','Walmart':'Walmart US','Wayfair':'Wayfair US','Temu':'Temu US',
    'ML':'Mercado Livre','ML1P':'Mercado Livre 1P','Shopee':'Shopee','Magalu':'Magalu','Privalia':'Privalia',
    'Netshoes':'Netshoes','RD':'RD','Centauro':'Centauro','Madeira':'MadeiraMadeira','Olist':'Olist',
    'AliExp':'AliExpress','Shein':'Shein','TikBR':'TikTok BR','SiteBR':'Site BR'
}


def slugify(s: str) -> str:
    if not s:
        return ''
    s = str(s).lower()
    for a, b in [(' ',''),('.',''),('-',''),('/',''),('&',''),("'",''),
                 ('ã','a'),('ç','c'),('é','e'),('í','i'),('ú','u'),
                 ('ê','e'),('á','a'),('ô','o'),('õ','o'),('ü','u')]:
        s = s.replace(a, b)
    return s


# ═══════════════════════════════════════
# GOOGLE SHEETS
# ═══════════════════════════════════════
def get_sheets_service():
    """Build Sheets API service from service account credentials."""
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    creds_dict = json.loads(GOOGLE_CREDS_JSON)
    creds = service_account.Credentials.from_service_account_info(
        creds_dict, scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
    )
    return build('sheets', 'v4', credentials=creds)


def read_sheet(service, sheet_id: str, range_name: str) -> List[List]:
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id, range=range_name
        ).execute()
        return result.get('values', [])
    except Exception as e:
        print(f"  ⚠️  Erro lendo {range_name}: {e}")
        return []


def cell(row, idx):
    """Pega célula com segurança, retornando '' se não existir."""
    if idx - 1 < len(row):
        v = row[idx - 1]
        return str(v).strip() if v else ''
    return ''


def read_mapa_geral(service) -> Dict[str, Any]:
    """Lê a aba Mapa de Clientes + Score + Logística + ADS + Pontos de Atenção."""
    print("📊 Lendo Mapa Geral de Operação...")

    # Mapa de Clientes
    rows = read_sheet(service, MAPA_GERAL_ID, "'Mapa de Clientes'!A1:CC500")
    clients = {}
    for i, row in enumerate(rows[2:], start=3):
        name = cell(row, 1)
        status = cell(row, 4)
        if not name or status.lower() == 'inativo':
            continue

        slug = slugify(name)
        canais = {}
        for canal, col in CANAL_COLS.items():
            st = cell(row, col)
            comment = cell(row, col + 2) if col + 2 <= len(row) + 2 else ''
            if st and st.lower() not in ('não', 'nao', ''):
                canais[canal] = {'st': st, 'c': comment}

        clients[slug] = {
            'name': name,
            'am': cell(row, 2) or '—',
            'pm': cell(row, 3) or '—',
            'status': status,
            'cat': cell(row, 5) or '—',
            'ads': cell(row, 6) or '—',
            'canais': canais,
            'score': None,
            'sc': [],
        }

    print(f"  ✓ {len(clients)} clientes carregados")

    # Score do Seller
    score_rows = read_sheet(service, MAPA_GERAL_ID, "'Score do Seller'!A147:K6000")
    for row in score_rows:
        client_name = cell(row, 1)
        canal = cell(row, 3)
        try:
            score = float(cell(row, 10))
        except:
            continue
        if not client_name or score == 0:
            continue

        slug = slugify(client_name)
        # Fuzzy match
        matched = slug if slug in clients else next(
            (k for k in clients if slugify(clients[k]['name']) == slug), None
        )
        if matched:
            clients[matched]['sc'].append({'canal': canal, 'score': int(score)})

    for c in clients.values():
        if c['sc']:
            c['score'] = round(sum(s['score'] for s in c['sc']) / len(c['sc']), 1)

    print(f"  ✓ Scores aplicados")

    # Logística e ADS
    for sheet_name, field in [('Mapa de Logistica', 'log'), ('Mapa de ADS', 'ads_status')]:
        rows = read_sheet(service, MAPA_GERAL_ID, f"'{sheet_name}'!A2:AF500")
        if not rows:
            continue
        headers = rows[0]
        col_map = {}
        for i, h in enumerate(headers):
            h_clean = str(h).strip()
            if h_clean in ABBREV:
                col_map[ABBREV[h_clean]] = i
        for row in rows[1:]:
            name = row[0] if row else ''
            if not name:
                continue
            slug = slugify(name)
            matched = slug if slug in clients else next(
                (k for k in clients if slugify(clients[k]['name']) == slug), None
            )
            if matched:
                for canal, idx in col_map.items():
                    if idx < len(row):
                        v = str(row[idx]).strip()
                        if v and v not in ('N/A', '') and canal in clients[matched]['canais']:
                            clients[matched]['canais'][canal][field] = v

    print(f"  ✓ Logística e ADS aplicados")

    # Pontos de Atenção
    atencao = {}
    atencao_rows = read_sheet(service, MAPA_GERAL_ID, "'Pontos de Atenção'!A1:I500")
    if not atencao_rows:
        # Tenta outros nomes possíveis
        for alt in ['Pontos Atenção', 'Atenção', 'Pontos de atencao']:
            atencao_rows = read_sheet(service, MAPA_GERAL_ID, f"'{alt}'!A1:I500")
            if atencao_rows:
                break

    for row in atencao_rows[2:] if len(atencao_rows) > 2 else []:
        name = cell(row, 1)
        if not name:
            continue
        slug = slugify(name)
        if slug not in clients:
            continue
        # Colunas G, H, I = Atenção 1, 2, 3
        for i, col in enumerate([7, 8, 9]):
            text = cell(row, col)
            if text and len(text) > 2:
                atencao.setdefault(slug, []).append({
                    't': 'wa',
                    'text': text,
                    'src': 'Pontos de Atenção · Mapa Geral'
                })

    print(f"  ✓ {sum(len(v) for v in atencao.values())} pontos de atenção")

    return {'clients': clients, 'atencao': atencao}


def read_expansoes(service) -> Dict[str, List]:
    """Lê planilha de Clientes Estratégicos (expansões)."""
    print("📊 Lendo Clientes Estratégicos (Expansões)...")
    rows = read_sheet(service, ESTRATEGICOS_ID, "'Expansão Canais'!A1:I2000")
    expansoes = {}
    current_onda = None

    for row in rows:
        if not row:
            continue
        a = cell(row, 1)
        if 'ONDA' in a.upper():
            current_onda = a
            continue
        if a.lower() == 'cliente':
            continue

        cliente = cell(row, 1)
        canal = cell(row, 4)
        prio = cell(row, 3)
        if not cliente or not canal:
            continue

        slug = slugify(cliente)
        expansoes.setdefault(slug, []).append({
            'cliente': cliente,
            'am': cell(row, 2) or '—',
            'prio': prio or '—',
            'canal': canal,
            'ja_canal': cell(row, 5) or '—',
            'gestao': cell(row, 6) or '—',
            'status': cell(row, 7) or '—',
            'ctrl': cell(row, 8) or '',
            'onda': current_onda,
        })

    print(f"  ✓ {sum(len(v) for v in expansoes.values())} expansões em {len(expansoes)} clientes")
    return expansoes


# ═══════════════════════════════════════
# CLICKUP
# ═══════════════════════════════════════
def clickup_get(endpoint: str, params: dict = None) -> dict:
    url = f"https://api.clickup.com/api/v2/{endpoint}"
    headers = {"Authorization": CLICKUP_TOKEN}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"  ⚠️  ClickUp error ({endpoint}): {e}")
        return {}


def get_clickup_tasks() -> Dict[str, List]:
    """Busca tarefas das listas GESTÃO de cada cliente."""
    print("📋 Buscando tarefas do ClickUp...")
    tasks_by_client = {}

    # Busca todas as folders da space "CLIENTE (NOVO MODELO)"
    folders_data = clickup_get(f"space/{CLICKUP_CLIENT_SPACE_ID}/folder")
    folders = folders_data.get('folders', [])
    print(f"  ✓ {len(folders)} pastas de cliente encontradas")

    for folder in folders:
        client_name = folder['name'].strip()
        slug = slugify(client_name)

        # Busca listas dentro da pasta
        lists_data = clickup_get(f"folder/{folder['id']}/list")
        for lst in lists_data.get('lists', []):
            list_name = lst['name']
            # Pega tarefas de TODAS as listas (Gestão + canais específicos)
            tasks_data = clickup_get(f"list/{lst['id']}/task", {
                'archived': 'false',
                'include_closed': 'false',
                'subtasks': 'true',
            })
            for t in tasks_data.get('tasks', []):
                # Infere team baseado no nome da lista
                team = 'Gestão'
                ln = list_name.upper()
                if 'LISTING' in ln or 'CADASTRO' in ln:
                    team = 'Listing'
                elif 'COPY' in ln:
                    team = 'Copy'
                elif 'DESIGN' in ln:
                    team = 'Design'
                elif 'SEO' in ln:
                    team = 'SEO'
                elif 'LOGIST' in ln or 'LOGÍSTICA' in ln:
                    team = 'Logística'

                task = {
                    'n': t['name'],
                    'st': (t.get('status') or {}).get('status', 'a iniciar'),
                    'pri': (t.get('priority') or {}).get('priority') if t.get('priority') else None,
                    'ow': ', '.join(a['username'] for a in t.get('assignees', [])[:2]) or '—',
                    'due': datetime.datetime.fromtimestamp(int(t['due_date'])/1000).strftime('%Y-%m-%d') if t.get('due_date') else None,
                    'url': t['url'],
                    'team': team,
                    'list': list_name,
                }
                tasks_by_client.setdefault(slug, []).append(task)

        print(f"  ✓ {client_name}: {len(tasks_by_client.get(slug, []))} tarefas")

    # Agrega por time
    tasks_by_team = {}
    for slug, tl in tasks_by_client.items():
        for t in tl:
            team = t.get('team', 'Gestão')
            tasks_by_team.setdefault(team, []).append({**t, 'client': slug})

    total = sum(len(v) for v in tasks_by_client.values())
    print(f"  ✓ Total: {total} tarefas em {len(tasks_by_client)} clientes")

    return tasks_by_client, tasks_by_team


# ═══════════════════════════════════════
# INSIGHTS (penetração por categoria)
# ═══════════════════════════════════════
def compute_penetration(clients: dict) -> dict:
    categories = {}
    for slug, d in clients.items():
        cat = d.get('cat', 'Outros')
        categories.setdefault(cat, []).append(slug)

    pen = {}
    for cat, slugs in categories.items():
        for canal in ['Amazon US','Amazon BR','Amazon Vendor','Mercado Livre','Shopee','Magalu','Walmart US','Netshoes','RD']:
            active = [s for s in slugs if clients[s]['canais'].get(canal, {}).get('st') == 'Sim']
            total_active = [s for s in slugs if clients[s]['status'] == 'Ativo']
            if len(total_active) >= 2:
                pct = round(len(active) / len(total_active) * 100)
                pen.setdefault(cat, {})[canal] = {
                    'pct': pct,
                    'count': len(active),
                    'total': len(total_active),
                    'active_clients': [clients[s]['name'] for s in active],
                }
    return pen


# ═══════════════════════════════════════
# MAIN
# ═══════════════════════════════════════
def main():
    if not CLICKUP_TOKEN:
        raise RuntimeError("CLICKUP_TOKEN não encontrado")
    if not GOOGLE_CREDS_JSON:
        raise RuntimeError("GOOGLE_CREDENTIALS_JSON não encontrado")

    print("🚀 Iniciando sync SellersFlow")
    print(f"  Data/hora: {datetime.datetime.now().isoformat()}\n")

    service = get_sheets_service()

    mapa = read_mapa_geral(service)
    clients = mapa['clients']
    atencao = mapa['atencao']

    expansoes = read_expansoes(service)
    tasks, tasks_by_team = get_clickup_tasks()
    penetration = compute_penetration(clients)

    bundle = {
        'generated_at': datetime.datetime.now().isoformat(),
        'clients': clients,
        'tasks': tasks,
        'tasks_by_team': tasks_by_team,
        'expansoes': expansoes,
        'atencao': atencao,
        'penetration': penetration,
        'calendar': CALENDAR_EVENTS,
    }

    out_path = os.path.join(os.path.dirname(__file__), 'data.json')
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(bundle, f, ensure_ascii=False, separators=(',', ':'))

    print(f"\n✅ data.json gerado: {out_path}")
    print(f"  {len(clients)} clientes · {sum(len(v) for v in tasks.values())} tarefas · {sum(len(v) for v in expansoes.values())} expansões")


if __name__ == '__main__':
    main()
