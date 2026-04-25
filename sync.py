#!/usr/bin/env python3
"""sync.py — Atualiza data.json.

Regras principais:
- Integralmedica / Nutrify / Darkness → tudo consolidado como INTEGRALMEDICA
- Pastas de times (Listing/Marketing/Logistics) não são clientes:
  tarefas dentro delas vão pro cliente identificado pelo [Nome] no título
- Aliases diretos para casos conhecidos (Dux, Trio, etc.)
"""

import os
import json
import datetime
from datetime import timezone, timedelta

BRT = timezone(timedelta(hours=-3))
import time
import re
import requests

MAPA_GERAL_ID = "1cHK_RUzbwEbqStv3umBBCQLTs3-IugTYTZbLnreEzPE"
ESTRATEGICOS_ID = "1Knf6Fp2yMDcPtOdFG_kLM1N3An1Q44nlFCMIGw6tGog"
CLICKUP_TEAM_ID = "9007167493"
CLICKUP_CLIENT_SPACE_ID = "90136482129"

CLICKUP_TOKEN = os.environ.get("CLICKUP_TOKEN")
GOOGLE_CREDS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON")


UMBRELLA_BRANDS = [
    {
        'keywords': ['integralmedica', 'integralmédica', 'integral medica', 'integral médica',
                     'nutrify', 'darkness'],
        'target_slug_candidates': ['integralmedica', 'integralmedicabrg', 'integralmédica'],
    },
]


CLICKUP_ALIASES = {
    'dux': 'duxnutrition',
    'dux nutrition': 'duxnutrition',
    'trio': 'triocoffee',
    'trio coffe': 'triocoffee',
    'trio coffee': 'triocoffee',
    'simple': 'simpleorganic',
    'simple organic': 'simpleorganic',
    'plie': 'plie',
    'plie us': 'plie',
    'plié': 'plie',
    'jack & milo': 'jackmilo',
    'jack e milo': 'jackmilo',
    'beauty': 'beautycolor',
    'beauty color': 'beautycolor',
    'kamaleão': 'kamaleaocolor',
    'kamaleao': 'kamaleaocolor',
    'kamaleão color': 'kamaleaocolor',
    'neo brasil': 'neobrasil',
    'neobrasil': 'neobrasil',
    'sfor': 'sforplast',
    'aho aloe': 'ahoaloe',
    'z2': 'z2foods',
    'z2 foods': 'z2foods',
    'colores de mexico': 'coloresdelmexico',
    'colores de mexico (mx)': 'coloresdelmexico',
    'colores del mexico': 'coloresdelmexico',
    'moonrise': 'moonrise',
    'moonrise (mx)': 'moonrise',
}


TEAM_FOLDERS = {
    'listing': 'Listing',
    'logistics': 'Logística',
    'logística': 'Logística',
    'marketing': 'Marketing',
    'leadership [eucaliptus green tea]': 'Leadership',
    'leadership': 'Leadership',
    'copy': 'Copy',
    'design': 'Design',
    'seo': 'SEO',
    'cx': 'CX',
}


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


def slugify(s):
    if not s: return ''
    s = str(s).lower().strip()
    for a, b in [(' ',''),('.',''),('-',''),('/',''),('&',''),("'",''),('(',''),(')','')]:
        s = s.replace(a, b)
    for a, b in [('ã','a'),('ç','c'),('é','e'),('í','i'),('ú','u'),
                 ('ê','e'),('á','a'),('ô','o'),('õ','o'),('ü','u')]:
        s = s.replace(a, b)
    return s


def cell(row, idx):
    if idx - 1 < len(row):
        v = row[idx - 1]
        return str(v).strip() if v else ''
    return ''


def match_umbrella(name, valid_slugs):
    if not name: return None
    low = name.lower()
    for brand in UMBRELLA_BRANDS:
        for kw in brand['keywords']:
            if kw.lower() in low:
                for candidate in brand['target_slug_candidates']:
                    if candidate in valid_slugs:
                        return candidate
    return None


def match_client_slug(folder_name, valid_slugs, task_title=None):
    # Prioridade 1: umbrella brand no folder
    if folder_name:
        umbrella = match_umbrella(folder_name, valid_slugs)
        if umbrella:
            return umbrella

        raw = folder_name.lower().strip()
        if raw in CLICKUP_ALIASES:
            target = CLICKUP_ALIASES[raw]
            if target in valid_slugs:
                return target

        slug = slugify(folder_name)
        if slug in valid_slugs:
            return slug

        if len(slug) >= 4:
            for vs in valid_slugs:
                if vs.startswith(slug) or slug.startswith(vs):
                    return vs

        if len(slug) >= 5:
            for vs in valid_slugs:
                if slug in vs or vs in slug:
                    return vs

    # Prioridade 2: tenta no título
    if task_title:
        umbrella = match_umbrella(task_title, valid_slugs)
        if umbrella:
            return umbrella

        matches = re.findall(r'\[([^\]]+)\]', task_title)
        for m in matches:
            term_slug = slugify(m.split('-')[0].split('/')[0].strip())
            if not term_slug:
                continue
            if term_slug in valid_slugs:
                return term_slug
            if len(term_slug) >= 4:
                for vs in valid_slugs:
                    if term_slug in vs or vs in term_slug:
                        return vs

    return None


def get_sheets_service():
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    creds = service_account.Credentials.from_service_account_info(
        json.loads(GOOGLE_CREDS_JSON),
        scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
    )
    return build('sheets', 'v4', credentials=creds)


def read_sheet(service, sheet_id, range_name):
    try:
        result = service.spreadsheets().values().get(spreadsheetId=sheet_id, range=range_name).execute()
        return result.get('values', [])
    except Exception as e:
        print(f"  ⚠️  {range_name}: {e}")
        return []


def read_mapa_geral(service):
    print("📊 Lendo Mapa Geral...")
    rows = read_sheet(service, MAPA_GERAL_ID, "'Mapa de Clientes'!A1:CC500")
    clients = {}
    for row in rows[2:]:
        name = cell(row, 1)
        status = cell(row, 4)
        if not name or status.lower() == 'inativo':
            continue
        slug = slugify(name)
        canais = {}
        for canal, col in CANAL_COLS.items():
            st = cell(row, col)
            comment = cell(row, col + 2) if col + 2 <= len(row) + 2 else ''
            if st and st.lower() not in ('não','nao',''):
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
            'sc': []
        }
    print(f"  ✓ {len(clients)} clientes")

    for row in read_sheet(service, MAPA_GERAL_ID, "'Score do Seller'!A147:K6000"):
        cname = cell(row, 1)
        canal = cell(row, 3)
        try:
            score = float(cell(row, 10))
        except:
            continue
        if not cname or score == 0:
            continue
        slug = slugify(cname)
        m = slug if slug in clients else next(
            (k for k in clients if slugify(clients[k]['name']) == slug), None
        )
        if m:
            clients[m]['sc'].append({'canal': canal, 'score': int(score)})
    for c in clients.values():
        if c['sc']:
            c['score'] = round(sum(s['score'] for s in c['sc']) / len(c['sc']), 1)
    print(f"  ✓ scores aplicados")

    for sheet_name, field in [('Mapa de Logistica', 'log'), ('Mapa de ADS', 'ads_status')]:
        rows = read_sheet(service, MAPA_GERAL_ID, f"'{sheet_name}'!A2:AF500")
        if not rows:
            continue
        headers = rows[0]
        col_map = {ABBREV[str(h).strip()]: i for i, h in enumerate(headers) if str(h).strip() in ABBREV}
        for row in rows[1:]:
            if not row: continue
            name = row[0] if row else ''
            if not name: continue
            slug = slugify(name)
            m = slug if slug in clients else next(
                (k for k in clients if slugify(clients[k]['name']) == slug), None
            )
            if m:
                for canal, idx in col_map.items():
                    if idx < len(row):
                        v = str(row[idx]).strip()
                        if v and v not in ('N/A','') and canal in clients[m]['canais']:
                            clients[m]['canais'][canal][field] = v
    print(f"  ✓ logística + ADS aplicados")

    atencao = {}
    rows = read_sheet(service, MAPA_GERAL_ID, "'Pontos de Atenção'!A1:I500")
    for row in rows[2:] if len(rows) > 2 else []:
        name = cell(row, 1)
        if not name: continue
        slug = slugify(name)
        if slug not in clients: continue
        for col in [7, 8, 9]:
            text = cell(row, col)
            if text and len(text) > 2:
                atencao.setdefault(slug, []).append({
                    't': 'wa',
                    'text': text,
                    'src': 'Pontos de Atenção · Mapa Geral'
                })
    print(f"  ✓ {sum(len(v) for v in atencao.values())} pontos de atenção")
    return clients, atencao


def read_expansoes(service):
    print("📊 Lendo Expansões...")
    rows = read_sheet(service, ESTRATEGICOS_ID, "'Expansão Canais'!A1:I2000")
    expansoes = {}
    current_onda = None
    for row in rows:
        if not row: continue
        a = cell(row, 1)
        if 'ONDA' in a.upper():
            current_onda = a
            continue
        if a.lower() == 'cliente': continue
        cliente = cell(row, 1)
        canal = cell(row, 4)
        if not cliente or not canal: continue
        slug = slugify(cliente)
        expansoes.setdefault(slug, []).append({
            'cliente': cliente,
            'am': cell(row, 2) or '—',
            'prio': cell(row, 3) or '—',
            'canal': canal,
            'ja_canal': cell(row, 5) or '—',
            'gestao': cell(row, 6) or '—',
            'status': cell(row, 7) or '—',
            'ctrl': cell(row, 8) or '',
            'onda': current_onda
        })
    print(f"  ✓ {sum(len(v) for v in expansoes.values())} expansões")
    return expansoes


def get_clickup_tasks_fast(valid_slugs):
    print("📋 Buscando tarefas ClickUp...")
    headers = {"Authorization": CLICKUP_TOKEN}
    valid_set = set(valid_slugs)

    all_tasks = []
    page = 0
    unmatched_folders = {}

    while page < 25:
        url = f"https://api.clickup.com/api/v2/team/{CLICKUP_TEAM_ID}/task"
        params = {
            'archived': 'false',
            'subtasks': 'true',
            'include_closed': 'false',
            'page': page,
            'space_ids[]': CLICKUP_CLIENT_SPACE_ID,
        }
        try:
            r = requests.get(url, headers=headers, params=params, timeout=60)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"  ⚠️  page {page}: {e}")
            break

        tasks = data.get('tasks', [])
        if not tasks: break
        all_tasks.extend(tasks)
        print(f"  ✓ page {page}: +{len(tasks)} (total {len(all_tasks)})")
        page += 1
        if len(tasks) < 100: break
        time.sleep(0.3)

    tasks_by_client = {}
    team_folder_rescued = 0

    for t in all_tasks:
        folder_name = (t.get('folder') or {}).get('name', '').strip()
        list_name = (t.get('list') or {}).get('name', '')
        task_title = t.get('name', '')

        if not folder_name: continue

        folder_low = folder_name.lower()
        is_team_folder = folder_low in TEAM_FOLDERS

        if is_team_folder:
            slug = match_client_slug(None, valid_set, task_title=task_title)
            if slug:
                team_folder_rescued += 1
            else:
                continue
        else:
            slug = match_client_slug(folder_name, valid_set, task_title=task_title)
            if not slug:
                unmatched_folders[folder_name] = unmatched_folders.get(folder_name, 0) + 1
                continue

        team = 'Gestão'
        if is_team_folder:
            team = TEAM_FOLDERS[folder_low]
        else:
            ln = list_name.upper()
            if 'LISTING' in ln or 'CADASTRO' in ln: team = 'Listing'
            elif 'COPY' in ln: team = 'Copy'
            elif 'DESIGN' in ln: team = 'Design'
            elif 'SEO' in ln: team = 'SEO'
            elif 'LOGIST' in ln: team = 'Logística'
            elif 'CX' in ln: team = 'CX'

        task = {
            'n': t['name'],
            'st': (t.get('status') or {}).get('status', 'a iniciar'),
            'pri': (t.get('priority') or {}).get('priority') if t.get('priority') else None,
            'ow': ', '.join(a['username'] for a in t.get('assignees', [])[:2]) or '—',
            'due': datetime.datetime.fromtimestamp(int(t['due_date'])/1000).strftime('%Y-%m-%d') if t.get('due_date') else None,
            'url': t['url'],
            'team': team,
            'list': list_name,
            'folder': folder_name,
        }
        tasks_by_client.setdefault(slug, []).append(task)

    if unmatched_folders:
        print(f"\n  ⚠️  {len(unmatched_folders)} pastas sem match:")
        for f, n in sorted(unmatched_folders.items(), key=lambda x: -x[1]):
            print(f"     - {f} ({n} tarefas)")

    if team_folder_rescued:
        print(f"\n  🎯 {team_folder_rescued} tarefas de pastas de time resgatadas pelo [Cliente] no título")

    tasks_by_team = {}
    for slug, tl in tasks_by_client.items():
        for t in tl:
            team = t.get('team', 'Gestão')
            tasks_by_team.setdefault(team, []).append({**t, 'client_slug': slug})

    total = sum(len(v) for v in tasks_by_client.values())
    print(f"\n  ✓ Total: {total} tarefas em {len(tasks_by_client)} clientes")
    return tasks_by_client, tasks_by_team


def compute_penetration(clients):
    categories = {}
    for slug, d in clients.items():
        categories.setdefault(d.get('cat', 'Outros'), []).append(slug)
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
                    'active_clients': [clients[s]['name'] for s in active]
                }
    return pen


def main():
    if not CLICKUP_TOKEN: raise RuntimeError("CLICKUP_TOKEN missing")
    if not GOOGLE_CREDS_JSON: raise RuntimeError("GOOGLE_CREDENTIALS_JSON missing")
    print("🚀 Sync SellersFlow iniciado")
    print(f"  {datetime.datetime.now(BRT).isoformat()}\n")

    service = get_sheets_service()
    clients, atencao = read_mapa_geral(service)
    expansoes = read_expansoes(service)
    tasks, tasks_by_team = get_clickup_tasks_fast(clients.keys())
    penetration = compute_penetration(clients)

    bundle = {
        'generated_at': datetime.datetime.now(BRT).isoformat(),
        'clients': clients, 'tasks': tasks, 'tasks_by_team': tasks_by_team,
        'expansoes': expansoes, 'atencao': atencao,
        'penetration': penetration, 'calendar': CALENDAR_EVENTS
    }

    out = os.path.join(os.path.dirname(__file__), 'data.json')
    with open(out, 'w', encoding='utf-8') as f:
        json.dump(bundle, f, ensure_ascii=False, separators=(',',':'))

    print(f"\n✅ data.json gerado:")
    print(f"   {len(clients)} clientes")
    print(f"   {sum(len(v) for v in tasks.values())} tarefas")
    print(f"   {sum(len(v) for v in expansoes.values())} expansões")
    print(f"   {sum(len(v) for v in atencao.values())} pontos de atenção")


if __name__ == '__main__':
    main()
