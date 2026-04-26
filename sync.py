#!/usr/bin/env python3
"""sync.py — Atualiza data.json.

Mudanças nesta versão:
- Horário em Brasília (BRT)
- Traz date_created e date_updated das tarefas ClickUp
- Calcula resumo por AM (backlog, tarefas paradas 30+ dias, urgentes)
- Identifica tarefas que precisam de ação (sem prazo OU paradas há +30 dias)
"""

import os
import json
import datetime
from datetime import timezone, timedelta
import time
import re
import requests

BRT = timezone(timedelta(hours=-3))

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
    'listing': 'Listing', 'logistics': 'Logística', 'logística': 'Logística',
    'marketing': 'Marketing', 'leadership [eucaliptus green tea]': 'Leadership',
    'leadership': 'Leadership', 'copy': 'Copy', 'design': 'Design',
    'seo': 'SEO', 'cx': 'CX',
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
    if task_title:
        umbrella = match_umbrella(task_title, valid_slugs)
        if umbrella:
            return umbrella
        matches = re.findall(r'\[([^\]]+)\]', task_title)
        for m in matches:
            term_slug = slugify(m.split('-')[0].split('/')[0].strip())
            if not term_slug: continue
            if term_slug in valid_slugs: return term_slug
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
        try: score = float(cell(row, 10))
        except: continue
        if not cname or score == 0: continue
        slug = slugify(cname)
        m = slug if slug in clients else next((k for k in clients if slugify(clients[k]['name']) == slug), None)
        if m: clients[m]['sc'].append({'canal': canal, 'score': int(score)})
    for c in clients.values():
        if c['sc']:
            c['score'] = round(sum(s['score'] for s in c['sc']) / len(c['sc']), 1)
    print(f"  ✓ scores aplicados")

    for sheet_name, field in [('Mapa de Logistica', 'log'), ('Mapa de ADS', 'ads_status')]:
        rows = read_sheet(service, MAPA_GERAL_ID, f"'{sheet_name}'!A2:AF500")
        if not rows: continue
        headers = rows[0]
        col_map = {ABBREV[str(h).strip()]: i for i, h in enumerate(headers) if str(h).strip() in ABBREV}
        for row in rows[1:]:
            if not row: continue
            name = row[0] if row else ''
            if not name: continue
            slug = slugify(name)
            m = slug if slug in clients else next((k for k in clients if slugify(clients[k]['name']) == slug), None)
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
                    't': 'wa', 'text': text, 'src': 'Pontos de Atenção · Mapa Geral'
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
            current_onda = a; continue
        if a.lower() == 'cliente': continue
        cliente = cell(row, 1)
        canal = cell(row, 4)
        if not cliente or not canal: continue
        slug = slugify(cliente)
        expansoes.setdefault(slug, []).append({
            'cliente': cliente, 'am': cell(row, 2) or '—',
            'prio': cell(row, 3) or '—', 'canal': canal,
            'ja_canal': cell(row, 5) or '—', 'gestao': cell(row, 6) or '—',
            'status': cell(row, 7) or '—', 'ctrl': cell(row, 8) or '',
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
            'archived': 'false', 'subtasks': 'true', 'include_closed': 'false',
            'page': page, 'space_ids[]': CLICKUP_CLIENT_SPACE_ID,
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
            if slug: team_folder_rescued += 1
            else: continue
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

        # Datas (em ISO, com timezone BRT pra ficar consistente)
        due_str = None
        if t.get('due_date'):
            try:
                due_str = datetime.datetime.fromtimestamp(int(t['due_date'])/1000, tz=BRT).strftime('%Y-%m-%d')
            except: pass

        created_str = None
        if t.get('date_created'):
            try:
                created_str = datetime.datetime.fromtimestamp(int(t['date_created'])/1000, tz=BRT).strftime('%Y-%m-%d')
            except: pass

        updated_str = None
        if t.get('date_updated'):
            try:
                updated_str = datetime.datetime.fromtimestamp(int(t['date_updated'])/1000, tz=BRT).strftime('%Y-%m-%d')
            except: pass

        assignees = t.get('assignees', [])
        owner_name = ', '.join(a['username'] for a in assignees[:2]) or '—'
        owner_ids = [a.get('id') for a in assignees if a.get('id')]

        task = {
            'n': t['name'],
            'st': (t.get('status') or {}).get('status', 'a iniciar'),
            'pri': (t.get('priority') or {}).get('priority') if t.get('priority') else None,
            'ow': owner_name,
            'ow_ids': owner_ids,
            'due': due_str,
            'created': created_str,
            'updated': updated_str,
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
        print(f"\n  🎯 {team_folder_rescued} tarefas de pastas de time resgatadas")

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
                    'pct': pct, 'count': len(active), 'total': len(total_active),
                    'active_clients': [clients[s]['name'] for s in active]
                }
    return pen


def compute_am_summary(clients, tasks_by_client, atencao):
    """Para cada AM, calcula backlog, tarefas paradas, urgentes, etc."""
    print("📊 Calculando resumo por AM...")
    today = datetime.datetime.now(BRT).date()

    # AM → list of client slugs
    am_clients = {}
    for slug, d in clients.items():
        am = d.get('am') or '—'
        if am in ('—', '', 'Ex Cliente'): continue
        am_clients.setdefault(am, []).append(slug)

    summary = {}
    for am, client_slugs in am_clients.items():
        client_data = [clients[s] for s in client_slugs]
        active_clients = [c for c in client_data if c['status'] == 'Ativo']
        scored = [c for c in client_data if c['score'] is not None]
        avg_score = round(sum(c['score'] for c in scored) / len(scored), 1) if scored else None
        critical_clients = [c for c in client_data if c['score'] is not None and c['score'] < 40]
        standby_clients = [c for c in client_data if c['status'] == 'Stand By']

        # Tarefas dos clientes do AM
        all_tasks = []
        for s in client_slugs:
            for t in tasks_by_client.get(s, []):
                if t.get('st') == 'feito': continue
                all_tasks.append({**t, 'client_slug': s, 'client_name': clients[s]['name']})

        # Categorizar tarefas
        urgent_tasks = [t for t in all_tasks if t.get('pri') == 'urgent']
        case_tasks = [t for t in all_tasks if t.get('st') == 'case aberto']
        waiting_tasks = [t for t in all_tasks if t.get('st') in ('aguardando cliente', 'aguardando retorno', 'aguardando aprovação')]
        no_due = [t for t in all_tasks if not t.get('due')]

        # Tarefas paradas (criadas/atualizadas há +30 dias E ainda abertas)
        stalled = []
        for t in all_tasks:
            ref_date_str = t.get('updated') or t.get('created')
            if not ref_date_str: continue
            try:
                ref_date = datetime.datetime.strptime(ref_date_str, '%Y-%m-%d').date()
                age = (today - ref_date).days
                if age >= 30:
                    stalled.append({**t, 'days_stalled': age})
            except: pass
        stalled.sort(key=lambda x: -x.get('days_stalled', 0))

        # Pontos de atenção dos clientes do AM
        atencao_count = sum(len(atencao.get(s, [])) for s in client_slugs)

        # Saúde do AM (0-100): baseada em score do portfólio + tarefas paradas + cases
        # Score base = score médio dos clientes (ou 50 se não tem)
        health = avg_score if avg_score is not None else 50
        # Penaliza por tarefas paradas (10 pontos a cada 5 stalled)
        health -= min(30, len(stalled) * 2)
        # Penaliza por cases abertos
        health -= min(20, len(case_tasks) * 5)
        # Penaliza por clientes críticos
        health -= min(20, len(critical_clients) * 5)
        health = max(0, min(100, round(health)))

        summary[am] = {
            'am': am,
            'total_clients': len(client_data),
            'active_clients': len(active_clients),
            'standby_clients': len(standby_clients),
            'avg_score': avg_score,
            'critical_clients': [{'name': c['name'], 'score': c['score'], 'slug': slugify(c['name'])} for c in critical_clients],
            'open_tasks_total': len(all_tasks),
            'urgent_tasks': len(urgent_tasks),
            'case_tasks': len(case_tasks),
            'waiting_tasks': len(waiting_tasks),
            'no_due_tasks': len(no_due),
            'stalled_tasks_count': len(stalled),
            'stalled_tasks': stalled[:20],  # top 20 mais paradas
            'urgent_tasks_list': urgent_tasks[:10],
            'case_tasks_list': case_tasks[:10],
            'atencao_count': atencao_count,
            'health': health,
            'clients_list': [{'name': c['name'], 'slug': slugify(c['name']), 'status': c['status'], 'score': c['score']} for c in client_data],
        }

    print(f"  ✓ Resumo de {len(summary)} AMs")
    return summary


def main():
    if not CLICKUP_TOKEN: raise RuntimeError("CLICKUP_TOKEN missing")
    if not GOOGLE_CREDS_JSON: raise RuntimeError("GOOGLE_CREDENTIALS_JSON missing")
    print("🚀 Sync SellersFlow iniciado")
    now_brt = datetime.datetime.now(BRT)
    print(f"  {now_brt.isoformat()} (BRT)\n")

    service = get_sheets_service()
    clients, atencao = read_mapa_geral(service)
    expansoes = read_expansoes(service)
    tasks, tasks_by_team = get_clickup_tasks_fast(clients.keys())
    vendas = read_vendas(service)
    penetration = compute_penetration(clients)
    am_summary = compute_am_summary(clients, tasks, atencao)

    bundle = {
        'generated_at': datetime.datetime.now(BRT).isoformat(),
        'clients': clients,
        'tasks': tasks,
        'tasks_by_team': tasks_by_team,
        'expansoes': expansoes,
        'atencao': atencao,
        'penetration': penetration,
        'calendar': CALENDAR_EVENTS,
        'vendas': vendas,
        'am_summary': am_summary,
    }

    out = os.path.join(os.path.dirname(__file__), 'data.json')
    with open(out, 'w', encoding='utf-8') as f:
        json.dump(bundle, f, ensure_ascii=False, separators=(',',':'))

    total_stalled = sum(s['stalled_tasks_count'] for s in am_summary.values())
    print(f"\n✅ data.json gerado:")
    print(f"   {len(clients)} clientes")
    print(f"   {sum(len(v) for v in tasks.values())} tarefas")
    print(f"   {sum(len(v) for v in expansoes.values())} expansões")
    print(f"   {sum(len(v) for v in atencao.values())} pontos de atenção")
    print(f"   {len(am_summary)} AMs · {total_stalled} tarefas paradas há 30+ dias")


if __name__ == '__main__':
    main()


# ═══════════════════════════════════════════════════════════════
# PLANILHA DE VENDAS (Motor de Meta Comercial)
# Nunca exibe valores R$ — só gera tendências qualitativas
# ═══════════════════════════════════════════════════════════════
VENDAS_ID = "1CfBQR8qGcC-YacljIKJsRSVRrZTjBUeCJh-H69_HOqU"


def read_vendas(service):
    """Lê Base_Historico, Premissas, Analise, Ranking_AM, Sensibilidade."""
    print("📊 Lendo Motor de Meta (vendas)...")

    # Base_Historico — tendência por cliente/canal
    rows = read_sheet(service, VENDAS_ID, "'Base_Historico'!A4:J2000")
    trends = {}
    for row in rows[1:]:
        key = cell(row, 1)
        canal = cell(row, 2)
        am = cell(row, 4)
        if not key: continue
        try:
            m1 = float(cell(row, 6) or 0)
            m2 = float(cell(row, 7) or 0)
            m3 = float(cell(row, 8) or 0)
            m4 = float(cell(row, 9) or 0)
        except: continue

        last = m4 if m4 > 0 else m3
        prev = m3 if m4 > 0 else m2
        var_pct = round((last - prev) / prev * 100, 1) if prev > 0 else 0
        trend = 'subindo' if var_pct > 5 else ('caindo' if var_pct < -5 else 'estagnado')

        client_name = key.replace(f' {canal}', '').strip()
        slug = slugify(client_name)

        trends[key] = {
            'client': client_name, 'slug': slug, 'canal': canal, 'am': am,
            'var_pct': var_pct, 'trend': trend,
        }
    print(f"  ✓ {len(trends)} tendências")

    # Premissas
    rows = read_sheet(service, VENDAS_ID, "'Premissas_Editaveis'!A4:S2000")
    premissas = {}
    for row in rows[1:]:
        key = cell(row, 1)
        if not key: continue
        premissas[key] = {
            'maturidade': cell(row, 6), 'dep_bestseller': cell(row, 7),
            'dias_ruptura': cell(row, 8), 'buy_box': cell(row, 16),
            'problema_logistico': cell(row, 17), 'concorrencia': cell(row, 18),
            'promocao': cell(row, 11), 'preco': cell(row, 12),
        }
    print(f"  ✓ {len(premissas)} premissas")

    # Analise — top subindo e caindo
    rows = read_sheet(service, VENDAS_ID, "'Analise'!A4:N30")
    top_subindo = []
    top_caindo = []
    for row in rows[1:]:
        name_up = cell(row, 1)
        if name_up:
            top_subindo.append({
                'key': name_up, 'canal': cell(row, 2), 'am': cell(row, 3),
                'var': round(float(cell(row, 4) or 0) * 100, 1) if cell(row, 4) else 0
            })
        name_down = cell(row, 11)
        if name_down:
            top_caindo.append({
                'key': name_down, 'canal': cell(row, 12), 'am': cell(row, 13),
                'var': round(float(cell(row, 14) or 0) * 100, 1) if cell(row, 14) else 0
            })
    print(f"  ✓ Top {len(top_subindo)} subindo, {len(top_caindo)} caindo")

    # Ranking AM (da planilha de vendas)
    rows = read_sheet(service, VENDAS_ID, "'Ranking_AM'!A10:R30")
    ranking_am = []
    for row in rows[1:]:
        am_name = cell(row, 2)
        if not am_name: continue
        try:
            ranking_am.append({
                'rank': int(float(cell(row, 1) or 0)),
                'am': am_name,
                'clientes': int(float(cell(row, 3) or 0)),
                'var_meta_pct': round(float(cell(row, 6) or 0) * 100, 1),
                'score_meta': round(float(cell(row, 7) or 0), 1),
                'score_cresc': round(float(cell(row, 8) or 0), 1),
                'score_carteira': round(float(cell(row, 9) or 0), 1),
                'score_oport': round(float(cell(row, 10) or 0), 1),
                'score_risco': round(float(cell(row, 11) or 0), 1),
                'score_cliente': round(float(cell(row, 12) or 0), 1),
                'score_final': round(float(cell(row, 13) or 0), 1),
                'rating': cell(row, 14),
                'subindo': int(float(cell(row, 15) or 0)),
                'estagnado': int(float(cell(row, 16) or 0)),
                'caindo': int(float(cell(row, 17) or 0)),
            })
        except: pass
    print(f"  ✓ Ranking de {len(ranking_am)} AMs")

    # Sensibilidade — semáforo por canal
    rows = read_sheet(service, VENDAS_ID, "'📊 Sensibilidade_Canal'!A5:T500")
    sensibilidade = {}
    for row in rows[1:]:
        key = cell(row, 1)
        if not key: continue
        sensibilidade[key] = {
            'canal': cell(row, 2),
            'status_tendencia': cell(row, 5),
            'semaforo': cell(row, 19),
            'acao_recomendada': cell(row, 20),
        }
    print(f"  ✓ {len(sensibilidade)} sensibilidades")

    # Visão agregada por canal
    canal_view = {}
    for key, t in trends.items():
        canal = t['canal']
        if canal not in canal_view:
            canal_view[canal] = {'subindo': 0, 'caindo': 0, 'estagnado': 0, 'clients': [], 'total_count': 0}
        canal_view[canal]['total_count'] += 1
        canal_view[canal][t['trend']] += 1
        # Sem valores R$, só proporções
        canal_view[canal]['clients'].append({
            'client': t['client'], 'slug': t['slug'],
            'var_pct': t['var_pct'], 'trend': t['trend'],
            'am': t['am'],
        })

    # Ordena clientes em cada canal
    for canal in canal_view:
        canal_view[canal]['clients'].sort(key=lambda x: x['var_pct'], reverse=True)
        total = canal_view[canal]['total_count']
        sub = canal_view[canal]['subindo']
        cai = canal_view[canal]['caindo']
        if total > 0:
            canal_view[canal]['pct_subindo'] = round(sub / total * 100)
            canal_view[canal]['pct_caindo'] = round(cai / total * 100)
            # Saúde do canal
            if cai > sub * 2:
                canal_view[canal]['saude'] = 'critico'
            elif cai > sub:
                canal_view[canal]['saude'] = 'atencao'
            elif sub > cai:
                canal_view[canal]['saude'] = 'saudavel'
            else:
                canal_view[canal]['saude'] = 'estavel'

    print(f"  ✓ Visão de {len(canal_view)} canais")

    return {
        'trends': trends,
        'premissas': premissas,
        'top_subindo': top_subindo,
        'top_caindo': top_caindo,
        'ranking_am': ranking_am,
        'sensibilidade': sensibilidade,
        'canal_view': canal_view,
    }
