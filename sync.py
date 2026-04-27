#!/usr/bin/env python3
"""sync.py — Atualiza data.json com dados das planilhas + ClickUp.

Versão com:
- time.sleep(2) entre chamadas ao Google Sheets (resolve rate limit)
- Leitura completa da planilha de vendas (Motor de Meta)
- Consolidação de marcas guarda-chuva
- Vendas cruzadas por AM
"""

import os
import json
import datetime
from datetime import timezone, timedelta
import time
import re
import requests
from typing import Dict, Any, List

BRT = timezone(timedelta(hours=-3))

MAPA_GERAL_ID = "1cHK_RUzbwEbqStv3umBBCQLTs3-IugTYTZbLnreEzPE"
ESTRATEGICOS_ID = "1Knf6Fp2yMDcPtOdFG_kLM1N3An1Q44nlFCMIGw6tGog"
VENDAS_ID = "1CfBQR8qGcC-YacljIKJsRSVRrZTjBUeCJh-H69_HOqU"
CLICKUP_TEAM_ID = "9007167493"
CLICKUP_CLIENT_SPACE_ID = "90136482129"

CLICKUP_TOKEN = os.environ.get("CLICKUP_TOKEN")
GOOGLE_CREDS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON")

# Colunas de canais no Mapa de Clientes (posição 1-indexed)
CANAL_COLS = {
    'Amazon BR': 7, 'Amazon US': 10, 'Amazon MX': 13, 'Amazon CA': 16,
    'Amazon Vendor': 19, 'Mercado Livre': 22, 'Shopee': 25, 'Magalu': 28,
    'TikTok BR': 31, 'TikTok US': 34, 'Walmart': 37, 'Shein': 40,
    'Leroy Merlin': 43, 'Casas Bahia': 46, 'Netshoes': 49,
    'Centauro': 52, 'Dafiti': 55, 'Madeira Madeira': 58,
    'RD Farma': 61, 'Pague Menos': 64, 'Ultrafarma': 67,
    'Site Próprio': 70, 'Canal 1': 73, 'Canal 2': 76, 'Canal 3': 79,
}

# Consolidação de marcas guarda-chuva
UMBRELLA_BRANDS = [
    {
        'keywords': ['integralmedica', 'integralmédica', 'integral medica',
                     'integral médica', 'nutrify', 'darkness'],
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
    'moonrise mx': 'moonrise',
    'granado': 'granado',
    'granado perfumaria': 'granado',
    'phebo': 'granado',
    'yvy': 'yvybrasil',
    'yvy brasil': 'yvybrasil',
    'hidrolight': 'hidrolight',
}

# Eventos de calendário
CALENDAR_EVENTS = [
    {'title': 'Reunião Semanal de AMs', 'dow': 1, 'time': '10:00'},
    {'title': 'Review Estratégico', 'dow': 4, 'time': '14:00'},
]


# ═══════════════════════════════════════
# UTILS
# ═══════════════════════════════════════
def slugify(s):
    s = s.lower().strip()
    for a, b in [(' ', ''), ('.', ''), (',', ''), ('-', ''), ('/', ''),
                 ('&', ''), ("'", ''),
                 ('ã', 'a'), ('ç', 'c'), ('é', 'e'), ('í', 'i'), ('ú', 'u'),
                 ('ê', 'e'), ('á', 'a'), ('ô', 'o'), ('õ', 'o'), ('ü', 'u')]:
        s = s.replace(a, b)
    return s


def cell(row, idx):
    """Pega célula com segurança (1-indexed)."""
    if idx - 1 < len(row):
        v = row[idx - 1]
        return str(v).strip() if v else ''
    return ''


def match_client_slug(folder_name, valid_slugs):
    """Tenta casar o nome da pasta do ClickUp com um slug válido."""
    if not folder_name:
        return None
    raw = folder_name.lower().strip()

    # 1) Alias direto
    if raw in CLICKUP_ALIASES:
        target = CLICKUP_ALIASES[raw]
        if target in valid_slugs:
            return target

    # 2) Slug direto
    slug = slugify(folder_name)
    if slug in valid_slugs:
        return slug

    # 3) Match por prefixo
    for vs in valid_slugs:
        if vs.startswith(slug) or slug.startswith(vs):
            if len(slug) >= 3:
                return vs

    # 4) Match parcial
    for vs in valid_slugs:
        if len(slug) >= 4 and (slug in vs or vs in slug):
            return vs

    # 5) Umbrella brands
    for ub in UMBRELLA_BRANDS:
        for kw in ub['keywords']:
            if kw in raw:
                for candidate in ub['target_slug_candidates']:
                    if candidate in valid_slugs:
                        return candidate

    return None


# ═══════════════════════════════════════
# GOOGLE SHEETS (com rate limit fix)
# ═══════════════════════════════════════
_sheets_call_count = 0


def get_sheets_service():
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    creds = service_account.Credentials.from_service_account_info(
        json.loads(GOOGLE_CREDS_JSON),
        scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
    )
    return build('sheets', 'v4', credentials=creds)


def read_sheet(service, sheet_id, range_name):
    """Lê uma aba com sleep entre chamadas pra não estourar rate limit."""
    global _sheets_call_count
    _sheets_call_count += 1

    # A cada 3 chamadas, espera 2s pra não estourar o rate limit de 60/min
    if _sheets_call_count > 1:
        time.sleep(2)

    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id, range=range_name
        ).execute()
        rows = result.get('values', [])
        print(f"    ✓ {range_name} → {len(rows)} linhas")
        return rows
    except Exception as e:
        print(f"    ⚠️  {range_name}: {e}")
        # Retry uma vez após esperar 5s (pode ser rate limit temporário)
        time.sleep(5)
        try:
            result = service.spreadsheets().values().get(
                spreadsheetId=sheet_id, range=range_name
            ).execute()
            rows = result.get('values', [])
            print(f"    ✓ {range_name} → {len(rows)} linhas (retry)")
            return rows
        except Exception as e2:
            print(f"    ❌ {range_name}: falhou no retry: {e2}")
            return []


# ═══════════════════════════════════════
# LEITURA: MAPA GERAL
# ═══════════════════════════════════════
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
            if st and st.lower() not in ('não', 'nao', ''):
                canais[canal] = {'st': st}
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
    print(f"  ✓ {len(clients)} clientes")

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
        if slug in clients:
            clients[slug]['sc'].append({'canal': canal, 'score': score})
            if clients[slug]['score'] is None or score > clients[slug]['score']:
                clients[slug]['score'] = score

    scored = sum(1 for c in clients.values() if c['score'] is not None)
    print(f"  ✓ {scored} clientes com score")

    # Logística
    log_rows = read_sheet(service, MAPA_GERAL_ID, "'Logística'!A3:Z500")
    for row in log_rows:
        name = cell(row, 1)
        if not name:
            continue
        slug = slugify(name)
        if slug in clients:
            clients[slug]['logistica'] = cell(row, 5) or '—'

    # ADS
    ads_rows = read_sheet(service, MAPA_GERAL_ID, "'ADS'!A3:Z500")
    for row in ads_rows:
        name = cell(row, 1)
        if not name:
            continue
        slug = slugify(name)
        if slug in clients:
            clients[slug]['ads_detail'] = cell(row, 5) or '—'

    # Pontos de Atenção
    atencao_rows = read_sheet(service, MAPA_GERAL_ID, "'Pontos de Atenção'!A3:E500")
    atencao = []
    for row in atencao_rows:
        name = cell(row, 1)
        if not name:
            continue
        atencao.append({
            'client': name,
            'slug': slugify(name),
            'canal': cell(row, 2),
            'tipo': cell(row, 3),
            'desc': cell(row, 4),
            'prioridade': cell(row, 5),
        })

    return clients, atencao


# ═══════════════════════════════════════
# LEITURA: CLIENTES ESTRATÉGICOS
# ═══════════════════════════════════════
def read_estrategicos(service):
    print("📊 Lendo Clientes Estratégicos...")
    rows = read_sheet(service, ESTRATEGICOS_ID, "'Expansões'!A3:J200")
    expansoes = []
    for row in rows:
        name = cell(row, 1)
        if not name:
            continue
        expansoes.append({
            'client': name,
            'slug': slugify(name),
            'canal': cell(row, 2),
            'status': cell(row, 3),
            'am': cell(row, 4),
            'prioridade': cell(row, 5),
        })
    print(f"  ✓ {len(expansoes)} expansões")
    return expansoes


# ═══════════════════════════════════════
# LEITURA: VENDAS (Motor de Meta)
# ═══════════════════════════════════════
def read_vendas(service):
    """Lê Meta, Premissas, Analise, Ranking_AM, Sensibilidade.
    Nunca exibe valores R$ — só tendências qualitativas.

    Estrutura da aba Meta:
      A = Cliente+Canal (chave), B = Canal, D = AM
      F = Janeiro, G = Fevereiro, H = Março, I = Mês Atual (vendas)
      Y = Meta Vigente (col 25), Z = Desvio Meta-Atual (col 26)
    """
    print("📊 Lendo Motor de Meta (vendas)...")

    # --- Meta — tendência por cliente/canal ---
    # Lê até col Z (26 colunas) pra pegar Meta e Desvio
    rows = read_sheet(service, VENDAS_ID, "'Meta'!A4:Z2000")
    trends = {}
    for row in rows[1:]:
        key = cell(row, 1)       # A = chave (ex: "DUX Nutrition Amazon BR")
        canal = cell(row, 2)     # B = canal
        am = cell(row, 4)        # D = AM
        if not key:
            continue
        try:
            jan = float(cell(row, 6) or 0)   # F = Janeiro
            fev = float(cell(row, 7) or 0)   # G = Fevereiro
            mar = float(cell(row, 8) or 0)   # H = Março
            atual = float(cell(row, 9) or 0) # I = Mês Atual
        except:
            continue

        # Tendência: compara mês atual (I) com mês anterior (H=Março)
        # Se mês atual for 0 (ainda não tem dado), compara Mar vs Fev
        if atual > 0 and mar > 0:
            var_pct = round((atual - mar) / mar * 100, 1)
        elif mar > 0 and fev > 0:
            var_pct = round((mar - fev) / fev * 100, 1)
        elif fev > 0 and jan > 0:
            var_pct = round((fev - jan) / jan * 100, 1)
        else:
            var_pct = 0

        trend = 'subindo' if var_pct > 5 else ('caindo' if var_pct < -5 else 'estagnado')

        # Meta vs Atual (qualitativo)
        try:
            meta_vigente = float(cell(row, 25) or 0)  # Y = Meta Vigente
        except:
            meta_vigente = 0
        try:
            desvio = float(cell(row, 26) or 0)         # Z = Desvio
        except:
            desvio = 0

        # Atingimento qualitativo (sem mostrar R$)
        if meta_vigente > 0 and atual > 0:
            atingimento_pct = round(atual / meta_vigente * 100, 1)
            meta_status = 'acima' if atingimento_pct >= 100 else (
                'proximo' if atingimento_pct >= 80 else 'abaixo'
            )
        else:
            atingimento_pct = 0
            meta_status = 'sem_meta'

        client_name = key.replace(f' {canal}', '').strip()
        slug = slugify(client_name)

        trends[key] = {
            'client': client_name, 'slug': slug, 'canal': canal, 'am': am,
            'var_pct': var_pct, 'trend': trend,
            'atingimento_pct': atingimento_pct, 'meta_status': meta_status,
        }
    print(f"  ✓ {len(trends)} tendências")

    # --- Premissas ---
    rows = read_sheet(service, VENDAS_ID, "'Premissas_Editaveis'!A4:S2000")
    premissas = {}
    for row in rows[1:]:
        key = cell(row, 1)
        if not key:
            continue
        premissas[key] = {
            'maturidade': cell(row, 6), 'dep_bestseller': cell(row, 7),
            'dias_ruptura': cell(row, 8), 'buy_box': cell(row, 16),
            'problema_logistico': cell(row, 17), 'concorrencia': cell(row, 18),
            'promocao': cell(row, 11), 'preco': cell(row, 12),
        }
    print(f"  ✓ {len(premissas)} premissas")

    # --- Analise — top subindo e caindo ---
    rows = read_sheet(service, VENDAS_ID, "'Analise'!A4:N30")
    top_subindo = []
    top_caindo = []
    for row in rows[1:]:
        name_up = cell(row, 1)
        if name_up:
            try:
                var_val = round(float(cell(row, 4) or 0) * 100, 1)
            except:
                var_val = 0
            top_subindo.append({
                'key': name_up, 'canal': cell(row, 2), 'am': cell(row, 3),
                'var': var_val,
            })
        name_down = cell(row, 11)
        if name_down:
            try:
                var_val_d = round(float(cell(row, 14) or 0) * 100, 1)
            except:
                var_val_d = 0
            top_caindo.append({
                'key': name_down, 'canal': cell(row, 12), 'am': cell(row, 13),
                'var': var_val_d,
            })
    print(f"  ✓ {len(top_subindo)} subindo, {len(top_caindo)} caindo")

    # --- Ranking_AM ---
    # A aba Ranking_AM sorted começa na linha 10
    # Colunas: A=AM, B=Meta%, D=Crescimento, F=Carteira, H=Oportunidade, J=Risco, L=Score Final
    rows = read_sheet(service, VENDAS_ID, "'Ranking_AM'!A10:Q30")
    ranking_am = []
    rank_pos = 0
    for row in rows:
        am_name = cell(row, 1)
        if not am_name or am_name.lower() in ('am', 'account manager', ''):
            continue
        rank_pos += 1
        try:
            score_final = round(float(cell(row, 12) or 0), 1)
        except:
            score_final = 0

        # Rating qualitativo baseado no score
        if score_final >= 80:
            rating = '⭐ Excelente'
        elif score_final >= 65:
            rating = '✅ Bom'
        elif score_final >= 50:
            rating = '⚠️ Regular'
        else:
            rating = '🔴 Crítico'

        # Conta subindo/estagnado/caindo das contas desse AM
        am_sub = 0
        am_est = 0
        am_cai = 0
        for key, t in trends.items():
            if t.get('am') == am_name:
                if t['trend'] == 'subindo':
                    am_sub += 1
                elif t['trend'] == 'caindo':
                    am_cai += 1
                else:
                    am_est += 1

        ranking_am.append({
            'am': am_name,
            'rank': rank_pos,
            'score': score_final,
            'score_final': score_final,
            'rating': rating,
            'meta_pct': cell(row, 2),
            'cresc': cell(row, 4),
            'carteira': cell(row, 6),
            'oport': cell(row, 8),
            'risco': cell(row, 10),
            'subindo': am_sub,
            'estagnado': am_est,
            'caindo': am_cai,
        })
    ranking_am.sort(key=lambda x: x['score'], reverse=True)
    # Reatribui rank após sort
    for i, r in enumerate(ranking_am):
        r['rank'] = i + 1
    print(f"  ✓ {len(ranking_am)} AMs ranqueados")

    # --- Sensibilidade (nome com emoji pode variar) ---
    rows = read_sheet(service, VENDAS_ID, "'📊 Sensibilidade_Canal'!A5:T500")
    if not rows:
        rows = read_sheet(service, VENDAS_ID, "'Sensibilidade_Canal'!A5:T500")
    sensibilidade = {}
    for row in rows:
        key = cell(row, 1)
        if not key:
            continue
        semaforo = cell(row, 18) or cell(row, 17) or '—'
        sensibilidade[key] = {
            'semaforo': semaforo,
            'acao': cell(row, 19) or cell(row, 20) or '',
        }
    print(f"  ✓ {len(sensibilidade)} sensibilidades")

    # --- Visão por Canal (agregação) ---
    canal_view = {}
    for key, t in trends.items():
        canal = t['canal']
        if canal not in canal_view:
            canal_view[canal] = {
                'total_count': 0, 'subindo': 0, 'estagnado': 0, 'caindo': 0,
                'pct_subindo': 0, 'pct_caindo': 0, 'saude': 'estavel',
                'clients': [],
            }
        canal_view[canal]['total_count'] += 1
        canal_view[canal][t['trend']] += 1
        canal_view[canal]['clients'].append({
            'client': t['client'], 'slug': t['slug'],
            'var_pct': t['var_pct'], 'trend': t['trend'],
            'am': t['am'],
            'meta_status': t.get('meta_status', 'sem_meta'),
            'atingimento_pct': t.get('atingimento_pct', 0),
        })

    for canal in canal_view:
        canal_view[canal]['clients'].sort(key=lambda x: x['var_pct'], reverse=True)
        total = canal_view[canal]['total_count']
        sub = canal_view[canal]['subindo']
        cai = canal_view[canal]['caindo']
        if total > 0:
            canal_view[canal]['pct_subindo'] = round(sub / total * 100)
            canal_view[canal]['pct_caindo'] = round(cai / total * 100)
            if cai > sub * 2:
                canal_view[canal]['saude'] = 'critico'
            elif cai > sub:
                canal_view[canal]['saude'] = 'atencao'
            elif sub > cai:
                canal_view[canal]['saude'] = 'saudavel'
            else:
                canal_view[canal]['saude'] = 'estavel'
    print(f"  ✓ Visão de {len(canal_view)} canais")

    # --- Vendas por AM (cruzamento com meta) ---
    am_vendas = {}
    for key, t in trends.items():
        am = t.get('am', '—')
        if am == '—' or not am:
            continue
        if am not in am_vendas:
            am_vendas[am] = {
                'total': 0, 'subindo': 0, 'caindo': 0, 'estagnado': 0,
                'acima_meta': 0, 'abaixo_meta': 0, 'proximo_meta': 0,
                'top_subindo': [], 'top_caindo': [],
            }
        am_vendas[am]['total'] += 1
        am_vendas[am][t['trend']] += 1

        # Atingimento de meta
        ms = t.get('meta_status', 'sem_meta')
        if ms == 'acima':
            am_vendas[am]['acima_meta'] += 1
        elif ms == 'abaixo':
            am_vendas[am]['abaixo_meta'] += 1
        elif ms == 'proximo':
            am_vendas[am]['proximo_meta'] += 1

        if t['trend'] == 'subindo':
            am_vendas[am]['top_subindo'].append({
                'client': t['client'], 'canal': t['canal'],
                'var_pct': t['var_pct'],
                'meta_status': ms, 'atingimento_pct': t.get('atingimento_pct', 0),
            })
        elif t['trend'] == 'caindo':
            am_vendas[am]['top_caindo'].append({
                'client': t['client'], 'canal': t['canal'],
                'var_pct': t['var_pct'],
                'meta_status': ms, 'atingimento_pct': t.get('atingimento_pct', 0),
            })

    for am in am_vendas:
        am_vendas[am]['top_subindo'].sort(key=lambda x: x['var_pct'], reverse=True)
        am_vendas[am]['top_caindo'].sort(key=lambda x: x['var_pct'])
        am_vendas[am]['top_subindo'] = am_vendas[am]['top_subindo'][:5]
        am_vendas[am]['top_caindo'] = am_vendas[am]['top_caindo'][:5]
        total = am_vendas[am]['total']
        if total > 0:
            am_vendas[am]['pct_subindo'] = round(am_vendas[am]['subindo'] / total * 100)
            am_vendas[am]['pct_caindo'] = round(am_vendas[am]['caindo'] / total * 100)
    print(f"  ✓ Vendas cruzadas por {len(am_vendas)} AMs")

    return {
        'trends': trends,
        'premissas': premissas,
        'top_subindo': top_subindo,
        'top_caindo': top_caindo,
        'ranking_am': ranking_am,
        'sensibilidade': sensibilidade,
        'canal_view': canal_view,
        'am_vendas': am_vendas,
    }


# ═══════════════════════════════════════
# CLICKUP
# ═══════════════════════════════════════
def get_clickup_tasks_fast(valid_slugs):
    """Busca tarefas do ClickUp via endpoint team/tasks com paginação."""
    print("📋 Lendo ClickUp...")
    headers = {'Authorization': CLICKUP_TOKEN}
    valid_set = set(valid_slugs)
    tasks_by_client = {}
    tasks_by_team = {}
    page = 0
    total = 0
    now = datetime.datetime.now(tz=BRT)

    while True:
        url = f"https://api.clickup.com/api/v2/team/{CLICKUP_TEAM_ID}/task"
        params = {
            'page': page,
            'subtasks': 'true',
            'include_closed': 'false',
            'order_by': 'updated',
            'space_ids[]': CLICKUP_CLIENT_SPACE_ID,
        }
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=30)
            data = resp.json()
        except Exception as e:
            print(f"  ⚠️  ClickUp page {page}: {e}")
            break

        tasks = data.get('tasks', [])
        if not tasks:
            break

        for t in tasks:
            task_name = t.get('name', '')
            status_name = t.get('status', {}).get('status', '—')
            folder = t.get('folder', {}).get('name', '') or ''
            list_name = t.get('list', {}).get('name', '') or ''
            assignees = [a.get('username', '') for a in t.get('assignees', [])]
            due = t.get('due_date')
            created = t.get('date_created')
            updated = t.get('date_updated')
            priority = t.get('priority', {})
            priority_name = priority.get('priority', '—') if priority else '—'

            # Converte timestamps
            due_str = ''
            if due:
                try:
                    due_str = datetime.datetime.fromtimestamp(
                        int(due) / 1000, tz=BRT
                    ).strftime('%Y-%m-%d')
                except:
                    pass
            created_str = ''
            if created:
                try:
                    created_str = datetime.datetime.fromtimestamp(
                        int(created) / 1000, tz=BRT
                    ).strftime('%Y-%m-%d')
                except:
                    pass
            updated_str = ''
            if updated:
                try:
                    updated_str = datetime.datetime.fromtimestamp(
                        int(updated) / 1000, tz=BRT
                    ).strftime('%Y-%m-%d')
                except:
                    pass

            # URL do ClickUp
            task_url = t.get('url', '')

            task_obj = {
                'id': t.get('id', ''),
                'n': task_name,           # front usa t.n
                'st': status_name.lower(),  # front usa t.st (lowercase)
                'folder': folder,
                'list': list_name,
                'ow': assignees[0] if assignees else '',  # front usa t.ow
                'due': due_str,
                'created': created_str,
                'updated': updated_str,
                'priority': priority_name,
                'url': task_url,
                'team': folder.split()[0].lower() if folder else '',
            }

            # Calcula days_stalled pra tarefas paradas
            if updated_str:
                try:
                    upd_dt = datetime.datetime.strptime(updated_str, '%Y-%m-%d').replace(tzinfo=BRT)
                    task_obj['days_stalled'] = (now - upd_dt).days
                except:
                    task_obj['days_stalled'] = 0
            else:
                task_obj['days_stalled'] = 0

            # Identifica se é tarefa de time (Listing/Logistics/Marketing)
            folder_lower = folder.lower()
            is_team_folder = any(kw in folder_lower for kw in
                                ['listing', 'logistics', 'logística', 'marketing',
                                 'creative', 'criativo', 'design'])

            if is_team_folder:
                # Tenta extrair nome do cliente do título: [ClientName] Tarefa
                bracket_match = re.search(r'\[(.+?)\]', task_name)
                if bracket_match:
                    extracted = bracket_match.group(1)
                    slug = match_client_slug(extracted, valid_set)
                    if slug:
                        if slug not in tasks_by_client:
                            tasks_by_client[slug] = []
                        tasks_by_client[slug].append(task_obj)
                        total += 1
                        continue

                # Fallback: guarda em tasks_by_team
                team_key = folder_lower.split()[0] if folder_lower else 'other'
                if team_key not in tasks_by_team:
                    tasks_by_team[team_key] = []
                tasks_by_team[team_key].append(task_obj)
                total += 1
            else:
                # Tenta casar pasta com cliente
                slug = match_client_slug(folder, valid_set)
                if slug:
                    if slug not in tasks_by_client:
                        tasks_by_client[slug] = []
                    tasks_by_client[slug].append(task_obj)
                    total += 1

        page += 1
        if page > 50:
            break
        time.sleep(0.5)  # rate limit ClickUp

    # Recupera tarefas de pastas de time que têm [NomeCliente]
    rescued = 0
    for team_key in list(tasks_by_team.keys()):
        remaining = []
        for t in tasks_by_team[team_key]:
            bracket_match = re.search(r'\[(.+?)\]', t['name'])
            if bracket_match:
                extracted = bracket_match.group(1)
                slug = match_client_slug(extracted, valid_set)
                if slug:
                    if slug not in tasks_by_client:
                        tasks_by_client[slug] = []
                    tasks_by_client[slug].append(t)
                    rescued += 1
                    continue
            remaining.append(t)
        tasks_by_team[team_key] = remaining

    print(f"  ✓ {total} tarefas em {len(tasks_by_client)} clientes")
    if rescued:
        print(f"  ✓ {rescued} tarefas resgatadas das pastas de time")
    return tasks_by_client, tasks_by_team


# ═══════════════════════════════════════
# AM SUMMARY
# ═══════════════════════════════════════
def build_am_summary(clients, tasks_by_client, vendas):
    """Constrói resumo por AM com backlog, tarefas paradas, e dados de vendas."""
    now = datetime.datetime.now(tz=BRT)
    am_summary = {}

    for slug, c in clients.items():
        am = c['am']
        if am == '—':
            continue
        if am not in am_summary:
            am_summary[am] = {
                'total_clients': 0,
                'total_tasks': 0,
                'overdue': 0,
                'no_due': 0,
                'stale_30': 0,
                'urgent': 0,
                'clients': [],
            }
        am_summary[am]['total_clients'] += 1

        client_tasks = tasks_by_client.get(slug, [])
        client_overdue = 0
        client_stale = 0
        client_no_due = 0
        client_urgent = 0

        for t in client_tasks:
            am_summary[am]['total_tasks'] += 1

            # Overdue
            if t.get('due'):
                try:
                    due_dt = datetime.datetime.strptime(t['due'], '%Y-%m-%d').replace(tzinfo=BRT)
                    if due_dt < now:
                        client_overdue += 1
                except:
                    pass
            else:
                client_no_due += 1

            # Stale (30+ days sem update)
            if t.get('updated'):
                try:
                    upd_dt = datetime.datetime.strptime(t['updated'], '%Y-%m-%d').replace(tzinfo=BRT)
                    if (now - upd_dt).days >= 30:
                        client_stale += 1
                except:
                    pass

            # Urgent priority
            if t.get('priority') in ('urgent', 'Urgent', '1'):
                client_urgent += 1

        am_summary[am]['overdue'] += client_overdue
        am_summary[am]['no_due'] += client_no_due
        am_summary[am]['stale_30'] += client_stale
        am_summary[am]['urgent'] += client_urgent

        am_summary[am]['clients'].append({
            'slug': slug,
            'name': c['name'],
            'status': c['status'],
            'tasks': len(client_tasks),
            'overdue': client_overdue,
            'stale': client_stale,
            'score': c.get('score'),
        })

    # Adiciona dados de vendas por AM
    am_vendas = vendas.get('am_vendas', {})
    for am in am_summary:
        if am in am_vendas:
            am_summary[am]['vendas'] = am_vendas[am]
        else:
            am_summary[am]['vendas'] = None

    return am_summary


# ═══════════════════════════════════════
# MAIN
# ═══════════════════════════════════════
def main():
    print("=" * 60)
    print("  SELLERSFLOW PAINEL — SYNC")
    print(f"  {datetime.datetime.now(tz=BRT).strftime('%d/%m/%Y %H:%M BRT')}")
    print("=" * 60)

    # Google Sheets
    service = get_sheets_service()
    clients, atencao = read_mapa_geral(service)
    expansoes = read_estrategicos(service)

    # Vendas (com tratamento de erro global)
    try:
        vendas = read_vendas(service)
    except Exception as e:
        print(f"  ⚠️  Erro ao ler vendas: {e}")
        print("  ⚠️  Painel será gerado sem dados de vendas")
        vendas = {
            'trends': {}, 'premissas': {}, 'top_subindo': [], 'top_caindo': [],
            'ranking_am': [], 'sensibilidade': {}, 'canal_view': {},
            'am_vendas': {},
        }

    # ClickUp
    tasks_by_client, tasks_by_team = get_clickup_tasks_fast(clients.keys())

    # AM Summary (com vendas cruzadas)
    am_summary = build_am_summary(clients, tasks_by_client, vendas)

    # Monta tarefas nos clientes (com client_slug e client_name)
    for slug, c in clients.items():
        client_tasks = tasks_by_client.get(slug, [])
        for t in client_tasks:
            t['client_slug'] = slug
            t['client_name'] = c['name']
        c['tasks'] = client_tasks
        c['task_count'] = len(client_tasks)

    # Bundle
    bundle = {
        'updated_at': datetime.datetime.now(tz=BRT).isoformat(),
        'clients': clients,
        'tasks': {slug: tasks_by_client.get(slug, []) for slug in clients},
        'atencao': atencao,
        'expansoes': expansoes,
        'am_summary': am_summary,
        'vendas': vendas,
        'calendar': CALENDAR_EVENTS,
        'stats': {
            'total_clients': len(clients),
            'total_tasks': sum(len(t) for t in tasks_by_client.values()),
            'ams': list(am_summary.keys()),
        },
    }

    with open('data.json', 'w', encoding='utf-8') as f:
        json.dump(bundle, f, ensure_ascii=False, indent=2)

    print(f"\n✅ data.json gerado: {os.path.getsize('data.json') / 1024:.0f}KB")
    print(f"   {len(clients)} clientes | {bundle['stats']['total_tasks']} tarefas")
    print(f"   {len(vendas.get('trends', {}))} tendências de vendas")
    print(f"   {len(vendas.get('am_vendas', {}))} AMs com dados de vendas")


if __name__ == '__main__':
    main()
