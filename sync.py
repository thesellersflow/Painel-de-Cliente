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


def parse_money(s):
    """Parseia valores monetários em qualquer formato:
    BR: $58.062,45 ou 58.062,45 ou \\-$500,61
    US: 58062.45 ou -500.61
    """
    if not s:
        return 0.0
    s = str(s).strip()
    neg = False
    # Handle escaped negative: \\-$500,61 ou \-500
    if '\\-' in s:
        neg = True
        s = s.replace('\\-', '')
    elif s.startswith('-'):
        neg = True
        s = s[1:]
    s = s.replace('$', '').replace(' ', '').strip()
    if not s:
        return 0.0

    # Detectar formato: se tem vírgula, é BR (vírgula = decimal)
    has_comma = ',' in s
    has_dot = '.' in s

    if has_comma and has_dot:
        # Formato BR: 58.062,45 → ponto é milhar, vírgula é decimal
        s = s.replace('.', '').replace(',', '.')
    elif has_comma and not has_dot:
        # Só vírgula: 500,61 → vírgula é decimal
        s = s.replace(',', '.')
    elif has_dot and not has_comma:
        # Só ponto: pode ser milhar (58.062) ou decimal (500.61)
        # Se tem mais de um ponto, todos são milhares
        dot_count = s.count('.')
        if dot_count > 1:
            s = s.replace('.', '')
        else:
            # Um ponto: se depois do ponto tem exatamente 3 dígitos E
            # o número é > 999, é milhar. Senão é decimal.
            parts = s.split('.')
            if len(parts[1]) == 3 and len(parts[0]) >= 1:
                s = s.replace('.', '')  # milhar
            # else: decimal, manter como está

    try:
        v = float(s)
        return -v if neg else v
    except:
        return 0.0


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
def read_estrategicos(service, clients):
    """Extrai expansões a partir dos canais com status 'Expansão' no Mapa de Clientes."""
    print("📊 Extraindo expansões...")
    expansoes = []
    for slug, c in clients.items():
        for canal, info in c.get('canais', {}).items():
            st = info.get('st', '').lower()
            if 'expans' in st:
                expansoes.append({
                    'client': c['name'],
                    'slug': slug,
                    'canal': canal,
                    'status': 'Expansão',
                    'am': c['am'],
                    'prioridade': '—',
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
    rows = read_sheet(service, VENDAS_ID, "'Meta'!A4:AC2000")
    trends = {}
    for row in rows[1:]:
        key = cell(row, 1)       # A = chave (ex: "DUX Nutrition Amazon BR")
        canal = cell(row, 2)     # B = canal
        am = cell(row, 4)        # D = AM
        if not key:
            continue

        jan = parse_money(cell(row, 6))    # F = Janeiro
        fev = parse_money(cell(row, 7))    # G = Fevereiro
        mar = parse_money(cell(row, 8))    # H = Março
        atual = parse_money(cell(row, 9))  # I = Mês Atual (Abril)

        # Tendência: compara mês atual (I) com mês anterior (H=Março)
        if atual > 0 and mar > 0:
            var_pct = round((atual - mar) / mar * 100, 1)
        elif mar > 0 and fev > 0:
            var_pct = round((mar - fev) / fev * 100, 1)
        elif fev > 0 and jan > 0:
            var_pct = round((fev - jan) / jan * 100, 1)
        else:
            var_pct = 0

        trend = 'subindo' if var_pct > 5 else ('caindo' if var_pct < -5 else 'estagnado')

        # Meta Vigente e Desvio
        meta_vigente = parse_money(cell(row, 25))  # Y = Meta Vigente
        desvio = parse_money(cell(row, 26))         # Z = Desvio

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
    # A aba tem duas tabelas separadas. Vou ler a aba inteira e parsear
    rows = read_sheet(service, VENDAS_ID, "'Analise'!A1:D50")
    top_subindo = []
    top_caindo = []
    current_section = None
    for row in rows:
        first = cell(row, 1)
        if not first:
            continue
        if 'Caindo' in first and 'Top' in first:
            current_section = 'caindo'
            continue
        elif 'Subindo' in first and 'Top' in first:
            current_section = 'subindo'
            continue
        elif 'Estagnado' in first and 'Top' in first:
            current_section = 'estagnado'
            continue
        elif first in (':-:', 'Resumo', 'Status'):
            continue

        canal = cell(row, 2)
        am = cell(row, 3)
        var_raw = cell(row, 4)
        if not canal or not am:
            continue

        # Parse var: "\\-87,3%" ou "2228,8%"
        var_str = var_raw.replace('\\-', '-').replace('\\', '').replace('%', '').replace(',', '.').strip()
        try:
            var_val = round(float(var_str), 1)
        except:
            var_val = 0

        entry = {'key': first, 'canal': canal, 'am': am, 'var': var_val}
        if current_section == 'caindo':
            top_caindo.append(entry)
        elif current_section == 'subindo':
            top_subindo.append(entry)
    print(f"  ✓ {len(top_subindo)} subindo, {len(top_caindo)} caindo")

    # --- Ranking_AM ---
    # Colunas reais: A=Pos, B=AM, C=Clientes, D=Receita$, E=Meta$, F=Var%,
    # G=Sc.Meta, H=Sc.Cresc, I=Sc.Cart, J=Sc.Oport, K=Sc.Risco, L=Sc.Manual,
    # M=SCORE FINAL, N=Rating, O=Subindo, P=Estagnado, Q=Caindo, R=Sc.Sensib
    rows = read_sheet(service, VENDAS_ID, "'Ranking_AM'!A10:R30")
    ranking_am = []
    for row in rows:
        am_name = cell(row, 2)  # B = Account Manager
        if not am_name or am_name.lower() in ('am', 'account manager', ''):
            continue
        pos_str = cell(row, 1)
        try:
            pos = int(pos_str)
        except:
            continue

        score_final = parse_money(cell(row, 13))  # M = SCORE FINAL
        rating_raw = cell(row, 14)  # N = Rating (⭐⭐⭐⭐⭐)

        # Subindo/Estagnado/Caindo da planilha
        try:
            am_sub = int(cell(row, 15) or 0)  # O
        except:
            am_sub = 0
        try:
            am_est = int(cell(row, 16) or 0)  # P
        except:
            am_est = 0
        try:
            am_cai = int(cell(row, 17) or 0)  # Q
        except:
            am_cai = 0

        ranking_am.append({
            'am': am_name,
            'rank': pos,
            'score': round(score_final, 1),
            'score_final': round(score_final, 1),
            'rating': rating_raw or '—',
            'meta_pct': cell(row, 6),   # F = Var%
            'cresc': cell(row, 8),      # H = Sc.Cresc
            'carteira': cell(row, 9),   # I = Sc.Cart
            'oport': cell(row, 10),     # J = Sc.Oport
            'risco': cell(row, 11),     # K = Sc.Risco
            'subindo': am_sub,
            'estagnado': am_est,
            'caindo': am_cai,
        })
    ranking_am.sort(key=lambda x: x['score'], reverse=True)
    for i, r in enumerate(ranking_am):
        r['rank'] = i + 1
    print(f"  ✓ {len(ranking_am)} AMs ranqueados")

    # --- Sensibilidade ---
    # Headers reais: ...S(19)=Semáforo, T(20)=Ação. Mas o nome da aba tem emoji.
    rows = read_sheet(service, VENDAS_ID, "'📊 Sensibilidade_Canal'!A5:T500")
    if not rows:
        rows = read_sheet(service, VENDAS_ID, "'Sensibilidade_Canal'!A5:T500")
    sensibilidade = {}
    for row in rows:
        key = cell(row, 1)
        if not key or key.startswith(':-') or key == 'NO_HEADER':
            continue
        # Semáforo pode estar em diferentes colunas dependendo de merged cells
        semaforo = cell(row, 19) or cell(row, 18) or cell(row, 14) or '—'
        acao = cell(row, 20) or cell(row, 19) or cell(row, 15) or ''
        sensibilidade[key] = {
            'semaforo': semaforo,
            'acao': acao,
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
            start = t.get('start_date')
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
            start_str = ''
            if start:
                try:
                    start_str = datetime.datetime.fromtimestamp(
                        int(start) / 1000, tz=BRT
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
                'n': task_name,
                'st': status_name.lower(),
                'folder': folder,
                'list': list_name,
                'ow': assignees[0] if assignees else '',
                'due': due_str,
                'start': start_str,
                'created': created_str,
                'updated': updated_str,
                'pri': priority_name.lower() if priority_name else '',
                'url': task_url,
                'team': folder.split()[0].lower() if folder else '',
            }

            # Detecta se é promoção pelo nome da task
            name_lower = task_name.lower()
            promo_kw = ['cupom', 'desconto', 'sales price', 'ped ', '%off',
                        'off ', 'promoção', 'promocao', 'promo ', 'lightning',
                        'deal', 'voucher', 'frete grátis', 'frete gratis',
                        'cashback', 'relâmpago', 'progressivo']
            task_obj['is_promo'] = any(kw in name_lower for kw in promo_kw)

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
            bracket_match = re.search(r'\[(.+?)\]', t['n'])
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
def build_am_summary(clients, tasks_by_client, vendas, atencao):
    """Constrói resumo por AM com todos os campos que o front espera."""
    now = datetime.datetime.now(tz=BRT)
    am_summary = {}

    # Index pontos de atenção por slug
    atencao_by_slug = {}
    for a in atencao:
        s = a.get('slug', '')
        if s not in atencao_by_slug:
            atencao_by_slug[s] = []
        atencao_by_slug[s].append(a)

    for slug, c in clients.items():
        am = c['am']
        if am == '—' or not am:
            continue
        if am not in am_summary:
            am_summary[am] = {
                'am': am,
                'total_clients': 0,
                'active_clients': 0,
                'standby_clients': 0,
                'total_tasks': 0,
                'open_tasks_total': 0,
                'overdue': 0,
                'no_due': 0,
                'no_due_tasks': 0,
                'stale_30': 0,
                'stalled_tasks_count': 0,
                'stalled_tasks': [],
                'urgent': 0,
                'urgent_tasks': 0,
                'urgent_tasks_list': [],
                'case_tasks': 0,
                'case_tasks_list': [],
                'waiting_tasks': 0,
                'clients': [],
                'clients_list': [],
                'critical_clients': [],
                'atencao_count': 0,
            }
        s = am_summary[am]
        s['total_clients'] += 1

        # Status do cliente
        st_lower = c.get('status', '').lower()
        if 'stand' in st_lower or 'pausa' in st_lower or 'inativ' in st_lower:
            s['standby_clients'] += 1
        else:
            s['active_clients'] += 1

        # Pontos de atenção
        client_atencao = atencao_by_slug.get(slug, [])
        s['atencao_count'] += len(client_atencao)

        # Tarefas
        client_tasks = tasks_by_client.get(slug, [])
        client_overdue = 0
        client_stale = 0
        client_no_due = 0
        client_urgent = 0
        open_count = 0

        for t in client_tasks:
            s['total_tasks'] += 1
            t_st = (t.get('st') or '').lower()

            # Skip closed/done
            if t_st in ('feito', 'done', 'closed', 'complete', 'completo'):
                continue

            open_count += 1

            # Cases (lista especial)
            t_list = (t.get('list') or '').lower()
            if 'case' in t_list or 'case' in (t.get('folder') or '').lower():
                s['case_tasks'] += 1
                s['case_tasks_list'].append(t)

            # Waiting
            if 'espera' in t_st or 'wait' in t_st or 'blocked' in t_st:
                s['waiting_tasks'] += 1

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
            days_stalled = t.get('days_stalled', 0)
            if days_stalled >= 30:
                client_stale += 1
                s['stalled_tasks'].append(t)

            # Urgent priority
            if t.get('pri') in ('urgent', '1'):
                client_urgent += 1
                s['urgent_tasks_list'].append(t)

        s['open_tasks_total'] += open_count
        s['overdue'] += client_overdue
        s['no_due'] += client_no_due
        s['no_due_tasks'] += client_no_due
        s['stale_30'] += client_stale
        s['stalled_tasks_count'] += client_stale
        s['urgent'] += client_urgent
        s['urgent_tasks'] += client_urgent

        client_entry = {
            'slug': slug,
            'name': c['name'],
            'status': c['status'],
            'tasks': len(client_tasks),
            'open': open_count,
            'overdue': client_overdue,
            'stale': client_stale,
            'score': c.get('score'),
        }
        s['clients'].append(client_entry)
        s['clients_list'].append(client_entry)

        # Critical: score <= 30 ou muitas atrasadas
        if (c.get('score') is not None and c['score'] <= 30) or client_overdue > 3:
            s['critical_clients'].append(client_entry)

    # Limitar listas de tarefas (pra não explodir o JSON)
    for am in am_summary:
        s = am_summary[am]
        s['stalled_tasks'] = sorted(s['stalled_tasks'],
                                     key=lambda t: t.get('days_stalled', 0), reverse=True)[:20]
        s['urgent_tasks_list'] = s['urgent_tasks_list'][:10]
        s['case_tasks_list'] = s['case_tasks_list'][:10]

    # Calcula avg_score por AM
    for am in am_summary:
        s = am_summary[am]
        scores = [c['score'] for c in s['clients'] if c.get('score') is not None]
        s['avg_score'] = round(sum(scores) / len(scores), 1) if scores else None

    # Adiciona dados de vendas por AM
    am_vendas = vendas.get('am_vendas', {})
    ranking_am = vendas.get('ranking_am', [])
    ranking_map = {r['am']: r for r in ranking_am}

    for am in am_summary:
        s = am_summary[am]

        # Vendas cruzadas
        s['vendas'] = am_vendas.get(am, None)

        # ═══════════════════════════════════════
        # HEALTH SCORE (0-100)
        # ═══════════════════════════════════════
        # Camada 1 — Performance Comercial (50%)
        rank = ranking_map.get(am)
        if rank and rank.get('score_final', 0) > 0:
            perf = min(rank['score_final'], 100)
        else:
            perf = 50  # neutro

        # Camada 2 — Execução Operacional (30%)
        if s['open_tasks_total'] > 0:
            penalty = (s['overdue'] * 4) + (s['stalled_tasks_count'] * 6) + (s['no_due'] * 1)
            exec_score = max(0, 100 - penalty)
        else:
            exec_score = 50

        # Camada 3 — Saúde da Carteira (20%)
        if s['avg_score'] is not None:
            score_comp = min(s['avg_score'], 100)
        else:
            score_comp = 50

        total_c = s['total_clients']
        if total_c > 0:
            criticos = len(s['critical_clients'])
            saude = score_comp * (1 - (criticos / total_c) * 0.5)
        else:
            saude = 50

        health = round((perf * 0.5) + (exec_score * 0.3) + (saude * 0.2))
        health = max(0, min(100, health))
        s['health'] = health

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
    expansoes = read_estrategicos(service, clients)

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
    am_summary = build_am_summary(clients, tasks_by_client, vendas, atencao)

    # Monta tarefas nos clientes (com client_slug e client_name)
    for slug, c in clients.items():
        client_tasks = tasks_by_client.get(slug, [])
        for t in client_tasks:
            t['client_slug'] = slug
            t['client_name'] = c['name']
        c['tasks'] = client_tasks
        c['task_count'] = len(client_tasks)

    # ═══════════════════════════════════════
    # TRANSFORM DATA PRO FRONT
    # ═══════════════════════════════════════
    now_dt = datetime.datetime.now(tz=BRT)

    # atencao: front espera {slug: [items]}
    atencao_by_slug = {}
    for a in atencao:
        s = a.get('slug', '')
        if s not in atencao_by_slug:
            atencao_by_slug[s] = []
        atencao_by_slug[s].append(a)

    # expansoes: front espera {slug: [items]}
    expansoes_by_slug = {}
    for e in expansoes:
        s = e.get('slug', '')
        if s not in expansoes_by_slug:
            expansoes_by_slug[s] = []
        expansoes_by_slug[s].append(e)

    # calendar: front espera [{date: '2026-05-15', title: '...', ...}]
    # Gera próximos 90 dias de eventos recorrentes + datas do Motor de Meta
    calendar_events = []
    for ev in CALENDAR_EVENTS:
        for offset in range(90):
            day = now_dt + datetime.timedelta(days=offset)
            if day.weekday() == ev.get('dow', -1):
                calendar_events.append({
                    'date': day.strftime('%Y-%m-%d'),
                    'title': ev['title'],
                    'time': ev.get('time', ''),
                    'type': 'recorrente',
                })

    # Datas promocionais fixas (exemplo)
    promo_dates = [
        {'date': '2026-06-12', 'title': 'Dia dos Namorados', 'type': 'promo'},
        {'date': '2026-06-28', 'title': 'Aniversário ML', 'type': 'promo'},
        {'date': '2026-08-10', 'title': 'Dia dos Pais', 'type': 'promo'},
        {'date': '2026-09-01', 'title': 'Prime Day BR', 'type': 'promo'},
        {'date': '2026-11-27', 'title': 'Black Friday', 'type': 'promo'},
        {'date': '2026-12-25', 'title': 'Natal', 'type': 'promo'},
        {'date': '2026-07-15', 'title': 'Prime Day US', 'type': 'promo'},
        {'date': '2026-10-08', 'title': 'Prime Big Deal Days US', 'type': 'promo'},
    ]
    calendar_events.extend(promo_dates)
    calendar_events.sort(key=lambda x: x['date'])

    # penetration: canal penetration por categoria
    penetration = {}
    for slug, c in clients.items():
        cat = c.get('cat', '—')
        if cat == '—':
            continue
        if cat not in penetration:
            penetration[cat] = {'total': 0, 'canais': {}}
        penetration[cat]['total'] += 1
        for canal in c.get('canais', {}):
            if canal not in penetration[cat]['canais']:
                penetration[cat]['canais'][canal] = 0
            penetration[cat]['canais'][canal] += 1

    # tasks_by_team: dict de listas
    teams_flat = {}
    for team_key, task_list in tasks_by_team.items():
        teams_flat[team_key] = task_list[:50]

    # ═══════════════════════════════════════
    # PROMOÇÕES (extraídas das tasks do ClickUp)
    # ═══════════════════════════════════════
    today_str = now_dt.strftime('%Y-%m-%d')
    promos_all = []      # lista global
    promos_by_slug = {}  # por cliente
    promos_by_am = {}    # por AM

    for slug, task_list in tasks_by_client.items():
        c = clients.get(slug, {})
        am = c.get('am', '—')
        for t in task_list:
            if not t.get('is_promo'):
                continue
            # Só promos com datas (start ou due)
            start = t.get('start', '')
            due = t.get('due', '')
            if not start and not due:
                continue

            # Parseia nome: "Tipo - Canal - Cliente" ou "Canal - Tipo <> Cliente"
            name = t.get('n', '')
            parts = [p.strip() for p in name.replace('<>', '-').split('-')]

            # Tenta detectar canal e tipo
            canal_keywords = {'meli': 'Mercado Livre', 'mercado livre': 'Mercado Livre',
                             'amazon': 'Amazon', 'amz': 'Amazon', 'shopee': 'Shopee',
                             'magalu': 'Magalu', 'shein': 'Shein', 'tiktok': 'TikTok',
                             'walmart': 'Walmart'}
            canal_detected = ''
            tipo_parts = []
            for p in parts:
                p_lower = p.lower().strip()
                matched = False
                for kw, canal_name in canal_keywords.items():
                    if kw in p_lower:
                        # Captura sufixo: "Amazon US", "Amz BR"
                        canal_detected = p.strip()
                        matched = True
                        break
                if not matched and p.strip():
                    # Verifica se é o nome do cliente (ignora)
                    if slugify(p.strip()) != slug:
                        tipo_parts.append(p.strip())

            tipo = ' - '.join(tipo_parts) if tipo_parts else name

            # Status temporal
            if due and due < today_str:
                status_promo = 'encerrada'
            elif start and start <= today_str and (not due or due >= today_str):
                status_promo = 'ativa'
            elif start and start > today_str:
                status_promo = 'planejada'
            else:
                status_promo = 'planejada'

            promo_obj = {
                'id': t.get('id', ''),
                'name': name,
                'tipo': tipo,
                'canal': canal_detected,
                'client': c.get('name', ''),
                'client_slug': slug,
                'am': am,
                'start': start,
                'due': due,
                'status': status_promo,
                'st': t.get('st', ''),
                'url': t.get('url', ''),
            }

            promos_all.append(promo_obj)

            if slug not in promos_by_slug:
                promos_by_slug[slug] = []
            promos_by_slug[slug].append(promo_obj)

            if am and am != '—':
                if am not in promos_by_am:
                    promos_by_am[am] = []
                promos_by_am[am].append(promo_obj)

    promos_all.sort(key=lambda x: x.get('start') or x.get('due') or '')
    for am in promos_by_am:
        promos_by_am[am].sort(key=lambda x: x.get('start') or x.get('due') or '')
    print(f"  ✓ {len(promos_all)} promoções extraídas em {len(promos_by_slug)} clientes")

    # Adiciona promos ao am_summary
    for am in am_summary:
        am_summary[am]['promos'] = promos_by_am.get(am, [])
        am_summary[am]['promos_count'] = len(promos_by_am.get(am, []))
        am_summary[am]['promos_ativas'] = len([p for p in promos_by_am.get(am, []) if p['status'] == 'ativa'])

    # ═══════════════════════════════════════
    # BUNDLE FINAL
    # ═══════════════════════════════════════
    bundle = {
        'generated_at': now_dt.isoformat(),
        'updated_at': now_dt.isoformat(),
        'clients': clients,
        'tasks': {slug: tasks_by_client.get(slug, []) for slug in clients},
        'tasks_by_team': teams_flat,
        'atencao': atencao_by_slug,
        'expansoes': expansoes_by_slug,
        'am_summary': am_summary,
        'vendas': vendas,
        'calendar': calendar_events,
        'penetration': penetration,
        'promos': promos_all,
        'promos_by_client': promos_by_slug,
        'stats': {
            'total_clients': len(clients),
            'total_tasks': sum(len(t) for t in tasks_by_client.values()),
            'total_promos': len(promos_all),
            'promos_ativas': len([p for p in promos_all if p['status'] == 'ativa']),
            'ams': list(am_summary.keys()),
        },
    }

    with open('data.json', 'w', encoding='utf-8') as f:
        json.dump(bundle, f, ensure_ascii=False, indent=2)

    print(f"\n✅ data.json gerado: {os.path.getsize('data.json') / 1024:.0f}KB")
    print(f"   {len(clients)} clientes | {bundle['stats']['total_tasks']} tarefas")
    print(f"   {len(promos_all)} promoções ({len([p for p in promos_all if p['status']=='ativa'])} ativas)")
    print(f"   {len(vendas.get('trends', {}))} tendências de vendas")
    print(f"   {len(vendas.get('am_vendas', {}))} AMs com dados de vendas")


if __name__ == '__main__':
    main()
