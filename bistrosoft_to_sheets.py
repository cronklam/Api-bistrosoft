"""
bistrosoft_to_sheets.py  — Cronklam / L'Harmonie
==============================================================
Arquitectura:
    1. Consulta Bistrosoft API para UN SOLO DÍA (ayer por defecto).
       IMPORTANTE: el API de Bistrosoft devuelve todos los registros con la
       fecha del rango más reciente si se consulta un rango multi-día.
       Por eso siempre usamos startDate = endDate = un día específico.
    2. Calcula el resumen diario en Python.
    3. Pestaña "Resumen"        → histórico acumulativo (merge inteligente).
    4. Pestaña "Transacciones"  → detalle del día descargado.
    5. Pestaña "Promedio x Día" → venta promedio por artículo/local/día semana.

Sin Supabase. Sin VIEW. Sin dedup complejo.
Basado en el script original de Pomodoro Consulting (Tomás).
"""

import requests
import gspread
from datetime import datetime, timedelta
from collections import defaultdict, Counter
import time
import sys

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================

BISTRO_BASE_URL = "https://ar-api.bistrosoft.com/api/v1"
BISTRO_USERNAME = "pomodoroconsulting@gmail.com"
BISTRO_PASSWORD = "7027"

GOOGLE_CREDENTIALS_FILE = "service_account.json"
GOOGLE_SHEET_ID = "1s6kPguwD25k3xpmbUoHq1KNFd_SEva3z7pvTGhA4bsE"

GOOGLE_SHEET_TAB_TRANS = "Transacciones"
GOOGLE_SHEET_TAB_RESUMEN = "Resumen"
GOOGLE_SHEET_TAB_PROMEDIO = "Vta Promedio x Día"

# Día a consultar en modo normal.  None = ayer automáticamente.
FECHA_ESPECIFICA = None

# ── MODO BACKFILL ──────────────────────────────────────────────────────────────
MODO_BACKFILL = False
BACKFILL_DESDE = "2026-03-01"
# ──────────────────────────────────────────────────────────────────────────────

# ── COLORES PARA FORMATO ─────────────────────────────────────────────────────
COLOR_HEADER_BG    = {"red": 0.15, "green": 0.15, "blue": 0.15}  # Gris oscuro
COLOR_HEADER_FG    = {"red": 1.0,  "green": 1.0,  "blue": 1.0}   # Blanco
COLOR_ROW_EVEN     = {"red": 0.95, "green": 0.95, "blue": 0.97}  # Gris lavanda
COLOR_ROW_ODD      = {"red": 1.0,  "green": 1.0,  "blue": 1.0}   # Blanco
COLOR_ACCENT       = {"red": 0.20, "green": 0.66, "blue": 0.33}  # Verde oscuro
COLOR_ACCENT_FG    = {"red": 1.0,  "green": 1.0,  "blue": 1.0}   # Blanco
COLOR_LIGHT_GREEN  = {"red": 0.85, "green": 0.94, "blue": 0.85}  # Verde claro
COLOR_LIGHT_BLUE   = {"red": 0.85, "green": 0.91, "blue": 0.97}  # Azul claro
COLOR_LIGHT_YELLOW = {"red": 1.0,  "green": 0.97, "blue": 0.85}  # Amarillo claro
# ─────────────────────────────────────────────────────────────────────────────

# ==============================================================================
# FIN CONFIGURACIÓN
# ==============================================================================


def now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ── Bistrosoft ──────────────────────────────────────────────────────────────────

def obtener_token():
    resp = requests.post(
        f"{BISTRO_BASE_URL}/Token",
        json={"username": BISTRO_USERNAME, "password": BISTRO_PASSWORD},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    token = data.get("token") or data.get("access_token") or data.get("Token")
    if not token:
        raise Exception(f"No se encontró token en respuesta: {data}")
    print(f"[{now()}] ✅ Token Bistrosoft obtenido")
    return token


def descargar_transacciones(token, fecha_desde, fecha_hasta):
    """Descarga TODAS las páginas del TransactionDetailReport para el rango dado."""
    url = f"{BISTRO_BASE_URL}/TransactionDetailReport"
    headers = {"Authorization": f"Bearer {token}"}
    todas, page = [], 0
    print(f"[{now()}] 📥 Descargando Bistrosoft: {fecha_desde} → {fecha_hasta}...")
    while True:
        params = {"startDate": fecha_desde, "endDate": fecha_hasta, "pageNumber": page}
        resp = requests.get(url, headers=headers, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items", [])
        total_pages = data.get("totalPages", 1)
        todas.extend(items)
        print(f"[{now()}]   Pág {page + 1}/{total_pages} — {len(items)} registros")
        if page + 1 >= total_pages or not items:
            break
        page += 1
        time.sleep(5)
    print(f"[{now()}] ✅ {len(todas)} transacciones descargadas")
    return todas


# ── Cálculo del resumen ──────────────────────────────────────────────────────
_TIPOS_VENTA = {"Venta", "Comanda"}
_STATUS_VALIDO = {"CLOSE"}


def calcular_resumen(transacciones):
    """Calcula ventas brutas agrupadas por (date, shop) con dedup."""
    ticket_map = {}
    for t in transacciones:
        tipo = (t.get("transactionType") or "").strip()
        status = (t.get("status") or "").strip()
        amount = float(t.get("amount") or 0)
        fecha = (t.get("date") or "").strip()
        shop = (t.get("shop") or "").strip()
        if tipo not in _TIPOS_VENTA or status not in _STATUS_VALIDO:
            continue
        if amount <= 0 or not fecha or not shop:
            continue
        ticket = str(t.get("ticketNumber") or t.get("ticket_number") or "")
        key = (fecha, shop, ticket, tipo)
        if key not in ticket_map or amount > float(ticket_map[key].get("amount") or 0):
            ticket_map[key] = t

    total_antes = sum(
        1 for t in transacciones
        if (t.get("transactionType") or "").strip() in _TIPOS_VENTA
        and (t.get("status") or "").strip() in _STATUS_VALIDO
        and float(t.get("amount") or 0) > 0
    )
    print(f"[{now()}] 🔄 Dedup tickets: {total_antes} registros → {len(ticket_map)} únicos")

    agg = defaultdict(lambda: {
        "ventas_n": 0, "ventas_m": 0.0,
        "comandas_n": 0, "comandas_m": 0.0,
        "efectivo": 0.0, "tarjeta": 0.0,
    })
    for t in ticket_map.values():
        tipo = (t.get("transactionType") or "").strip()
        amount = float(t.get("amount") or 0)
        fecha = (t.get("date") or "").strip()
        shop = (t.get("shop") or "").strip()
        payment = (t.get("paymentMethod") or "").lower()
        key = (fecha, shop)
        if tipo == "Venta":
            agg[key]["ventas_n"] += 1
            agg[key]["ventas_m"] += amount
        else:
            agg[key]["comandas_n"] += 1
            agg[key]["comandas_m"] += amount
        if "efectivo" in payment or "cash" in payment:
            agg[key]["efectivo"] += amount
        else:
            agg[key]["tarjeta"] += amount

    result = []
    for (fecha, shop), v in agg.items():
        total = round(v["ventas_m"] + v["comandas_m"], 2)
        result.append({
            "date": fecha, "shop": shop,
            "ventas": v["ventas_n"], "comandas": v["comandas_n"],
            "total_transacciones": v["ventas_n"] + v["comandas_n"],
            "ventas_brutas": total,
            "efectivo": round(v["efectivo"], 2),
            "tarjeta_otros": round(v["tarjeta"], 2),
        })
    result.sort(key=_sort_key)
    return result


def _sort_key(r):
    try:
        d = datetime.strptime(str(r.get("date", "")), "%d-%m-%Y")
    except ValueError:
        d = datetime.min
    return (-d.toordinal(), str(r.get("shop", "")))


# ── Cálculo de promedios por día de semana ───────────────────────────────────

DIAS_SEMANA_ES = {
    0: "Lunes", 1: "Martes", 2: "Miércoles",
    3: "Jueves", 4: "Viernes", 5: "Sábado", 6: "Domingo",
}
DIAS_SEMANA_ORDER = [0, 1, 2, 3, 4, 5, 6]


def _parse_fecha(fecha_str):
    """Intenta parsear fecha en varios formatos. Retorna datetime o None."""
    for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(fecha_str.strip(), fmt)
        except ValueError:
            continue
    return None


def calcular_vta_promedio_por_dia(resumen_ws):
    """
    A partir del historial de Resumen (ventas_brutas por fecha/local),
    calcula la venta promedio $ por día de semana por local.
    Retorna: {shop: {dia_num: {"promedio": X, "num_dias": N, "fechas": [...]}}}
    """
    # {(shop, dia_num): {fecha_key: monto}}
    ventas = defaultdict(lambda: defaultdict(float))

    for r in resumen_ws:
        fecha_str = str(r.get("date", "") or r.get("Fecha", ""))
        shop = str(r.get("shop", "") or r.get("Local", ""))
        monto = r.get("ventas_brutas", 0) or r.get("Ventas Brutas $", 0)
        try:
            monto = float(str(monto).replace("$", "").replace(",", "").strip() or 0)
        except (ValueError, TypeError):
            monto = 0

        if not fecha_str or not shop or monto <= 0:
            continue

        fecha_dt = _parse_fecha(fecha_str)
        if not fecha_dt:
            continue

        dia_num = fecha_dt.weekday()
        fecha_key = fecha_dt.strftime("%Y-%m-%d")
        ventas[(shop, dia_num)][fecha_key] += monto

    # Agrupar por shop
    resultado = defaultdict(dict)
    for (shop, dia_num), fechas_dict in ventas.items():
        total = sum(fechas_dict.values())
        num_dias = len(fechas_dict)
        resultado[shop][dia_num] = {
            "promedio": round(total / num_dias, 2) if num_dias > 0 else 0,
            "num_dias": num_dias,
        }

    return resultado


def calcular_vta_promedio_por_articulo(transacciones_ws):
    """
    Calcula venta promedio de qty por artículo por local por día de semana.
    Retorna lista de dicts ordenada por local → producto.
    """
    # {(shop, product): {fecha_key: qty}}
    ventas = defaultdict(lambda: defaultdict(float))

    for t in transacciones_ws:
        tipo = str(t.get("transaction_type") or t.get("transactionType") or t.get("Tipo") or "").strip()
        product = str(t.get("product") or t.get("Producto") or "").strip()
        shop = str(t.get("shop") or t.get("Local") or "").strip()
        fecha_str = str(t.get("date") or t.get("Fecha") or "").strip()
        qty = 0
        try:
            qty = float(t.get("quantity") or t.get("Cantidad") or t.get("qty") or 0)
        except (ValueError, TypeError):
            qty = 0

        if "ITEM" not in tipo.upper():
            continue
        if not product or product == "-" or not shop or not fecha_str:
            continue
        if qty <= 0:
            continue

        fecha_dt = _parse_fecha(fecha_str)
        if not fecha_dt:
            continue

        fecha_key = fecha_dt.strftime("%Y-%m-%d")
        ventas[(shop, product)][fecha_key] += qty

    resultado = []
    for (shop, product), fechas_dict in ventas.items():
        total_qty = sum(fechas_dict.values())
        num_dias = len(fechas_dict)
        promedio = round(total_qty / num_dias, 1) if num_dias > 0 else 0
        resultado.append({
            "local": shop,
            "producto": product,
            "promedio_qty": promedio,
            "total_vendido": round(total_qty, 1),
            "dias_con_venta": num_dias,
        })

    resultado.sort(key=lambda r: (r["local"], -r["promedio_qty"]))
    return resultado


# ── Google Sheets ───────────────────────────────────────────────────────────────

COLS_RESUMEN = [
    "date", "shop", "total_transacciones", "ventas_brutas",
    "efectivo", "tarjeta_otros", "ventas", "comandas",
]

COLS_RESUMEN_DISPLAY = [
    "Fecha", "Local", "Total Transacciones", "Ventas Brutas $",
    "Efectivo $", "Tarjeta/Otros $", "Ventas", "Comandas",
]

COLS_TRANS = [
    "date", "hour", "shop", "transaction_type", "status",
    "ticket_number", "amount", "payment_method", "product",
    "quantity", "client", "waiter", "table_name", "category",
]

COLS_TRANS_DISPLAY = [
    "Fecha", "Hora", "Local", "Tipo", "Estado",
    "Ticket #", "Monto $", "Medio Pago", "Producto",
    "Cantidad", "Cliente", "Mozo", "Mesa", "Categoría",
]

COLS_PROMEDIO_ART = [
    "Producto", "Local", "Prom. Qty/Día",
    "Total Vendido", "Días c/Venta",
]


def _get_or_create_ws(sh, nombre, rows=5000, cols=20):
    try:
        return sh.worksheet(nombre)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=nombre, rows=str(rows), cols=str(cols))


def _format_header(ws, num_cols, color_bg=None, color_fg=None):
    """Aplica formato al header (fila 1) de cualquier pestaña."""
    if color_bg is None:
        color_bg = COLOR_HEADER_BG
    if color_fg is None:
        color_fg = COLOR_HEADER_FG

    col_letter = chr(ord('A') + num_cols - 1)
    ws.format(f"A1:{col_letter}1", {
        "backgroundColor": color_bg,
        "textFormat": {"bold": True, "foregroundColor": color_fg, "fontSize": 11},
        "horizontalAlignment": "CENTER",
        "borders": {
            "bottom": {"style": "SOLID", "width": 2,
                       "color": {"red": 0.3, "green": 0.3, "blue": 0.3}},
        },
    })


def _format_alternating_rows(ws, num_rows, num_cols):
    """Aplica colores alternados a las filas de datos."""
    if num_rows <= 1:
        return
    col_letter = chr(ord('A') + num_cols - 1)
    # Filas pares (2, 4, 6...)
    for i in range(2, num_rows + 1, 2):
        ws.format(f"A{i}:{col_letter}{i}", {
            "backgroundColor": COLOR_ROW_EVEN,
        })


def _format_number_cols(ws, col_indices, num_rows):
    """Formatea columnas numéricas con separador de miles."""
    for col_idx in col_indices:
        col_letter = chr(ord('A') + col_idx)
        if num_rows > 1:
            ws.format(f"{col_letter}2:{col_letter}{num_rows}", {
                "numberFormat": {"type": "NUMBER", "pattern": "#,##0"},
                "horizontalAlignment": "RIGHT",
            })


def _format_currency_cols(ws, col_indices, num_rows):
    """Formatea columnas de moneda."""
    for col_idx in col_indices:
        col_letter = chr(ord('A') + col_idx)
        if num_rows > 1:
            ws.format(f"{col_letter}2:{col_letter}{num_rows}", {
                "numberFormat": {"type": "NUMBER", "pattern": "$ #,##0.00"},
                "horizontalAlignment": "RIGHT",
            })


def _freeze_and_autofilter(ws, num_cols):
    """Congela la primera fila y agrega filtro."""
    ws.freeze(rows=1)
    col_letter = chr(ord('A') + num_cols - 1)
    ws.set_basic_filter(f"A1:{col_letter}")


def actualizar_resumen_en_sheets(nuevo_resumen, sh):
    """Merge inteligente con formato visual."""
    ws = _get_or_create_ws(sh, GOOGLE_SHEET_TAB_RESUMEN, rows=2000, cols=len(COLS_RESUMEN) + 2)

    existing_raw = ws.get_all_records()
    fechas_nuevas = {r["date"] for r in nuevo_resumen}
    # Map existing records using display or internal column names
    historico_sin_overlap = []
    for r in existing_raw:
        fecha = str(r.get("date", "") or r.get("Fecha", ""))
        if fecha not in fechas_nuevas:
            # Normalize keys
            historico_sin_overlap.append({
                "date": fecha,
                "shop": str(r.get("shop", "") or r.get("Local", "")),
                "total_transacciones": r.get("total_transacciones", 0) or r.get("Total Transacciones", 0),
                "ventas_brutas": r.get("ventas_brutas", 0) or r.get("Ventas Brutas $", 0),
                "efectivo": r.get("efectivo", 0) or r.get("Efectivo $", 0),
                "tarjeta_otros": r.get("tarjeta_otros", 0) or r.get("Tarjeta/Otros $", 0),
                "ventas": r.get("ventas", 0) or r.get("Ventas", 0),
                "comandas": r.get("comandas", 0) or r.get("Comandas", 0),
            })

    todos = historico_sin_overlap + nuevo_resumen
    todos.sort(key=_sort_key)

    filas = [COLS_RESUMEN_DISPLAY]
    for r in todos:
        filas.append([
            str(r.get("date", "")),
            str(r.get("shop", "")),
            r.get("total_transacciones", 0),
            r.get("ventas_brutas", 0),
            r.get("efectivo", 0),
            r.get("tarjeta_otros", 0),
            r.get("ventas", 0),
            r.get("comandas", 0),
        ])

    ws.clear()
    time.sleep(1)
    ws.resize(rows=len(filas) + 5)
    ws.update(filas, "A1")

    # Formato visual
    time.sleep(1)
    num_cols = len(COLS_RESUMEN)
    _format_header(ws, num_cols)
    _format_alternating_rows(ws, len(filas), num_cols)
    _format_currency_cols(ws, [3, 4, 5], len(filas))  # ventas_brutas, efectivo, tarjeta
    _format_number_cols(ws, [2, 6, 7], len(filas))     # total_trans, ventas, comandas
    _freeze_and_autofilter(ws, num_cols)

    # Timestamp
    ws.update_cell(len(filas) + 2, 1, f"Última actualización: {now()}")
    ws.format(f"A{len(filas) + 2}:H{len(filas) + 2}", {
        "textFormat": {"italic": True, "foregroundColor": {"red": 0.5, "green": 0.5, "blue": 0.5}},
    })

    print(f"[{now()}] ✅ Resumen actualizado — {len(todos)} filas históricas "
          f"({len(nuevo_resumen)} fechas nuevas/actualizadas)")


def actualizar_transacciones_en_sheets(transacciones, sh):
    """Escribe el detalle de transacciones con formato visual."""
    ws = _get_or_create_ws(sh, GOOGLE_SHEET_TAB_TRANS, rows=max(len(transacciones) + 10, 5000))

    filas = [COLS_TRANS_DISPLAY]
    for t in transacciones:
        filas.append([
            str(t.get("date") or ""),
            str(t.get("hour") or ""),
            str(t.get("shop") or ""),
            str(t.get("transactionType") or ""),
            str(t.get("status") or ""),
            str(t.get("ticketNumber") or ""),
            float(t.get("amount") or 0),
            str(t.get("paymentMethod") or ""),
            str(t.get("product") or ""),
            str(t.get("quantity") or ""),
            str(t.get("client") or ""),
            str(t.get("waiter") or ""),
            str(t.get("tableName") or ""),
            str(t.get("category") or ""),
        ])

    ws.clear()
    time.sleep(1)
    ws.resize(rows=len(filas) + 5)
    CHUNK = 5000
    for i in range(0, len(filas), CHUNK):
        ws.update(filas[i:i + CHUNK], f"A{i + 1}")
        if i + CHUNK < len(filas):
            time.sleep(2)

    # Formato visual
    time.sleep(1)
    num_cols = len(COLS_TRANS)
    _format_header(ws, num_cols)
    if len(filas) <= 1000:
        _format_alternating_rows(ws, len(filas), num_cols)
    _format_currency_cols(ws, [6], len(filas))  # amount
    _freeze_and_autofilter(ws, num_cols)

    # Timestamp
    ws.update_cell(len(filas) + 2, 1, f"Última actualización: {now()}")
    ws.format(f"A{len(filas) + 2}", {
        "textFormat": {"italic": True, "foregroundColor": {"red": 0.5, "green": 0.5, "blue": 0.5}},
    })

    print(f"[{now()}] ✅ Transacciones actualizadas — {len(filas) - 1} filas")


def actualizar_promedios_en_sheets(sh):
    """
    Lee el histórico de Resumen + Transacciones y genera la pestaña pivot
    'Vta Promedio x Día' con dos secciones:
      1. Tabla pivot: Local × Día de semana (venta $ promedio)
      2. Detalle por artículo: promedio qty vendida por producto/local
    """
    print(f"[{now()}] 📊 Calculando promedios por día de semana...")

    # ── Leer historial de Resumen para ventas brutas por día ──
    try:
        ws_res = sh.worksheet(GOOGLE_SHEET_TAB_RESUMEN)
        all_resumen = ws_res.get_all_records()
    except Exception as e:
        print(f"[{now()}] ⚠️ No se pudo leer Resumen para promedios: {e}")
        all_resumen = []

    # ── Leer historial de Transacciones para detalle por artículo ──
    try:
        ws_trans = sh.worksheet(GOOGLE_SHEET_TAB_TRANS)
        all_trans = ws_trans.get_all_records()
    except Exception as e:
        print(f"[{now()}] ⚠️ No se pudo leer Transacciones para promedios: {e}")
        all_trans = []

    if not all_resumen and not all_trans:
        print(f"[{now()}] ⚠️ Sin datos para calcular promedios")
        return

    # ── SECCIÓN 1: Pivot de Venta Promedio $ por Local × Día ──
    vta_por_dia = calcular_vta_promedio_por_dia(all_resumen)
    shops = sorted(vta_por_dia.keys())

    filas = []
    # Título sección 1
    filas.append(["VENTA PROMEDIO $ POR DÍA DE SEMANA", "", "", "", "", "", "", ""])
    filas.append(["", "", "", "", "", "", "", ""])

    # Header pivot: Local | Lunes | Martes | ... | Domingo
    header_pivot = ["Local"] + [DIAS_SEMANA_ES[d] for d in DIAS_SEMANA_ORDER]
    filas.append(header_pivot)
    pivot_header_row = len(filas)  # fila 3

    for shop in shops:
        row = [shop]
        for dia_num in DIAS_SEMANA_ORDER:
            info = vta_por_dia[shop].get(dia_num)
            if info and info["promedio"] > 0:
                row.append(info["promedio"])
            else:
                row.append("-")
        filas.append(row)

    # Fila con cantidad de días usados para el promedio
    filas.append([""])  # espacio
    filas.append(["Basado en:"] + ["" for _ in DIAS_SEMANA_ORDER])
    for shop in shops:
        row = [shop]
        for dia_num in DIAS_SEMANA_ORDER:
            info = vta_por_dia[shop].get(dia_num)
            if info and info["num_dias"] > 0:
                dia_name = DIAS_SEMANA_ES[dia_num][:3].lower()
                row.append(f"{info['num_dias']} {dia_name}.")
            else:
                row.append("-")
        filas.append(row)

    # ── SECCIÓN 2: Detalle por Artículo ──
    art_promedios = calcular_vta_promedio_por_articulo(all_trans)

    filas.append([""])
    filas.append([""])
    sec2_title_row = len(filas) + 1
    filas.append(["VENTA PROMEDIO POR ARTÍCULO", "", "", "", ""])
    filas.append(["", "", "", "", ""])
    filas.append(COLS_PROMEDIO_ART)
    art_header_row = len(filas)

    current_local = None
    for a in art_promedios:
        if a["local"] != current_local:
            if current_local is not None:
                filas.append(["", "", "", "", ""])  # separador
            current_local = a["local"]
        filas.append([
            a["producto"],
            a["local"],
            a["promedio_qty"],
            a["total_vendido"],
            a["dias_con_venta"],
        ])

    # ── Escribir al sheet ──
    total_rows = len(filas) + 10
    total_cols = max(len(header_pivot), len(COLS_PROMEDIO_ART)) + 1
    ws = _get_or_create_ws(sh, GOOGLE_SHEET_TAB_PROMEDIO, rows=max(total_rows, 500), cols=total_cols)

    ws.clear()
    time.sleep(1)
    ws.resize(rows=total_rows, cols=total_cols)
    ws.update(filas, "A1")

    # ── Formato visual ──
    time.sleep(1)

    # Título sección 1 - fondo verde, texto blanco, bold
    ws.format("A1:H1", {
        "backgroundColor": COLOR_ACCENT,
        "textFormat": {"bold": True, "foregroundColor": COLOR_ACCENT_FG, "fontSize": 12},
    })

    # Header pivot (fila 3)
    pivot_cols = len(header_pivot)
    col_end = chr(ord('A') + pivot_cols - 1)
    ws.format(f"A{pivot_header_row}:{col_end}{pivot_header_row}", {
        "backgroundColor": COLOR_HEADER_BG,
        "textFormat": {"bold": True, "foregroundColor": COLOR_HEADER_FG, "fontSize": 11},
        "horizontalAlignment": "CENTER",
    })

    # Formato moneda para celdas de datos pivot (filas después del header pivot)
    data_start = pivot_header_row + 1
    data_end = pivot_header_row + len(shops)
    if data_end >= data_start:
        ws.format(f"B{data_start}:{col_end}{data_end}", {
            "numberFormat": {"type": "NUMBER", "pattern": "$ #,##0"},
            "horizontalAlignment": "RIGHT",
        })
        # Colorear filas alternadas en la tabla pivot
        for i in range(data_start, data_end + 1, 2):
            ws.format(f"A{i}:{col_end}{i}", {
                "backgroundColor": COLOR_LIGHT_BLUE,
            })

    # "Basado en:" header
    basado_row = data_end + 2
    ws.format(f"A{basado_row}:A{basado_row}", {
        "textFormat": {"bold": True, "italic": True, "fontSize": 10},
    })

    # Título sección 2
    ws.format(f"A{sec2_title_row}:E{sec2_title_row}", {
        "backgroundColor": {"red": 0.16, "green": 0.50, "blue": 0.73},
        "textFormat": {"bold": True, "foregroundColor": COLOR_ACCENT_FG, "fontSize": 12},
    })

    # Header artículos
    ws.format(f"A{art_header_row}:E{art_header_row}", {
        "backgroundColor": COLOR_HEADER_BG,
        "textFormat": {"bold": True, "foregroundColor": COLOR_HEADER_FG, "fontSize": 11},
        "horizontalAlignment": "CENTER",
    })

    # Formato numérico para columnas C, D, E de artículos
    art_data_end = len(filas)
    if art_data_end > art_header_row:
        ws.format(f"C{art_header_row + 1}:E{art_data_end}", {
            "numberFormat": {"type": "NUMBER", "pattern": "#,##0.#"},
            "horizontalAlignment": "RIGHT",
        })

    # Freeze fila 1
    ws.freeze(rows=1)

    # Timestamp
    ts_row = len(filas) + 2
    ws.update_cell(ts_row, 1, f"Última actualización: {now()}")
    ws.format(f"A{ts_row}", {
        "textFormat": {"italic": True, "foregroundColor": {"red": 0.5, "green": 0.5, "blue": 0.5}},
    })

    print(f"[{now()}] ✅ Promedios actualizados — {len(shops)} locales, "
          f"{len(art_promedios)} artículos")


# ── Main ────────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*60}")
    modo = "BACKFILL" if MODO_BACKFILL else "diario"
    print(f"  Bistrosoft → Sheets ({modo}) | Cronklam | {now()}")
    print(f"{'='*60}\n")

    try:
        token = obtener_token()
        hoy = datetime.today()
        fecha_hasta = hoy.strftime("%Y-%m-%d")

        if MODO_BACKFILL:
            fecha_desde = BACKFILL_DESDE
            print(f"[{now()}] 🗓 BACKFILL: {fecha_desde} → {fecha_hasta} (puede tardar varios minutos)")
        elif FECHA_ESPECIFICA:
            fecha_desde = FECHA_ESPECIFICA
            print(f"[{now()}] 🗓 Fecha específica: {fecha_desde} → {fecha_hasta}")
        else:
            fecha_desde = (hoy - timedelta(days=2)).strftime("%Y-%m-%d")
            print(f"[{now()}] 🗓 Ventana diaria: {fecha_desde} → {fecha_hasta}")

        transacciones = descargar_transacciones(token, fecha_desde, fecha_hasta)

        if not transacciones:
            print(f"[{now()}] ⚠️ 0 registros — Bistrosoft no tiene datos para esta ventana.")
            return

        # Distribución de fechas
        dist_fechas = Counter(t.get("date", "?") for t in transacciones)
        print(f"[{now()}] 🔍 Fechas en respuesta: {len(dist_fechas)} días distintos, "
              f"{len(transacciones)} registros totales")
        for f, n in sorted(dist_fechas.items()):
            print(f"[{now()}]   {f}: {n} registros")

        fechas_validas = [f for f in dist_fechas if f != "?"]
        if not fechas_validas:
            print(f"[{now()}] ❌ No se encontraron fechas válidas.")
            return

        # Filtrar registros según el modo
        if MODO_BACKFILL:
            print(f"[{now()}] 📅 Backfill: procesando {len(fechas_validas)} día(s)")
        else:
            try:
                fecha_objetivo_dt = max(datetime.strptime(f, "%d-%m-%Y") for f in fechas_validas)
                fecha_objetivo = fecha_objetivo_dt.strftime("%d-%m-%Y")
            except Exception:
                fecha_objetivo = fechas_validas[0]

            trans_filtradas = [t for t in transacciones if t.get("date", "") == fecha_objetivo]
            descartados = len(transacciones) - len(trans_filtradas)
            print(f"[{now()}] 📅 Día objetivo: {fecha_objetivo} — "
                  f"{len(trans_filtradas)} registros ({descartados} de otros días descartados)")
            transacciones = trans_filtradas

        # Calcular resumen
        resumen = calcular_resumen(transacciones)
        print(f"\n[{now()}] 📊 Resumen ({len(resumen)} filas):")
        for r in resumen:
            print(f"  {r['date']} | {r['shop'][:35]:<35} | "
                  f"ventas={r['ventas']:>3} | coman={r['comandas']:>3} | "
                  f"total=${r['ventas_brutas']:>14,.0f}")

        # Conectar Google Sheets
        gc = gspread.service_account(filename=GOOGLE_CREDENTIALS_FILE)
        sh = gc.open_by_key(GOOGLE_SHEET_ID)

        # Actualizar Resumen
        actualizar_resumen_en_sheets(resumen, sh)

        # Actualizar Transacciones
        actualizar_transacciones_en_sheets(transacciones, sh)

        # Actualizar Promedios por Día
        time.sleep(2)  # Pequeña pausa para no saturar la API de Google
        actualizar_promedios_en_sheets(sh)

        print(f"\n[{now()}] ✅ Proceso completado exitosamente.\n")

    except SystemExit:
        raise
    except Exception as e:
        print(f"\n[{now()}] ❌ ERROR: {e}\n", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
