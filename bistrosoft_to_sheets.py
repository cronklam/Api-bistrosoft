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
    5. Pestaña "Stock Mínimo"   → stock mínimo diario = prom. qty últimos 2 días × 2.

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
GOOGLE_SHEET_TAB_PROMEDIO = "Stock Mínimo"

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


def _parse_fecha(fecha_str):
    """Intenta parsear fecha en varios formatos. Retorna datetime o None."""
    for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(fecha_str.strip(), fmt)
        except ValueError:
            continue
    return None


def _parse_monto(valor):
    """Parsea un valor monetario que puede venir como número o string con formato argentino.
    Maneja: 11524500.0, "$ 11.524.500,00", "11524500", "$11,524,500.00", etc.
    """
    if isinstance(valor, (int, float)):
        return float(valor)
    s = str(valor).replace("$", "").replace(" ", "").strip()
    if not s:
        return 0.0
    # Detectar formato argentino: "11.524.500,00" (. = miles, , = decimal)
    if "," in s and "." in s:
        # Si la coma viene después del último punto → formato argentino
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            # Formato US/internacional: "11,524,500.00"
            s = s.replace(",", "")
    elif "," in s:
        # Solo coma → puede ser decimal argentino "500,50"
        s = s.replace(",", ".")
    # Si quedan múltiples puntos, eliminar todos menos el último (miles)
    parts = s.split(".")
    if len(parts) > 2:
        s = "".join(parts[:-1]) + "." + parts[-1]
    try:
        return float(s)
    except (ValueError, TypeError):
        return 0.0


def calcular_stock_minimo(transacciones_ws):
    """
    Calcula stock mínimo diario por artículo por local.
    Fórmula: (total qty del período / días en rango) × 2.
    NOTA: la API de Bistrosoft devuelve todas las fechas iguales (la más
    reciente del rango), así que no podemos diferenciar por día real.
    Usamos el total acumulado dividido por la cantidad de días del rango.
    """
    DIAS_RANGO = 15  # ventana de consulta

    # {(shop, product): total_qty}
    ventas = defaultdict(float)

    for t in transacciones_ws:
        tipo = str(t.get("transaction_type") or t.get("transactionType") or t.get("Tipo") or "").strip()
        product = str(t.get("product") or t.get("Producto") or "").strip()
        shop = str(t.get("shop") or t.get("Local") or "").strip()
        qty = 0
        try:
            qty = float(t.get("quantity") or t.get("Cantidad") or t.get("qty") or 0)
        except (ValueError, TypeError):
            qty = 0

        if "ITEM" not in tipo.upper():
            continue
        if not product or product == "-" or not shop:
            continue
        if qty <= 0:
            continue

        ventas[(shop, product)] += qty

    resultado = []
    for (shop, product), total_qty in ventas.items():
        promedio_diario = total_qty / DIAS_RANGO
        stock_min = round(promedio_diario * 2)
        if stock_min <= 0:
            continue
        resultado.append({
            "local": shop,
            "producto": product,
            "prom_qty_diario": round(promedio_diario, 1),
            "stock_minimo": stock_min,
            "total_qty": round(total_qty, 1),
            "dias_rango": DIAS_RANGO,
        })

    resultado.sort(key=lambda r: (r["local"], -r["stock_minimo"]))
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

COLS_STOCK_MIN = [
    "Local", "Producto", "Prom. Qty Diario",
    "Stock Mínimo", "Qty Total (15d)", "Días Rango",
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
    """Aplica colores alternados a las filas de datos.
    DESHABILITADO: cada fila genera 1 API call, excede rate limit de Google Sheets.
    """
    return  # Skip para evitar 429 rate limit


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
    Lee el histórico de Transacciones y genera la pestaña 'Stock Mínimo'
    con el stock mínimo diario por artículo por local.
    Fórmula: promedio qty últimos 2 días × 2.
    """
    print(f"[{now()}] 📊 Calculando stock mínimo por artículo/local...")

    # ── Leer historial de Transacciones ──
    try:
        ws_trans = sh.worksheet(GOOGLE_SHEET_TAB_TRANS)
        all_trans = ws_trans.get_all_records()
    except Exception as e:
        print(f"[{now()}] ⚠️ No se pudo leer Transacciones: {e}")
        all_trans = []

    if not all_trans:
        print(f"[{now()}] ⚠️ Sin datos para calcular stock mínimo")
        return

    stock_data = calcular_stock_minimo(all_trans)

    filas = []
    # Título
    filas.append(["STOCK MÍNIMO DIARIO POR LOCAL", "", "", "", "", ""])
    filas.append(["Fórmula: (qty total 15 días / 15) × 2", "", "", "", "", ""])
    filas.append([""])

    # Header
    filas.append(COLS_STOCK_MIN)
    header_row = len(filas)  # fila 4

    current_local = None
    for item in stock_data:
        if item["local"] != current_local:
            if current_local is not None:
                filas.append(["", "", "", "", "", ""])  # separador entre locales
            current_local = item["local"]
        filas.append([
            item["local"],
            item["producto"],
            item["prom_qty_diario"],
            item["stock_minimo"],
            item["total_qty"],
            item["dias_rango"],
        ])

    # ── Escribir al sheet ──
    total_rows = len(filas) + 10
    total_cols = len(COLS_STOCK_MIN) + 1
    ws = _get_or_create_ws(sh, GOOGLE_SHEET_TAB_PROMEDIO, rows=max(total_rows, 500), cols=total_cols)

    ws.clear()
    time.sleep(1)
    ws.resize(rows=total_rows, cols=total_cols)
    ws.update(filas, "A1")

    # ── Formato visual ──
    time.sleep(1)

    num_cols = len(COLS_STOCK_MIN)
    col_end = chr(ord('A') + num_cols - 1)

    # Título fila 1 - fondo verde oscuro
    ws.format(f"A1:{col_end}1", {
        "backgroundColor": COLOR_ACCENT,
        "textFormat": {"bold": True, "foregroundColor": COLOR_ACCENT_FG, "fontSize": 12},
    })

    # Subtítulo fila 2 - itálica
    ws.format(f"A2:{col_end}2", {
        "textFormat": {"italic": True, "foregroundColor": {"red": 0.4, "green": 0.4, "blue": 0.4}, "fontSize": 10},
    })

    # Header (fila 4)
    ws.format(f"A{header_row}:{col_end}{header_row}", {
        "backgroundColor": COLOR_HEADER_BG,
        "textFormat": {"bold": True, "foregroundColor": COLOR_HEADER_FG, "fontSize": 11},
        "horizontalAlignment": "CENTER",
    })

    # Formato numérico columnas C y D (Prom. Qty y Stock Mínimo)
    data_end = len(filas)
    if data_end > header_row:
        ws.format(f"C{header_row + 1}:C{data_end}", {
            "numberFormat": {"type": "NUMBER", "pattern": "#,##0.0"},
            "horizontalAlignment": "RIGHT",
        })
        ws.format(f"D{header_row + 1}:D{data_end}", {
            "numberFormat": {"type": "NUMBER", "pattern": "#,##0"},
            "horizontalAlignment": "CENTER",
            "textFormat": {"bold": True},
        })
        ws.format(f"E{header_row + 1}:E{data_end}", {
            "numberFormat": {"type": "NUMBER", "pattern": "#,##0"},
            "horizontalAlignment": "CENTER",
        })

    # Colorear filas alternadas — DESHABILITADO (excede rate limit)
    # Se omite zebra-striping para no superar 60 writes/min de Google Sheets API

    # Resaltar columna Stock Mínimo con fondo amarillo claro
    if data_end > header_row:
        ws.format(f"D{header_row + 1}:D{data_end}", {
            "backgroundColor": COLOR_LIGHT_YELLOW,
            "numberFormat": {"type": "NUMBER", "pattern": "#,##0"},
            "horizontalAlignment": "CENTER",
            "textFormat": {"bold": True},
        })

    # Freeze header
    ws.freeze(rows=header_row)

    # Filtro
    ws.set_basic_filter(f"A{header_row}:{col_end}")

    # Timestamp
    ts_row = len(filas) + 2
    ws.update_cell(ts_row, 1, f"Última actualización: {now()}")
    ws.format(f"A{ts_row}", {
        "textFormat": {"italic": True, "foregroundColor": {"red": 0.5, "green": 0.5, "blue": 0.5}},
    })

    print(f"[{now()}] ✅ Stock mínimo actualizado — {len(stock_data)} artículos")


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
            fecha_desde = (hoy - timedelta(days=15)).strftime("%Y-%m-%d")
            print(f"[{now()}] 🗓 Ventana 15 días: {fecha_desde} → {fecha_hasta}")

        # ── Descargar transacciones ──
        # NOTA: la API de Bistrosoft NO soporta consultas de un solo día
        # (startDate = endDate devuelve 0).  Se debe usar rango multi-día.
        # La API puede devolver todos los registros con la fecha del
        # extremo más reciente; por eso confiamos en el campo "date" tal
        # cual viene.
        transacciones = descargar_transacciones(token, fecha_desde, fecha_hasta)
        print(f"[{now()}] ✅ Total descargado: {len(transacciones)} transacciones")

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

        # En modo 15 días y backfill procesamos TODOS los días
        print(f"[{now()}] 📅 Procesando {len(fechas_validas)} día(s), "
              f"{len(transacciones)} registros totales")

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
        time.sleep(30)  # Pausa larga para resetear rate limit de Google Sheets API
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
