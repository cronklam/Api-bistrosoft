"""
bistrosoft_to_sheets.py  — Cronklam / L'Harmonie
==============================================================
Arquitectura:
  1. Consulta Bistrosoft API para UN SOLO DÍA (ayer por defecto).
     IMPORTANTE: el API de Bistrosoft devuelve todos los registros con la
     fecha del rango más reciente si se consulta un rango multi-día.
     Por eso siempre usamos startDate = endDate = un día específico.
  2. Calcula el resumen diario en Python.
  3. Pestaña "Resumen"       → histórico acumulativo (merge inteligente).
  4. Pestaña "Transacciones" → detalle del día descargado.

Sin Supabase. Sin VIEW. Sin dedup complejo.
Basado en el script original de Pomodoro Consulting (Tomás).
"""

import requests
import gspread
from datetime import datetime, timedelta
from collections import defaultdict
import time
import sys

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================

BISTRO_BASE_URL  = "https://ar-api.bistrosoft.com/api/v1"
BISTRO_USERNAME  = "pomodoroconsulting@gmail.com"
BISTRO_PASSWORD  = "7027"

GOOGLE_CREDENTIALS_FILE  = "service_account.json"
GOOGLE_SHEET_ID          = "1s6kPguwD25k3xpmbUoHq1KNFd_SEva3z7pvTGhA4bsE"
GOOGLE_SHEET_TAB_TRANS   = "Transacciones"
GOOGLE_SHEET_TAB_RESUMEN = "Resumen"

# Día a consultar en modo normal. None = ayer automáticamente.
FECHA_ESPECIFICA = None

# ── MODO BACKFILL ──────────────────────────────────────────────────────────────
# Para traer todo un mes de golpe:
#   1. Cambiar MODO_BACKFILL = True
#   2. Ajustar BACKFILL_DESDE a la fecha inicial que quieras
#   3. Subir al repo y correr manualmente (puede tardar 20-40 min)
#   4. Volver a cambiar MODO_BACKFILL = False para el cron diario
MODO_BACKFILL   = False
BACKFILL_DESDE  = "2026-03-01"  # Inicio del período a importar
# ──────────────────────────────────────────────────────────────────────────────

# ==============================================================================
# FIN CONFIGURACIÓN
# ==============================================================================


def now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ── Bistrosoft ────────────────────────────────────────────────────────────────

def obtener_token():
    resp = requests.post(
        f"{BISTRO_BASE_URL}/Token",
        json={"username": BISTRO_USERNAME, "password": BISTRO_PASSWORD},
        timeout=30,
    )
    resp.raise_for_status()
    data  = resp.json()
    token = data.get("token") or data.get("access_token") or data.get("Token")
    if not token:
        raise Exception(f"No se encontró token en respuesta: {data}")
    print(f"[{now()}] ✅ Token Bistrosoft obtenido")
    return token


def descargar_transacciones(token, fecha_desde, fecha_hasta):
    """Descarga TODAS las páginas del TransactionDetailReport para el rango dado."""
    url     = f"{BISTRO_BASE_URL}/TransactionDetailReport"
    headers = {"Authorization": f"Bearer {token}"}
    todas, page = [], 0

    print(f"[{now()}] 📥 Descargando Bistrosoft: {fecha_desde} → {fecha_hasta}...")
    while True:
        params = {"startDate": fecha_desde, "endDate": fecha_hasta, "pageNumber": page}
        resp   = requests.get(url, headers=headers, params=params, timeout=60)
        resp.raise_for_status()
        data        = resp.json()
        items       = data.get("items", [])
        total_pages = data.get("totalPages", 1)
        todas.extend(items)
        print(f"[{now()}]   Pág {page + 1}/{total_pages} — {len(items)} registros")
        if page + 1 >= total_pages or not items:
            break
        page += 1
        time.sleep(5)   # Límite Bistrosoft: máx 12 req/min

    print(f"[{now()}] ✅ {len(todas)} transacciones descargadas")
    return todas


# ── Cálculo del resumen ────────────────────────────────────────────────────────

# Tipos que cuentan como venta/comanda en el TOTAL VENDIDO del portal.
# Solo Venta y Comanda con status CLOSE y amount > 0.
_TIPOS_VENTA    = {"Venta", "Comanda"}
_STATUS_VALIDO  = {"CLOSE"}


def calcular_resumen(transacciones):
    """
    Calcula ventas brutas agrupadas por (date, shop).
    Regla: transaction_type IN ('Venta','Comanda') AND status='CLOSE' AND amount > 0.

    DEDUP: el API de Bistrosoft puede devolver múltiples registros para el mismo
    ticket (por modificaciones de orden, pagos parciales, etc.). Deduplicamos por
    (date, shop, ticketNumber, transactionType) y mantenemos el de mayor amount,
    para contar cada venta exactamente una vez — igual que el portal.
    """
    # Paso 1: deduplicar por ticket
    ticket_map: dict = {}
    for t in transacciones:
        tipo    = (t.get("transactionType") or "").strip()
        status  = (t.get("status") or "").strip()
        amount  = float(t.get("amount") or 0)
        fecha   = (t.get("date") or "").strip()
        shop    = (t.get("shop") or "").strip()

        if tipo not in _TIPOS_VENTA:   continue
        if status not in _STATUS_VALIDO: continue
        if amount <= 0 or not fecha or not shop: continue

        ticket = str(t.get("ticketNumber") or t.get("ticket_number") or "")
        key    = (fecha, shop, ticket, tipo)
        if key not in ticket_map or amount > float(ticket_map[key].get("amount") or 0):
            ticket_map[key] = t

    # Informativo: cuántos registros eliminó el dedup
    total_antes = sum(
        1 for t in transacciones
        if (t.get("transactionType") or "").strip() in _TIPOS_VENTA
        and (t.get("status") or "").strip() in _STATUS_VALIDO
        and float(t.get("amount") or 0) > 0
    )
    print(f"[{now()}] 🔄 Dedup tickets: {total_antes} registros → {len(ticket_map)} únicos")

    # Paso 2: agregar los registros deduplicados
    agg = defaultdict(lambda: {
        "ventas_n": 0, "ventas_m": 0.0,
        "comandas_n": 0, "comandas_m": 0.0,
        "efectivo": 0.0, "tarjeta": 0.0,
    })

    for t in ticket_map.values():
        tipo    = (t.get("transactionType") or "").strip()
        amount  = float(t.get("amount") or 0)
        fecha   = (t.get("date") or "").strip()
        shop    = (t.get("shop") or "").strip()
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
            "date":               fecha,
            "shop":               shop,
            "ventas":             v["ventas_n"],
            "comandas":           v["comandas_n"],
            "total_transacciones": v["ventas_n"] + v["comandas_n"],
            "ventas_brutas":      total,
            "efectivo":           round(v["efectivo"], 2),
            "tarjeta_otros":      round(v["tarjeta"],  2),
        })

    result.sort(key=_sort_key)
    return result


def _sort_key(r):
    try:
        d = datetime.strptime(str(r.get("date", "")), "%d-%m-%Y")
    except ValueError:
        d = datetime.min
    return (-d.toordinal(), str(r.get("shop", "")))


# ── Google Sheets ─────────────────────────────────────────────────────────────

COLS_RESUMEN = [
    "date", "shop", "total_transacciones", "ventas_brutas",
    "efectivo", "tarjeta_otros", "ventas", "comandas",
]

COLS_TRANS = [
    "date", "hour", "shop", "transaction_type", "status",
    "ticket_number", "amount", "payment_method", "product",
    "quantity", "client", "waiter", "table_name", "category",
]


def _get_or_create_ws(sh, nombre, rows=5000, cols=20):
    try:
        return sh.worksheet(nombre)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=nombre, rows=str(rows), cols=str(cols))


def actualizar_resumen_en_sheets(nuevo_resumen, sh):
    """
    Merge inteligente: mantiene el histórico completo y solo reemplaza
    las filas de las fechas que acabamos de calcular.
    """
    ws = _get_or_create_ws(sh, GOOGLE_SHEET_TAB_RESUMEN, rows=2000, cols=len(COLS_RESUMEN) + 2)

    # Leer histórico existente (puede estar vacío en la primera corrida)
    existing_raw = ws.get_all_records()

    # Fechas que vamos a actualizar
    fechas_nuevas = {r["date"] for r in nuevo_resumen}

    # Conservar historial de fechas que NO tocamos + sumar las nuevas
    historico_sin_overlap = [r for r in existing_raw if str(r.get("date", "")) not in fechas_nuevas]
    todos = historico_sin_overlap + nuevo_resumen
    todos.sort(key=_sort_key)

    # Construir tabla
    filas = [COLS_RESUMEN]
    for r in todos:
        filas.append([
            str(r.get("date",               "")),
            str(r.get("shop",               "")),
            r.get("total_transacciones",     0),
            r.get("ventas_brutas",           0),
            r.get("efectivo",                0),
            r.get("tarjeta_otros",           0),
            r.get("ventas",                  0),
            r.get("comandas",                0),
        ])

    ws.clear()
    time.sleep(1)
    ws.resize(rows=len(filas) + 5)
    ws.update(filas, "A1")
    ws.format(f"A1:{chr(ord('A') + len(COLS_RESUMEN) - 1)}1", {"textFormat": {"bold": True}})
    ws.update_cell(len(filas) + 2, 1, f"Última actualización: {now()}")
    print(f"[{now()}] ✅ Resumen actualizado — {len(todos)} filas históricas ({len(nuevo_resumen)} fechas nuevas/actualizadas)")


def actualizar_transacciones_en_sheets(transacciones, sh):
    """Escribe el detalle de transacciones del período descargado."""
    ws    = _get_or_create_ws(sh, GOOGLE_SHEET_TAB_TRANS, rows=max(len(transacciones) + 10, 5000))
    filas = [COLS_TRANS]

    for t in transacciones:
        filas.append([
            str(t.get("date")            or ""),
            str(t.get("hour")            or ""),
            str(t.get("shop")            or ""),
            str(t.get("transactionType") or ""),
            str(t.get("status")          or ""),
            str(t.get("ticketNumber")    or ""),
            float(t.get("amount")        or 0),
            str(t.get("paymentMethod")   or ""),
            str(t.get("product")         or ""),
            str(t.get("quantity")        or ""),
            str(t.get("client")          or ""),
            str(t.get("waiter")          or ""),
            str(t.get("tableName")       or ""),
            str(t.get("category")        or ""),
        ])

    ws.clear()
    time.sleep(1)
    ws.resize(rows=len(filas) + 5)
    CHUNK = 5000
    for i in range(0, len(filas), CHUNK):
        ws.update(filas[i:i + CHUNK], f"A{i + 1}")
        if i + CHUNK < len(filas):
            time.sleep(2)
    ws.format(f"A1:{chr(ord('A') + len(COLS_TRANS) - 1)}1", {"textFormat": {"bold": True}})
    ws.update_cell(len(filas) + 2, 1, f"Última actualización: {now()}")
    print(f"[{now()}] ✅ Transacciones actualizadas — {len(filas) - 1} filas")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*60}")
    modo = "BACKFILL" if MODO_BACKFILL else "diario"
    print(f"  Bistrosoft → Sheets ({modo}) | Cronklam | {now()}")
    print(f"{'='*60}\n")

    try:
        # 1. Descargar de Bistrosoft
        # El API requiere que endDate = hoy para devolver datos históricos.
        token = obtener_token()
        hoy         = datetime.today()
        fecha_hasta = hoy.strftime("%Y-%m-%d")

        if MODO_BACKFILL:
            fecha_desde = BACKFILL_DESDE
            print(f"[{now()}] 🗓  BACKFILL: {fecha_desde} → {fecha_hasta} (puede tardar varios minutos)")
        elif FECHA_ESPECIFICA:
            fecha_desde = FECHA_ESPECIFICA
            print(f"[{now()}] 🗓  Fecha específica: {fecha_desde} → {fecha_hasta}")
        else:
            fecha_desde = (hoy - timedelta(days=2)).strftime("%Y-%m-%d")
            print(f"[{now()}] 🗓  Ventana diaria: {fecha_desde} → {fecha_hasta}")

        transacciones = descargar_transacciones(token, fecha_desde, fecha_hasta)

        if not transacciones:
            print(f"[{now()}] ⚠️  0 registros — Bistrosoft no tiene datos para esta ventana.")
            print(f"[{now()}]    Si son las 9 AM Buenos Aires y esto pasa, esperá unos minutos y reintentá.")
            return

        # DEBUG: distribución de fechas
        from collections import Counter
        dist_fechas = Counter(t.get("date", "?") for t in transacciones)
        print(f"[{now()}] 🔍 Fechas en respuesta: {len(dist_fechas)} días distintos, "
              f"{len(transacciones)} registros totales")
        for f, n in sorted(dist_fechas.items()):
            print(f"[{now()}]    {f}: {n} registros")

        fechas_validas = [f for f in dist_fechas if f != "?"]
        if not fechas_validas:
            print(f"[{now()}] ❌ No se encontraron fechas válidas.")
            return

        # 2. Filtrar registros según el modo
        if MODO_BACKFILL:
            # Backfill: procesamos TODOS los días disponibles
            print(f"[{now()}] 📅 Backfill: procesando {len(fechas_validas)} día(s)")
        else:
            # Modo diario: solo el día más reciente con datos
            try:
                fecha_objetivo_dt = max(datetime.strptime(f, "%d-%m-%Y") for f in fechas_validas)
                fecha_objetivo    = fecha_objetivo_dt.strftime("%d-%m-%Y")
            except Exception:
                fecha_objetivo = fechas_validas[0]
            trans_filtradas = [t for t in transacciones if t.get("date", "") == fecha_objetivo]
            descartados     = len(transacciones) - len(trans_filtradas)
            print(f"[{now()}] 📅 Día objetivo: {fecha_objetivo} — "
                  f"{len(trans_filtradas)} registros ({descartados} de otros días descartados)")
            transacciones = trans_filtradas

        # 3. Calcular resumen en Python
        resumen = calcular_resumen(transacciones)
        print(f"\n[{now()}] 📊 Resumen ({len(resumen)} filas):")
        for r in resumen:
            print(f"   {r['date']} | {r['shop'][:35]:<35} | "
                  f"ventas={r['ventas']:>3} | coman={r['comandas']:>3} | "
                  f"total=${r['ventas_brutas']:>14,.0f}")

        # 4. Conectar Google Sheets
        gc = gspread.service_account(filename=GOOGLE_CREDENTIALS_FILE)
        sh = gc.open_by_key(GOOGLE_SHEET_ID)

        # 5. Actualizar Resumen (histórico, merge inteligente)
        actualizar_resumen_en_sheets(resumen, sh)

        # 6. Actualizar Transacciones (detalle del período)
        actualizar_transacciones_en_sheets(transacciones, sh)

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
