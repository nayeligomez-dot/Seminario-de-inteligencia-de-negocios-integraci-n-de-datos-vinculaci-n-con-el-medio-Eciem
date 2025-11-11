"""
ETL End-to-End (Extracción corregida desde API ECIEM)
- Lee la clave 'filas' del JSON
- Paginación por total/limit/offset
- Limpieza mínima (fechas "0000-00-00" -> NaT, strip strings)
- Detección de columna RUT
- Guarda CSV en ./data
"""

import os
import sys
import json
import logging
from typing import List, Dict, Any, Optional

import requests
import pandas as pd


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
log = logging.getLogger(__name__)


API_URL = "https://www.eciem.cl/api/api_datos.php"
TOKEN = "Eciem_20252026"  
TABLAS = [
    "alumn_pract",
    "alumn_pract_eva_inf_pract",
    "alumn_pract_eva_jef",
]
LIMIT = 2000  
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)



def _safe_json_preview(text: str, n: int = 300) -> str:
    t = text.replace("\n", " ")
    return (t[:n] + "...") if len(t) > n else t


def get_json(url: str, params: Dict[str, Any], timeout: int = 40) -> Dict[str, Any]:
    """GET y parseo JSON con manejo de errores legible."""
    r = requests.get(url, params=params, timeout=timeout)
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        raise RuntimeError(
            f"HTTP error {r.status_code} en {r.url} -> {_safe_json_preview(r.text)}"
        ) from e
    try:
        return r.json()
    except Exception as e:
        raise RuntimeError(
            f"No pude parsear JSON en {r.url} -> {_safe_json_preview(r.text)}"
        ) from e


def descargar_tabla(api_url: str, token: str, tabla: str, limit: int = 2000) -> pd.DataFrame:
    """
    Descarga todas las páginas de una tabla leyendo la clave 'filas' del JSON.
    Estructura esperada:
    {
      "tabla": "alumn_pract",
      "limit": 2000,
      "offset": 0,
      "total": 1576,
      "filas": [ { ...registros... } ]
    }
    """
    log.info("[API] Descargando %s ...", tabla)
    offset = 0
    acumulado: List[Dict[str, Any]] = []
    pagina = 1

    while True:
        log.info("[API] GET tabla='%s' offset=%d", tabla, offset)
        payload = get_json(api_url, {"tabla": tabla, "token": token, "limit": limit, "offset": offset})

        if not isinstance(payload, dict) or "filas" not in payload:
            raise RuntimeError(f"Respuesta inesperada para {tabla}: {json.dumps(payload)[:400]}")

        filas = payload.get("filas", [])
        total = int(payload.get("total", 0))
        log.info("[API] %s page %d OK (%d filas)", tabla, pagina, len(filas))

        if not filas:
            break

        # Unimos registros de esta página
        acumulado.extend(filas)

        # Siguiente página
        offset += limit
        pagina += 1
        if offset >= total:
            break

    # DataFrame final de la tabla
    df = pd.DataFrame(acumulado)
    return df


def limpiar_df(df: pd.DataFrame) -> pd.DataFrame:
    """Limpieza mínima genérica: strip de strings, fechas nulas a NaT."""
    if df.empty:
        return df

    # Trim y coerción básica a string en columnas tipo objeto
    for c in df.columns:
        if pd.api.types.is_object_dtype(df[c]):
            df[c] = df[c].astype(str).str.strip()

    # Detectar columnas de fecha por prefijo común y normalizar "0000-00-00"
    posibles_fecha = [c for c in df.columns if c.lower().startswith(("fech_", "fecha_"))]
    for c in posibles_fecha:
        if c in df.columns:
            df[c] = df[c].replace({"0000-00-00": None, "0000-00-00 00:00:00": None})
            df[c] = pd.to_datetime(df[c], errors="coerce")

    return df


def detectar_columna_rut(cols: List[str]) -> Optional[str]:
    """Devuelve la primera coincidencia conocida para RUT."""
    if not cols:
        return None
    lowered = [c.lower() for c in cols]
    candidatos = [
        "rut_alum", "rut", "rut_alumno", "rutalum", "rut_alumn",
        "rut_estudiante", "rut_alumno_practica"
    ]
    for alias in candidatos:
        if alias in lowered:
            return cols[lowered.index(alias)]
    return None


def guardar_csv(df: pd.DataFrame, nombre: str) -> str:
    """Guarda DF a CSV UTF-8 en la carpeta data con el nombre indicado."""
    ruta = os.path.join(DATA_DIR, f"{nombre}.csv")
    df.to_csv(ruta, index=False, encoding="utf-8")
    return ruta



def main():
    # Verificación rápida del token
    if not TOKEN or TOKEN == "pon tu token aca":
        log.error("Debes configurar el TOKEN válido antes de ejecutar.")
        sys.exit(1)

    resumen = []

    for tabla in TABLAS:
        # 1) Descargar
        df = descargar_tabla(API_URL, TOKEN, tabla, limit=LIMIT)

        if df.empty:
            log.warning("[API] %s sin filas.", tabla)
        else:
            log.info("%s: %d filas descargadas.", tabla, len(df))

        # 2) Limpiar
        df = limpiar_df(df)

        # 3) Guardar CSV
        ruta = guardar_csv(df, tabla)
        log.info("%s: %d filas → %s", tabla, len(df), ruta)

        # 4) Reporte de columnas y detección de RUT
        cols = list(df.columns)
        log.info("%s columnas finales: %s", tabla, cols[:12] + (["..."] if len(cols) > 12 else []))

        col_rut = detectar_columna_rut(cols)
        if col_rut:
            log.info("[MAP] %s: RUT_ALUM = %s", tabla, col_rut)
        else:
            log.warning("[MAP] %s: No detecté columna RUT en %s", tabla, cols)

        # 5) Muestra de datos
        if not df.empty:
            log.info("Muestra %s:\n%s", tabla, df.head(3).to_string(index=False))

        resumen.append({
            "tabla": tabla,
            "filas": len(df),
            "rut_detectado": col_rut or "(no)"
        })

    # 6) Resumen final en consola
    log.info("ETL completado con datos disponibles ✔")
    for r in resumen:
        log.info("→ %s | filas=%s | RUT=%s", r["tabla"], r["filas"], r["rut_detectado"])


if __name__ == "__main__":
    main()


import pandas as pd

def mostrar_estructura_real():
    tablas = ["alumn_pract", "alumn_pract_eva_inf_pract", "alumn_pract_eva_jef"]
    
    for tabla in tablas:
        try:
            df = pd.read_csv(f"data/{tabla}.csv")
            print(f"\n=== {tabla.upper()} ===")
            print(f"Columnas: {list(df.columns)}")
            print(f"Primeras 2 filas:")
            print(df.head(2).to_string(index=False))
            print("-" * 50)
        except Exception as e:
            print(f"Error leyendo {tabla}: {e}")

if __name__ == "__main__":
    mostrar_estructura_real()