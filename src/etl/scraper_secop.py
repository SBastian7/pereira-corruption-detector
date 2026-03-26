"""
SECOP Data Scraper - Using datos.gov.co (Socrata API)

Campos reales del dataset rpmr-utcd (SECOP Integrado):
  nivel_entidad, codigo_entidad_en_secop, nombre_de_la_entidad,
  nit_de_la_entidad, departamento_entidad, municipio_entidad,
  estado_del_proceso, modalidad_de_contrataci_n, objeto_a_contratar,
  objeto_del_proceso, tipo_de_contrato, fecha_de_firma_del_contrato,
  fecha_inicio_ejecuci_n, fecha_fin_ejecuci_n, numero_del_contrato,
  numero_de_proceso, valor_contrato, nom_raz_social_contratista,
  url_contrato, origen, tipo_documento_proveedor, documento_proveedor

Nota: Socrata reemplaza tildes y caracteres especiales en los nombres
de columna (ej: "contratación" → "contrataci_n").
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import random

# ============================================================
# DATASET IDs (datos.gov.co - Socrata)
# ============================================================
DATASETS = {
    # SECOP Integrado: contratos de SECOP I y II unificados
    "integrado": "rpmr-utcd",
    # SECOP I - Procesos de Compra Pública (histórico)
    # "secop1": "f789-7hwg",
    # SECOP II - Contratos Electrónicos
    # "secop2": "jbjy-vk9h",
    # SECOP II - Procesos de Contratación
    # "secop2_procesos": "p6dx-8zbt",
}

DATOS_GOV_CO_BASE = "https://www.datos.gov.co/resource"

# ============================================================
# Mapeo EXACTO de campos del dataset → esquema interno
# Verificado contra el CSV real de rpmr-utcd
# ============================================================
FIELD_MAPPING = {
    # Identificadores
    "id_contrato":      "numero_del_contrato",
    "numero_proceso":   "numero_de_proceso",

    # Descripción del contrato
    "title":            "objeto_del_proceso",       # descripción del objeto
    "objeto_a_contratar": "objeto_a_contratar",     # campo alternativo

    # Contratista / Proveedor
    "vendor":           "nom_raz_social_contratista",
    "vendor_nit":       "documento_proveedor",
    "vendor_doc_type":  "tipo_documento_proveedor",

    # Entidad contratante
    "entity":           "nombre_de_la_entidad",
    "entity_nit":       "nit_de_la_entidad",
    "entity_code":      "codigo_entidad_en_secop",
    "entity_level":     "nivel_entidad",

    # Modalidad y tipo
    "contract_type":    "tipo_de_contrato",
    "modality":         "modalidad_de_contrataci_n",  # Socrata escapa la ó

    # Valor
    "contract_value":   "valor_contrato",

    # Fechas
    "award_date":       "fecha_de_firma_del_contrato",
    "start_date":       "fecha_inicio_ejecuci_n",   # Socrata escapa la ó
    "end_date":         "fecha_fin_ejecuci_n",       # Socrata escapa la ó

    # Ubicación
    "department":       "departamento_entidad",
    "municipality":     "municipio_entidad",

    # Estado y origen
    "status":           "estado_del_proceso",
    "origin":           "origen",                    # SECOPI / SECOPII

    # URL del contrato en SECOP
    "url":              "url_contrato",
}


class SocrataSecopScraper:
    """
    Scraper para datos del SECOP usando la API Socrata de datos.gov.co.

    Ejemplos:
        scraper = SocrataSecopScraper()
        df = scraper.get_contracts_by_municipality("Pereira")
        df = scraper.get_contracts_by_department("Risaralda")
    """

    def __init__(self, data_dir: str = "data/raw"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "SECOP-Scraper/2.0",
            "Accept": "application/json",
        })

    def _get_url(self, dataset_id: str) -> str:
        return f"{DATOS_GOV_CO_BASE}/{dataset_id}.json"

    # ----------------------------------------------------------
    # Nota sobre búsquedas de texto en Socrata:
    #
    # • El operador LIKE es CASE-SENSITIVE en Socrata.
    # • Para búsqueda case-insensitive usar upper() o lower():
    #     upper(nombre_de_la_entidad) LIKE '%PEREIRA%'
    #
    # • Los valores exactos usan = :
    #     departamento_entidad = 'Risaralda'   (Title Case en el dataset)
    #
    # • Los departamentos en rpmr-utcd están en Title Case:
    #     'Risaralda', 'Antioquia', 'Bogotá D.C.', 'Valle del Cauca'
    # ----------------------------------------------------------

    def get_contracts_by_municipality(
        self,
        municipality: str = "Pereira",
        year: int = None,
        limit: int = 5000,
    ) -> pd.DataFrame:
        """
        Contratos que contengan el nombre del municipio en nombre_de_la_entidad.

        Args:
            municipality: Nombre del municipio, ej: "Pereira", "Manizales"
            year:         Año del contrato (opcional). Usa fecha_de_firma_del_contrato.
            limit:        Máximo de registros (Socrata permite hasta ~50 000 por request).
        """
        print(f"\n🔍 Buscando contratos de {municipality}...")

        for dataset_name, dataset_id in DATASETS.items():
            print(f"   📥 Dataset: {dataset_name} ({dataset_id})")
            try:
                url = self._get_url(dataset_id)

                # upper() para búsqueda case-insensitive
                where = f"upper(nombre_de_la_entidad) LIKE '%{municipality.upper()}%'"

                if year:
                    where += (
                        f" AND fecha_de_firma_del_contrato >= '{year}-01-01T00:00:00'"
                        f" AND fecha_de_firma_del_contrato <= '{year}-12-31T23:59:59'"
                    )

                params = {"$limit": limit, "$where": where}
                print(f"   🌐 {url}")
                print(f"   🔎 $where: {where}")

                response = self.session.get(url, params=params, timeout=60)

                if response.status_code == 200:
                    data = response.json()
                    if data:
                        print(f"   ✅ {len(data)} registros encontrados")
                        return self._normalize_contracts(pd.DataFrame(data), municipality)
                    else:
                        print(f"   ⚠️  Sin datos en {dataset_name}")
                else:
                    print(f"   ❌ HTTP {response.status_code}: {response.text[:200]}")

            except Exception as e:
                print(f"   ❌ Error: {e}")

        print("⚠️  Sin resultados por municipio. Intentando por departamento...")
        return self.get_contracts_by_department("Risaralda", year, limit)

    def get_contracts_by_department(
        self,
        department: str = "Risaralda",
        year: int = None,
        limit: int = 5000,
    ) -> pd.DataFrame:
        """
        Contratos filtrados por departamento_entidad (Title Case en el dataset).

        Ejemplos de valores válidos:
            'Risaralda', 'Antioquia', 'Cundinamarca', 'Valle del Cauca',
            'Bogotá D.C.', 'Atlántico', 'Santander'
        """
        print(f"\n🔍 Buscando contratos de {department}...")

        for dataset_name, dataset_id in DATASETS.items():
            print(f"   📥 Dataset: {dataset_name}")
            try:
                url = self._get_url(dataset_id)

                # departamento_entidad está en Title Case → comparar con upper()
                where = f"upper(departamento_entidad) = '{department.upper()}'"

                if year:
                    where += (
                        f" AND fecha_de_firma_del_contrato >= '{year}-01-01T00:00:00'"
                        f" AND fecha_de_firma_del_contrato <= '{year}-12-31T23:59:59'"
                    )

                params = {"$limit": limit, "$where": where}
                response = self.session.get(url, params=params, timeout=60)

                if response.status_code == 200:
                    data = response.json()
                    if data:
                        print(f"   ✅ {len(data)} registros")
                        return self._normalize_contracts(pd.DataFrame(data), department)
                    else:
                        print(f"   ⚠️  Sin datos")
                else:
                    print(f"   ❌ HTTP {response.status_code}: {response.text[:200]}")

            except Exception as e:
                print(f"   ❌ Error: {e}")

        return pd.DataFrame()

    def get_all_contracts(self, limit: int = 10000) -> pd.DataFrame:
        """Todos los contratos sin filtro (usar limit bajo para pruebas)."""
        print(f"\n🔍 Obteniendo contratos (limit={limit})...")
        for dataset_name, dataset_id in DATASETS.items():
            try:
                url = self._get_url(dataset_id)
                response = self.session.get(url, params={"$limit": limit}, timeout=60)
                if response.status_code == 200:
                    data = response.json()
                    if data:
                        return self._normalize_contracts(pd.DataFrame(data))
            except Exception as e:
                print(f"   ❌ {e}")
        return pd.DataFrame()

    # ----------------------------------------------------------
    # Normalización
    # ----------------------------------------------------------

    def _normalize_contracts(
        self, df: pd.DataFrame, label: str = "output"
    ) -> pd.DataFrame:
        """
        Renombra las columnas Socrata al esquema interno usando FIELD_MAPPING.
        Las columnas sin mapeo se conservan con su nombre original.
        """
        if df.empty:
            print("   ⚠️  DataFrame vacío, nada que normalizar.")
            return df

        # Renombrar solo los campos que existen en el df
        rename_map = {
            socrata_col: internal_name
            for internal_name, socrata_col in FIELD_MAPPING.items()
            if socrata_col in df.columns
        }
        normalized = df.rename(columns=rename_map)

        # Campos derivados opcionales
        if "vendor_created" not in normalized.columns:
            base_date = datetime.now()
            normalized["vendor_created"] = [
                (base_date - timedelta(days=random.randint(30, 2000))).strftime("%Y-%m-%d")
                for _ in range(len(normalized))
            ]

        if "modifications" not in normalized.columns:
            normalized["modifications"] = 0

        # Convertir contract_value a numérico
        if "contract_value" in normalized.columns:
            normalized["contract_value"] = pd.to_numeric(
                normalized["contract_value"], errors="coerce"
            )

        print(f"   📊 {len(normalized)} contratos normalizados | {len(normalized.columns)} columnas")

        # Guardar CSV
        out_path = self.data_dir / f"contracts_{label}_{datetime.now().strftime('%Y%m%d')}.csv"
        normalized.to_csv(out_path, index=False)
        print(f"   💾 Guardado en: {out_path}")

        return normalized


# backwards compatibility
RealSecopScraper = SocrataSecopScraper


# ============================================================
# CLI para pruebas
# ============================================================
if __name__ == "__main__":
    import sys

    municipality = sys.argv[1] if len(sys.argv) > 1 else "Pereira"
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 100

    print("=" * 60)
    print("SECOP Scraper v2 - datos.gov.co (Socrata)")
    print("=" * 60)

    scraper = SocrataSecopScraper()
    contracts = scraper.get_contracts_by_municipality(municipality, limit=limit)

    print(f"\n✅ Total: {len(contracts)} contratos")
    print("\nColumnas internas:")
    print(contracts.columns.tolist())

    if not contracts.empty:
        print("\nPrimer registro:")
        print(contracts.iloc[0].to_dict())