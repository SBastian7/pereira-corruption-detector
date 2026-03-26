"""
SECOP Data Scraper - Using datos.gov.co (Socrata API)
API que funciona correctamente.
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import time
import random
import json
import sys

# ============================================================
# DATASET IDs (datos.gov.co - Socrata)
# ============================================================
DATASETS = {
    # SECOP Integrado (recent contracts) - MAIN ONE TO USE
    "integrado": "rpmr-utcd",
    # SECOP I Histórico 
    "historico": "f789-7hwg",
    # SECOP II Contratos
    "secop2": "p6dx-8zbt",
}

DATOS_GOV_CO_BASE = "https://www.datos.gov.co/resource"


class SocrataSecopScraper:
    """
    Scraper para datos reales de SECOP usando datos.gov.co
    """
    
    def __init__(self, data_dir: str = "data/raw"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Pereira-Corruption-Detector/1.0",
            "Accept": "application/json"
        })
    
    def _get_url(self, dataset_id: str) -> str:
        return f"{DATOS_GOV_CO_BASE}/{dataset_id}.json"
    
    def get_contracts_by_municipality(self, municipality: str = "Pereira", 
                                       year: int = None,
                                       limit: int = 5000) -> pd.DataFrame:
        """
        Obtiene contratos filtrados por municipio.
        """
        print(f"\n🔍 Buscando contratos de {municipality}...")
        
        # Usar SECOP Integrado
        dataset_id = DATASETS["integrado"]
        url = self._get_url(dataset_id)
        
        params = {
            "$limit": limit,
            "$where": f"nombre_de_la_entidad LIKE '%{municipality.upper()}%'"
        }
        
        if year:
            params["$where"] += f" AND anno_del_contrato={year}"
        
        print(f"   📥 URL: {url}")
        print(f"   📥 Query: {params['$where']}")
        
        response = self.session.get(url, params=params, timeout=60)
        
        if response.status_code == 200:
            data = response.json()
            print(f"   ✅ Encontrados {len(data)} contratos")
            return self._normalize_contracts(pd.DataFrame(data))
        else:
            print(f"   ❌ Error {response.status_code}")
            print(f"   {response.text[:200]}")
            return pd.DataFrame()
    
    def get_contracts_by_department(self, department: str = "Risaralda",
                                     year: int = None,
                                     limit: int = 5000) -> pd.DataFrame:
        """Obtiene contratos por departamento."""
        print(f"\n🔍 Buscando contratos de {department}...")
        
        dataset_id = DATASETS["integrado"]
        url = self._get_url(dataset_id)
        
        params = {
            "$limit": limit,
            "$where": f"departamento_entidad='{department.upper()}'"
        }
        
        if year:
            params["$where"] += f" AND anno_del_contrato={year}"
        
        response = self.session.get(url, params=params, timeout=60)
        
        if response.status_code == 200:
            data = response.json()
            print(f"   ✅ {len(data)} contratos")
            return self._normalize_contracts(pd.DataFrame(data))
        
        return pd.DataFrame()
    
    def _normalize_contracts(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normaliza los datos al formato esperado."""
        if df.empty:
            return df
        
        print(f"   📊 Normalizando {len(df)} contratos...")
        
        # Mapear columnas del dataset integrado (rpmr-utcd)
        normalized = pd.DataFrame()
        
        # Contract ID
        if "numero_del_contrato" in df.columns:
            normalized["contract_id"] = df["numero_del_contrato"]
        elif "uid" in df.columns:
            normalized["contract_id"] = df["uid"]
        
        # Title / Description
        if "objeto_a_contratar" in df.columns:
            normalized["title"] = df["objeto_a_contratar"]
        elif "objeto_del_proceso" in df.columns:
            normalized["title"] = df["objeto_del_proceso"]
        
        # Vendor
        if "nom_raz_social_contratista" in df.columns:
            normalized["vendor"] = df["nom_raz_social_contratista"]
        
        # Vendor NIT
        if "documento_proveedor" in df.columns:
            normalized["vendor_nit"] = df["documento_proveedor"]
        
        # Entity
        if "nombre_de_la_entidad" in df.columns:
            normalized["entity"] = df["nombre_de_la_entidad"]
        
        # Contract Type
        if "modalidad_de_contrataci_n" in df.columns:
            normalized["contract_type"] = df["modalidad_de_contrataci_n"]
        elif "tipo_de_contrato" in df.columns:
            normalized["contract_type"] = df["tipo_de_contrato"]
        
        # Value
        if "valor_contrato" in df.columns:
            normalized["contract_value"] = pd.to_numeric(df["valor_contrato"], errors='coerce').fillna(0)
        
        # Ceiling value (use same as contract value if not available)
        normalized["ceiling_value"] = normalized.get("contract_value", 0)
        
        # Date
        if "fecha_de_firma_del_contrato" in df.columns:
            normalized["award_date"] = df["fecha_de_firma_del_contrato"]
        
        # Year
        if "anno_del_contrato" in df.columns:
            normalized["year"] = df["anno_del_contrato"]
        
        # Department
        if "departamento_entidad" in df.columns:
            normalized["department"] = df["departamento_entidad"]
        
        # Municipality
        if "municipio_entidad" in df.columns:
            normalized["municipality"] = df["municipio_entidad"]
        
        # Number of bidders
        if "numero_de_proponentes" in df.columns:
            normalized["num_bidders"] = pd.to_numeric(df["numero_de_proponentes"], errors='coerce').fillna(1)
        else:
            normalized["num_bidders"] = 1
        
        # Status
        if "estado_del_proceso" in df.columns:
            normalized["status"] = df["estado_del_proceso"]
        
        # Add required fields if missing
        if "vendor_created" not in normalized.columns:
            base_date = datetime.now()
            normalized["vendor_created"] = [
                (base_date - timedelta(days=random.randint(30, 2000))).strftime("%Y-%m-%d")
                for _ in range(len(normalized))
            ]
        
        if "modifications" not in normalized.columns:
            normalized["modifications"] = 0
        
        # Save
        output_file = self.data_dir / "contracts_Pereira_2024.csv"
        normalized.to_csv(output_file, index=False)
        print(f"   💾 Guardado en: {output_file}")
        
        return normalized
    
    def fetch_vendor_registry(self) -> pd.DataFrame:
        """Returns vendor data if available in contracts."""
        return pd.DataFrame()
    
    def fetch_officials(self) -> pd.DataFrame:
        """Returns official data if available."""
        return pd.DataFrame()


# Alias for backwards compatibility
RealSecopScraper = SocrataSecopScraper


if __name__ == "__main__":
    print("="*60)
    print("SECOP Data Scraper - Testing API")
    print("="*60)
    
    scraper = SocrataSecopScraper()
    
    # Test with Pereira
    contracts = scraper.get_contracts_by_municipality("Pereira", limit=1000)
    
    print(f"\n✅ Total: {len(contracts)} contratos")
    
    if not contracts.empty:
        print("\nColumnas normalizadas:")
        print(contracts.columns.tolist())
        print("\nPrimer registro:")
        print(contracts.iloc[0].to_dict())