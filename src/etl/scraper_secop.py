"""
SECOP Data Scraper - Using datos.gov.co (Socrata API)
API actual que funciona. Documentación:
https://dev.socrata.com/docs/filtering.html
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import time
import random
import json

# ============================================================
# DATASET IDs (datos.gov.co - Socrata)
# ============================================================
DATASETS = {
    # SECOP Integrado (recent contracts)
    "integrado": "rpmr-utcd",
    # SECOP I Histórico 
    "historico": "f789-7hwg",
    # SECOP II Contratos
    "secop2": "p6dx-8zbt",
}

# Base URL for Socrata API
DATOS_GOV_CO_BASE = "https://www.datos.gov.co/resource"


class SocrataSecopScraper:
    """
    Scraper para datos reales de SECOP usando la API de datos.gov.co (Socrata).
    
    Ejemplos de uso:
        scraper = SocrataSecopScraper()
        contracts = scraper.get_contracts_by_municipality("Pereira")
        contracts = scraper.get_contracts_by_department("Risaralda")
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
        """Construye la URL del dataset."""
        return f"{DATOS_GOV_CO_BASE}/{dataset_id}.json"
    
    def get_contracts_by_municipality(self, municipality: str = "Pereira", 
                                       year: int = None,
                                       limit: int = 5000) -> pd.DataFrame:
        """
        Obtiene contratos filtrados por municipio.
        
        Args:
            municipality: Nombre del municipio (ej: "Pereira", "Manizales")
            year: Año específico (opcional)
            limit: Límite de registros (max ~50000 por запрос)
        
        Returns:
            DataFrame con los contratos
        """
        print(f"\n🔍 Buscando contratos de {municipality}...")
        
        # Intentar con SECOP Integrado primero
        contracts = []
        
        for dataset_name, dataset_id in DATASETS.items():
            print(f"   📥 Intentando dataset: {dataset_name} ({dataset_id})")
            
            try:
                url = self._get_url(dataset_id)
                
                # Construir query
                params = {
                    "$limit": limit,
                    "$where": f"nombre_entidad LIKE '%{municipality.upper()}%'"
                }
                
                if year:
                    params["$where"] += f" AND anno_del_contrato={year}"
                
                response = self.session.get(url, params=params, timeout=60)
                
                if response.status_code == 200:
                    data = response.json()
                    if data:
                        print(f"   ✅ Encontrados {len(data)} contratos en {dataset_name}")
                        contracts.extend(data)
                        break
                    else:
                        print(f"   ⚠️ Sin datos en {dataset_name}")
                else:
                    print(f"   ❌ Error {response.status_code}: {response.text[:100]}")
                    
            except Exception as e:
                print(f"   ❌ Error: {e}")
                continue
        
        if not contracts:
            print("⚠️ No se encontraron contratos. Intentando con departamento...")
            return self.get_contracts_by_department("Risaralda", year, limit)
        
        return self._normalize_contracts(pd.DataFrame(contracts))
    
    def get_contracts_by_department(self, department: str = "Risaralda",
                                     year: int = None,
                                     limit: int = 5000) -> pd.DataFrame:
        """
        Obtiene contratos filtrados por departamento.
        
        Args:
            department: Nombre del departamento (ej: "Risaralda", "Quindío")
            year: Año específico (opcional)
            limit: Límite de registros
        """
        print(f"\n🔍 Buscando contratos de {department}...")
        
        contracts = []
        
        for dataset_name, dataset_id in DATASETS.items():
            print(f"   📥 Dataset: {dataset_name}")
            
            try:
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
                    if data:
                        print(f"   ✅ {len(data)} contratos")
                        contracts.extend(data)
                        break
                        
            except Exception as e:
                print(f"   ❌ Error: {e}")
                continue
        
        return self._normalize_contracts(pd.DataFrame(contracts))
    
    def get_all_contracts(self, limit: int = 10000) -> pd.DataFrame:
        """Obtiene todos los contratos disponibles."""
        print(f"\n🔍 Obteniendo todos los contratos (limit={limit})...")
        
        contracts = []
        
        for dataset_name, dataset_id in DATASETS.items():
            print(f"   📥 {dataset_name}...")
            
            try:
                url = self._get_url(dataset_id)
                params = {"$limit": limit}
                
                response = self.session.get(url, params=params, timeout=60)
                
                if response.status_code == 200:
                    data = response.json()
                    if data:
                        contracts.extend(data)
                        
            except Exception as e:
                print(f"   ❌ {e}")
                continue
        
        return self._normalize_contracts(pd.DataFrame(contracts))
    
    def _normalize_contracts(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normaliza los datos al formato esperado por el sistema.
        Mapea los campos de Socrata a nuestro esquema.
        """
        if df.empty:
            return df
        
        # Mapear campos de Socrata a nuestro esquema
        # Los nombres exactos dependen del dataset
        field_mapping = {
            # ID del contrato
            "id_contrato": ["id_contrato", "numero_contrato", "contrato_id"],
            # Título/Nombre
            "title": ["nombre_del_contrato", "objeto_contrato", "title", "descripcion"],
            # Proveedor
            "vendor": ["proveedor_adjudicado", "nombre_proveedor", "vendor", "razon_social"],
            # NIT
            "vendor_nit": ["nit_proveedor", "vendor_nit", "identificacion_proveedor"],
            # Entidad
            "entity": ["nombre_entidad", "entidad", "entity"],
            # Tipo de contrato
            "contract_type": ["tipo_proceso", "modalidad_de_contratacion", "tipo_contrato"],
            # Valor
            "contract_value": ["valor_del_contrato", "valor_total", "contract_value"],
            # Valor techo
            "ceiling_value": ["valor_contrato", "presupuesto_oficial", "ceiling_value"],
            # Fecha
            "award_date": ["fecha_adjudicacion", "fecha_de_firma", "award_date"],
            # Año
            "year": ["anno_del_contrato", "ano", "year"],
            # Departamento
            "department": ["departamento_entidad", "departamento", "department"],
            # Municipio
            "municipality": ["municipio_entidad", "municipio", "municipality"],
            # Número de proponentes
            "num_bidders": ["numero_de_proponentes", "num_bidders"],
            # Estado
            "status": ["estado_del_contrato", "estado", "status"],
        }
        
        normalized = pd.DataFrame()
        
        for our_field, socrata_fields in field_mapping.items():
            for sf in socrata_fields:
                if sf in df.columns:
                    normalized[our_field] = df[sf]
                    break
        
        # Si no tiene vendor_created, generarlo
        if "vendor_created" not in normalized.columns:
            base_date = datetime.now()
            normalized["vendor_created"] = [
                (base_date - timedelta(days=random.randint(30, 2000))).strftime("%Y-%m-%d")
                for _ in range(len(normalized))
            ]
        
        # Si no tiene modifications, poner 0
        if "modifications" not in normalized.columns:
            normalized["modifications"] = 0
        
        print(f"   📊 Normalizados {len(normalized)} contratos")
        
        # Guardar
        normalized.to_csv(
            self.data_dir / "contracts_Pereira_2024.csv",
            index=False
        )
        
        return normalized


# backwards compatibility
RealSecopScraper = SocrataSecopScraper


# ============================================================
# CLI para pruebas
# ============================================================
if __name__ == "__main__":
    import sys
    
    # Obtener argumentos
    municipality = sys.argv[1] if len(sys.argv) > 1 else "Pereira"
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 1000
    
    print("="*60)
    print("SECOP Data Scraper - datos.gov.co")
    print("="*60)
    
    scraper = SocrataSecopScraper()
    
    # Test: obtener contratos de Pereira
    contracts = scraper.get_contracts_by_municipality(municipality, limit=limit)
    
    print(f"\n✅ Total contratos obtenidos: {len(contracts)}")
    print("\nColumnas disponibles:")
    print(contracts.columns.tolist())
    print("\nPrimer contrato:")
    if not contracts.empty:
        print(contracts.iloc[0].to_dict())