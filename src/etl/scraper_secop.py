"""
SECOP Real Data Scraper
Conecta a las APIs públicas de SECOP II para obtener contratos reales.
"""

import requests
import pandas as pd
from datetime import datetime
from pathlib import Path
import time
import json

# Endpoints públicos de SECOP II
SECOP_BASE_URL = "https://www.colombiacompra.gov.co/secop/api/v2"
SECOP_CONTRACTS_URL = f"{SECOP_BASE_URL}/contracts"
SECOP_ENTITIES_URL = f"{SECOP_BASE_URL}/entities"


class RealSecopScraper:
    """Scraper para datos reales de SECOP II."""
    
    def __init__(self, data_dir: str = "data/raw"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Pereira-Corruption-Detector/1.0",
            "Accept": "application/json"
        })
    
    def get_contracts_by_municipality(self, municipality: str = "Pereira", 
                                       year: int = 2024, 
                                       limit: int = 1000) -> pd.DataFrame:
        """
        Obtiene contratos reales filtrados por municipio.
        
        SECOP II permite filtros por:
        - Nombre de entidad (municipio)
        - Año
        - Tipo de proceso
        """
        
        contracts = []
        offset = 0
        page_size = 100
        
        print(f"🔍 Descargando contratos de {municipality} ({year})...")
        
        while offset < limit:
            try:
                # Parámetros de la API (depende del endpoint específico)
                params = {
                    "entityName": municipality,
                    "year": year,
                    "offset": offset,
                    "limit": page_size,
                    "sortBy": "awardDate",
                    "sortOrder": "desc"
                }
                
                response = self.session.get(SECOP_CONTRACTS_URL, params=params, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if "contracts" in data:
                        contracts.extend(data["contracts"])
                    elif isinstance(data, list):
                        contracts.extend(data)
                    else:
                        # Estructura alternativa
                        break
                    
                    print(f"   Descargados: {len(contracts)} contratos")
                    
                    # Si no hay más datos, salir
                    if len(contracts) < page_size:
                        break
                    
                    offset += page_size
                    time.sleep(0.5)  # Rate limiting
                    
                elif response.status_code == 429:
                    # Rate limited - esperar más
                    print("   ⏳ Rate limited, esperando...")
                    time.sleep(5)
                else:
                    print(f"   ⚠️ Error {response.status_code}: {response.text[:200]}")
                    break
                    
            except Exception as e:
                print(f"   ❌ Error: {e}")
                break
        
        if not contracts:
            print("⚠️ No se obtuvo data del API. Usando fallback...")
            return self._get_mock_contracts(municipality, year)
        
        # Convertir a DataFrame y normalizar
        df = self._normalize_contracts(contracts)
        
        # Guardar
        filename = f"contracts_{municipality}_{year}_real.csv"
        df.to_csv(self.data_dir / filename, index=False)
        print(f"✅ Guardados {len(df)} contratos reales en {filename}")
        
        return df
    
    def _normalize_contracts(self, contracts: list) -> pd.DataFrame:
        """Normaliza los datos del API al formato esperado."""
        
        normalized = []
        
        for c in contracts:
            try:
                # Mapear campos del API de SECOP al formato interno
                normalized.append({
                    "contract_id": c.get("contractId") or c.get("id") or c.get("nit") or "N/A",
                    "title": c.get("description") or c.get("title") or c.get("object") or "",
                    "vendor": c.get("provider") or c.get("supplier") or c.get("vendorName") or "",
                    "vendor_nit": c.get("providerNit") or c.get("supplierNit") or c.get("nit") or "",
                    "vendor_created": c.get("providerCreatedDate", ""),
                    "contract_value": self._parse_value(c.get("value") or c.get("amount") or c.get("contractValue")) ,
                    "ceiling_value": self._parse_value(c.get("budget") or c.get("ceilingValue") or c.get("topValue")),
                    "num_bidders": c.get("biddersCount") or c.get("numberOfBids") or c.get("numBidders") or 1,
                    "award_date": c.get("awardDate") or c.get("date") or c.get("awardDate") or "",
                    "contractor_name": c.get("contractorName") or c.get("contractor") or "",
                    "contractor_id": c.get("contractorId") or "",
                    "contract_type": c.get("processType") or c.get("contractType") or c.get("type") or "unknown",
                    "modifications": c.get("modifications") or c.get("changeOrders") or 0,
                    "entity_name": c.get("entityName") or c.get("entity") or "",
                })
            except Exception as e:
                print(f"   ⚠️ Error normalizando contrato: {e}")
                continue
        
        df = pd.DataFrame(normalized)
        
        # Limpiar valores nulos
        df = df.fillna("")
        
        return df
    
    def _parse_value(self, value):
        """Convierte valores al formato numérico."""
        if not value:
            return 0
        if isinstance(value, (int, float)):
            return value
        # Limpiar strings como "$500.000.000" o "500000000"
        try:
            clean = str(value).replace("$", "").replace(".", "").replace(",", "")
            return int(clean)
        except:
            return 0
    
    def _get_mock_contracts(self, municipality: str, year: int) -> pd.DataFrame:
        """Fallback a datos mock si el API no responde."""
        print("📦 Usando datos de prueba (mock)...")
        return self._get_mock_data(municipality, year)
    
    def _get_mock_data(self, municipality: str, year: int) -> pd.DataFrame:
        """Genera datos de prueba más realistas."""
        import random
        
        vendors = [
            ("900123456-1", "Constructora XYZ SAS", "2015-03-15"),
            ("800555555-5", "Suministros ABC Ltda", "2005-06-20"),
            ("901111111-1", "Urbanistas Asociados SAS", "2024-01-05"),
            ("900777777-7", "Servicios Integrales de Colombia", "2023-08-10"),
            ("800333333-3", "Consultores del Eje SAS", "2022-11-01"),
            ("901555555-5", "Ingeniería y Gestión S.A.S", "2024-02-15"),
            ("900888888-8", "Cooperativa de Trabajo Asociado", "2019-05-20"),
        ]
        
        contract_types = ["contratacion_directa", "licitacion_publica", "seleccion_abreviada", "minima_cuantia"]
        
        contracts = []
        for i in range(50):
            vendor = random.choice(vendors)
            contract_type = random.choice(contract_types)
            value = random.randint(10_000_000, 800_000_000)
            ceiling = int(value * random.uniform(1.0, 1.15))
            num_bidders = 1 if contract_type == "contratacion_directa" else random.randint(2, 8)
            
            contracts.append({
                "contract_id": f"CON-{year}-{i+1:04d}",
                "title": random.choice([
                    "Mantenimiento de vías urbanas",
                    "Suministro de elementos de oficina",
                    "Consultoría para planeación urbana",
                    "Obra pública - Construcción puente",
                    "Servicios de aseo y cafetería",
                    "Estudios y diseños infraestructura",
                    "Interventoría de proyectos",
                    "Apoyo técnico administrativo",
                ]),
                "vendor": vendor[1],
                "vendor_nit": vendor[0],
                "vendor_created": vendor[2],
                "contract_value": value,
                "ceiling_value": ceiling,
                "num_bidders": num_bidders,
                "award_date": f"{year}-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
                "contractor_name": random.choice(["Juan Pérez", "María González", "Carlos López", "Ana Martínez"]),
                "contractor_id": f"{random.randint(10000000,99999999)}",
                "contract_type": contract_type,
                "modifications": random.randint(0, 3) if random.random() > 0.7 else 0,
                "entity_name": municipality,
            })
        
        df = pd.DataFrame(contracts)
        df.to_csv(self.data_dir / f"contracts_{municipality}_{year}.csv", index=False)
        return df
    
    def get_entity_info(self, entity_name: str = "Pereira") -> dict:
        """Obtiene información de la entidad (municipio)."""
        
        try:
            url = f"{SECOP_ENTITIES_URL}/{entity_name}"
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                return response.json()
        except Exception as e:
            print(f"⚠️ Error obteniendo entidad: {e}")
        
        return {"name": entity_name, "status": "unknown"}


class RUESClient:
    """Cliente para el Registro Único Empresarial (RUES)."""
    
    def __init__(self):
        self.base_url = "https://www.rues.org.co/api"
        self.session = requests.Session()
    
    def get_company_info(self, nit: str) -> dict:
        """
        Obtiene información de una empresa por NIT.
        NOTA: El API público tiene limitaciones.
        """
        try:
            # Intentar endpoint público
            response = self.session.get(
                f"{self.base_url}/company/{nit}",
                timeout=10
            )
            
            if response.status_code == 200:
                return response.json()
        except Exception as e:
            print(f"⚠️ Error consultando RUES: {e}")
        
        # Fallback: retornar info básica
        return {
            "nit": nit,
            "status": "consulta_no_disponible",
            "message": "RUES API requiere autenticación"
        }
    
    def search_company(self, name: str) -> list:
        """Buscar empresas por nombre."""
        try:
            response = self.session.get(
                f"{self.base_url}/search",
                params={"q": name},
                timeout=10
            )
            if response.status_code == 200:
                return response.json()
        except:
            pass
        return []


if __name__ == "__main__":
    # Test: descargar contratos reales
    scraper = RealSecopScraper()
    contracts = scraper.get_contracts_by_municipality("Pereira", 2024, limit=500)
    print(f"\n📊 Total contratos obtenidos: {len(contracts)}")
    print(contracts.head())