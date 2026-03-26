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
import random

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


class SecopScraper:
    """
    Mock SECOP Scraper - genera datos de prueba.
    Usa esto si no puedes obtener datos reales de SECOP.
    """
    
    VENDOR_NAMES = [
        "CONSTRUCTORA PEREIRA S.A.S.",
        "SERVICIOS INTEGRALES LTDA",
        "INGENIERÍA Y DISEÑO S.A.S.",
        "CONSULTORÍA COLOMBIA S.A.",
        "SUPPLY CHAIN SOLUTIONS S.A.S.",
        "TECHNOLOGY PARTNERS S.A.",
        "MULTISERVICIOS PROFESIONALES S.A.S.",
        "ASESORÍAS Y CONSULTORÍAS DEL EJE CAFETERO",
        "CONSTRUCCIONES Y REMODELACIONES PEREIRA",
        "SERVICIOS ESPECIALIZADOS DE RISARALDA",
    ]
    
    OFFICIAL_NAMES = [
        "Juan Carlos Ramírez",
        "María Elena González",
        "Carlos Alberto López",
        "Ana Patricia Díaz",
        "Roberto Carlos Mendoza",
    ]
    
    CONTRACT_TYPES = [
        "seleccion_abreviada",
        "contratacion_directa",
        "licitacion_publica",
        "minima_cuantia",
        "concurso_de_mritos"
    ]
    
    SUSPICIOUS_KEYWORDS = [
        "consultoría", "asesoría", "estudios", "diseño", "interventoría",
        "seguimiento", "evaluación", "auditoría", "soporte técnico"
    ]
    
    def __init__(self, data_dir: str = "data/raw"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
    
    def fetch_contracts(self, municipality: str = "Pereira", year: int = 2024, n: int = 500) -> pd.DataFrame:
        """Genera contratos mock con patrones realistas."""
        import random
        from datetime import datetime, timedelta
        
        random.seed(42)
        
        contracts = []
        base_date = datetime(year, 1, 1)
        
        for i in range(n):
            # Some contracts are suspicious
            is_suspicious = random.random() < 0.2
            
            contract = {
                "contract_id": f"CONTRATO-{year}-{i+1:05d}",
                "title": random.choice([
                    f"Contratación de {random.choice(self.SUSPICIOUS_KEYWORDS)} para {municipality}",
                    f"Servicios de {random.choice(self.SUSPICIOUS_KEYWORDS)}",
                    f"Obra pública para pavimentación calle {random.randint(1, 50)}",
                    f"Mantenimiento de espacios públicos {municipality}",
                ]),
                "vendor": random.choice(self.VENDOR_NAMES),
                "vendor_nit": f"{random.randint(800000000, 900000000)}-{random.randint(0, 9)}",
                "vendor_created": (base_date - timedelta(days=random.randint(30, 2000))).strftime("%Y-%m-%d"),
                "entity": f"Alcaldía de {municipality}",
                "contract_type": random.choice(self.CONTRACT_TYPES),
                "contract_value": random.randint(10_000_000, 500_000_000),
                "ceiling_value": 0,
                "award_date": (base_date + timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d"),
                "num_bidders": 1 if is_suspicious else random.randint(2, 8),
                "modifications": random.randint(0, 5) if is_suspicious else random.randint(0, 1),
                "municipality": municipality,
                "year": year,
            }
            
            # Set ceiling based on contract value
            contract["ceiling_value"] = int(contract["contract_value"] * random.uniform(1.0, 1.15))
            
            contracts.append(contract)
        
        df = pd.DataFrame(contracts)
        
        # Save
        df.to_csv(self.data_dir / f"contracts_{municipality}_{year}.csv", index=False)
        
        return df
    
    def fetch_vendor_registry(self) -> pd.DataFrame:
        """Genera datos mock de vendors."""
        import random
        from datetime import datetime, timedelta
        
        vendors = []
        base_date = datetime.now()
        
        for i, name in enumerate(self.VENDOR_NAMES):
            # Some vendors are new (suspicious)
            age_days = random.randint(30, 2000)  # 30 days to 5+ years
            vendors.append({
                "vendor_id": f"V{i+1}",
                "name": name,
                "nit": f"{random.randint(800000000, 900000000)}-{random.randint(0, 9)}",
                "registration_date": (base_date - timedelta(days=age_days)).strftime("%Y-%m-%d"),
                "status": "active"
            })
        
        return pd.DataFrame(vendors)
    
    def fetch_officials(self) -> pd.DataFrame:
        """Genera datos mock de funcionarios."""
        officials = []
        for i, name in enumerate(self.OFFICIAL_NAMES):
            officials.append({
                "official_id": f"O{i+1}",
                "name": name,
                "position": random.choice(["Director de Contratación", "Secretario de Infraestructura", "Gerente de Proyectos"]),
                "entity": "Alcaldía de Pereira"
            })
        
        return pd.DataFrame(officials)


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