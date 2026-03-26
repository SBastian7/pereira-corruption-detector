"""
Descargador de datos desde el portal público de SECOP
Método alternativo: descargar archivos CSV/Excel del portal público.
"""

import requests
import pandas as pd
from pathlib import Path
import time

# URLs del portal público de datos de SECOP
SECOP_OPEN_DATA = {
    "contratos_2024": "https://www.colombiacompra.gov.co/sites/default/files/secretaria-transparencia/contratos_2024.csv",
    "contratos_2023": "https://www.colombiacompra.gov.co/sites/default/files/secretaria-transparencia/contratos_2023.csv",
    # Añadir más años según disponibilidad
}


class SecopDataDownloader:
    """Descarga datos desde el portal abierto de SECOP."""
    
    def __init__(self, data_dir: str = "data/raw"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Pereira-Corruption-Detector/1.0"
        })
    
    def download_year(self, year: int = 2024) -> pd.DataFrame:
        """Descarga contratos de un año específico."""
        
        url = f"https://www.colombiacompra.gov.co/sites/default/files/secretaria-transparencia/contratos_{year}.csv"
        
        print(f"📥 Descargando {url}...")
        
        try:
            response = self.session.get(url, timeout=120)
            response.raise_for_status()
            
            # Guardar raw
            filepath = self.data_dir / f"secop_{year}.csv"
            with open(filepath, 'wb') as f:
                f.write(response.content)
            
            # Leer como DataFrame
            df = pd.read_csv(filepath, encoding='utf-8', on_bad_lines='skip')
            
            # Filtrar por Pereira
            df_pereira = self._filter_by_municipality(df, "Pereira")
            
            # Guardar filtrado
            filtered_path = self.data_dir / f"contracts_Pereira_{year}.csv"
            df_pereira.to_csv(filtered_path, index=False)
            
            print(f"✅ {len(df_pereira)} contratos de Pereira guardados en {filtered_path}")
            return df_pereira
            
        except Exception as e:
            print(f"❌ Error descargando: {e}")
            return pd.DataFrame()
    
    def _filter_by_municipality(self, df: pd.DataFrame, municipality: str) -> pd.DataFrame:
        """Filtra contratos por nombre de entidad (municipio)."""
        
        # Buscar columna de nombre de entidad
        entity_cols = [c for c in df.columns if 'entidad' in c.lower() or 'entity' in c.lower()]
        
        if not entity_cols:
            print("⚠️ No se encontró columna de entidad")
            return df
        
        col = entity_cols[0]
        
        # Filtrar (contiene el nombre, case insensitive)
        mask = df[col].str.contains(municipality, case=False, na=False)
        
        print(f"   Filtrados {mask.sum()} contratos de {len(df)} totales para {municipality}")
        
        return df[mask].copy()
    
    def download_all_years(self, start_year: int = 2019, end_year: int = 2024) -> pd.DataFrame:
        """Descarga todos los años disponibles."""
        
        all_contracts = []
        
        for year in range(start_year, end_year + 1):
            print(f"\n📅 Procesando año {year}...")
            df = self.download_year(year)
            
            if not df.empty:
                all_contracts.append(df)
            
            time.sleep(1)  # Rate limiting
        
        if all_contracts:
            combined = pd.concat(all_contracts, ignore_index=True)
            combined.to_csv(self.data_dir / "contracts_Pereira_all.csv", index=False)
            print(f"\n✅ Total: {len(combined)} contratos")
            return combined
        
        return pd.DataFrame()


if __name__ == "__main__":
    downloader = SecopDataDownloader()
    
    # Opción 1: descargar un año específico
    # df = downloader.download_year(2024)
    
    # Opción 2: descargar todos los años
    df = downloader.download_all_years(2020, 2024)
    print(df.head())