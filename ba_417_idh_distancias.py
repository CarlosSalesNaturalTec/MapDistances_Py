#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gera CSV com 417 municípios da Bahia contendo:
1) Nome do município
2) Código IBGE (7 dígitos)
3) IDHM 2010
4) Distância geodésica (km) até Salvador
5) Distância rodoviária (km) até Salvador (principais rodovias via OSRM)

Resumo: 
Na primeira execução, o script faz requisições para obter dados de fontes externas 
(API do IBGE, Wikipedia, Nominatim, OSRM) e armazena os resultados em arquivos de cache 
(geocode.json, route.json, idhm2010.json) dentro do diretório .cache_ba.

Nas execuções subsequentes, antes de fazer qualquer requisição, 
o script verifica se os dados já estão presentes nos arquivos de cache. 
Se estiverem, ele os carrega do cache, evitando a necessidade de fazer requisições repetidas.
Isso acelera o processo e reduz a carga sobre os serviços externos.

Fontes:
- Lista de municípios e códigos IBGE: API IBGE Localidades
  https://servicodados.ibge.gov.br/api/v1/localidades/estados/29/municipios
- IDHM 2010: Wikipedia - "Lista de municípios da Bahia por IDH-M"
  (lido via pandas.read_html; se a estrutura mudar, ajuste o seletor de tabela)
- Geocodificação: Nominatim (OpenStreetMap)
- Roteamento: OSRM público (router.project-osrm.org) = Serviço público gratuito que usa OSRM para cálculos de rota via API.

Requisitos:

  pip install pandas requests beautifulsoup4 tqdm lxml

Uso:
  python ba_417_idh_distancias.py --out distancias_bahia.csv

Dicas:
- O script usa cache (JSON) para geocodificação e rotas, para evitar repetir chamadas.
- Respeite as políticas de uso de Nominatim/OSRM (delays inseridos).

Observação:
- "Distância rodoviária" é uma estimativa baseada no grafo do OSRM público e pode
  variar de acordo com origem/destino exatos e mudanças na malha viária.
"""

import argparse
import json
import math
import os
import re
import time
from typing import Dict, Optional, Tuple
from io import StringIO

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

# ---------- Configurações ----------
HEADERS = {
    "User-Agent": "bahia-rotas-idh/1.0 (contato: carlos.souza@serin.ba.gov.br)"
}
UF_BA = 29
API_MUNICIPIOS = f"https://servicodados.ibge.gov.br/api/v1/localidades/estados/{UF_BA}/municipios"
WIKI_IDHM = "https://pt.wikipedia.org/wiki/Lista_de_munic%C3%ADpios_da_Bahia_por_IDH-M"
NOMINATIM = "https://nominatim.openstreetmap.org/search"
OSRM_ROUTE = "https://router.project-osrm.org/route/v1/driving/{olon},{olat};{dlon},{dlat}?overview=false&alternatives=false"

# Delays para cortesia com os serviços
SLEEP_NOMINATIM = 1.5
SLEEP_OSRM = 0.8

# Arquivos de cache
CACHE_DIR = ".cache_ba"
GEOCODE_CACHE = os.path.join(CACHE_DIR, "geocode.json")
ROUTE_CACHE = os.path.join(CACHE_DIR, "route.json")
IDHM_CACHE = os.path.join(CACHE_DIR, "idhm2010.json")
MUNICIPIOS_CACHE = os.path.join(CACHE_DIR, "municipios.json")

# ---------- Utils ----------
def ensure_cache_dir():
    os.makedirs(CACHE_DIR, exist_ok=True)

def save_json(path: str, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def load_json(path: str) -> Dict:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def normalize_name(name: str) -> str:
    """Normaliza strings para matching (remove acentos, caixa, espaços extras)."""
    import unicodedata
    s = name.strip().lower()
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
    s = re.sub(r"\s+", " ", s)
    return s

def haversine_km(lat1, lon1, lat2, lon2) -> float:
    """Distância geodésica aproximada (haversine) em km."""
    R = 6371.0088  # raio médio da Terra em km
    phi1 = math.radians(lat1); phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    c = 2*math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

# ---------- Coleta IBGE ----------
def get_municipios_ibge() -> pd.DataFrame:
    """Retorna DataFrame com colunas: municipio, codigo_ibge. Usa cache."""
    ensure_cache_dir()
    
    if os.path.exists(MUNICIPIOS_CACHE):
        rows = load_json(MUNICIPIOS_CACHE)
    else:
        print("Cache de municípios não encontrado. Buscando na API do IBGE...")
        try:
            r = requests.get(API_MUNICIPIOS, headers=HEADERS, timeout=60)
            r.raise_for_status()
            j = r.json()
            rows = [{"municipio": item["nome"], "codigo_ibge": int(item["id"])} for item in j]
            save_json(MUNICIPIOS_CACHE, rows)
        except requests.RequestException as e:
            print(f"Erro CRÍTICO ao buscar lista de municípios do IBGE: {e}")
            print("Não é possível continuar sem a lista de municípios. Verifique sua conexão e tente novamente.")
            exit(1)

    df = pd.DataFrame(rows)
    # Ordena alfabeticamente (ignorar acentos/caixa)
    df["nome_key"] = df["municipio"].map(normalize_name)
    df = df.sort_values("nome_key").drop(columns=["nome_key"]).reset_index(drop=True)
    return df

# ---------- IDHM 2010 ----------
def get_idhm_2010() -> Dict[str, float]:
    """Retorna dict {nome_normalizado: idhm_2010} usando cache; fonte Wikipedia."""
    ensure_cache_dir()
    cache = load_json(IDHM_CACHE)
    if cache:
        return {k: float(v) for k, v in cache.items()}

    # Tenta ler tabelas da página
    resp = requests.get(WIKI_IDHM, headers=HEADERS, timeout=60)
    resp.raise_for_status()
    tables = pd.read_html(StringIO(resp.text))
    # Encontrar tabela que contenha colunas Município e IDH
    idh_map: Dict[str, float] = {}
    found_any = False
    for tbl in tables:
        cols = [normalize_name(str(c)) for c in tbl.columns]
        if any("munic" in c for c in cols) and any("idh" in c for c in cols):
            # Tenta identificar as colunas
            try:
                # Procura a primeira coluna que tenha "munic"
                col_mun = next(c for c in tbl.columns if "munic" in normalize_name(str(c)))
            except StopIteration:
                continue
            # Procura a 1ª coluna com "idh"
            try:
                col_idh = next(c for c in tbl.columns if "idh" in normalize_name(str(c)))
            except StopIteration:
                continue

            for _, row in tbl[[col_mun, col_idh]].dropna().iterrows():
                nome = str(row[col_mun])
                # limpa notas/rodapés
                nome = re.sub(r"\[.*?\]", "", nome).strip()
                idh_txt = str(row[col_idh]).strip().replace(",", ".")
                try:
                    idh_val = float(re.sub(r"[^\d\.]", "", idh_txt))
                except Exception:
                    continue
                key = normalize_name(nome)
                idh_map[key] = idh_val
            found_any = True

    if not found_any or not idh_map:
        raise RuntimeError("Não foi possível extrair tabela de IDHM 2010 do Wikipedia.")

    save_json(IDHM_CACHE, idh_map)
    return idh_map

# ---------- Geocodificação ----------
def geocode(query: str) -> Optional[Tuple[float, float]]:
    """Geocodifica com Nominatim (lat, lon)."""
    params = {"q": query, "format": "jsonv2", "limit": 1, "addressdetails": 0}
    r = requests.get(NOMINATIM, params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()
    j = r.json()
    if not j:
        return None
    return float(j[0]["lat"]), float(j[0]["lon"])

def geocode_municipio(nome: str) -> Optional[Tuple[float, float]]:
    """Tenta geocodificar a sede municipal de forma robusta."""
    # 1) Prefeitura Municipal
    q1 = f"Prefeitura Municipal de {nome}, Bahia, Brasil"
    coords = geocode(q1)
    if coords:
        return coords
    time.sleep(SLEEP_NOMINATIM)
    # 2) Município
    q2 = f"{nome}, Bahia, Brasil"
    coords = geocode(q2)
    if coords:
        return coords
    return None

def get_salvador_coords() -> Tuple[float, float]:
    """Geocodifica Salvador de forma robusta."""
    # Reutiliza a lógica de geocodificação de município que já tem fallbacks
    # (tenta 'Prefeitura Municipal de Salvador' e depois 'Salvador').
    coords = geocode_municipio("Salvador")
    if not coords:
        raise RuntimeError("Falha ao geocodificar Salvador. Verifique a conexão e a API do Nominatim.")
    return coords

# ---------- OSRM (rota rodoviária) ----------
def osrm_distance_km(orig: Tuple[float, float], dest: Tuple[float, float]) -> Optional[float]:
    olat, olon = orig
    dlat, dlon = dest
    url = OSRM_ROUTE.format(olat=olat, olon=olon, dlat=dlat, dlon=dlon)
    r = requests.get(url, headers=HEADERS, timeout=60)
    if r.status_code != 200:
        return None
    j = r.json()
    routes = j.get("routes")
    if not routes:
        return None
    meters = routes[0].get("distance")
    if meters is None:
        return None
    return meters / 1000.0

# ---------- Pipeline principal ----------
def main():
    parser = argparse.ArgumentParser(description="Gera CSV com IDHM 2010, distância geodésica e rodoviária até Salvador para os 417 municípios da Bahia.")
    parser.add_argument("--out", default="distancias_bahia.csv", help="Caminho do CSV de saída (default: distancias_bahia.csv)")
    parser.add_argument("--resume", action="store_true", help="Se existir CSV parcial, reprocessa apenas linhas com valores ausentes.")
    parser.add_argument("--no-osrm", action="store_true", help="Pula o cálculo de distância rodoviária (apenas geodésica).")
    args = parser.parse_args()

    ensure_cache_dir()
    geocode_cache = load_json(GEOCODE_CACHE)
    route_cache = load_json(ROUTE_CACHE)

    print("1) Coletando municípios e códigos IBGE (IBGE Localidades)...")
    df = get_municipios_ibge()  # municipio, codigo_ibge

    print("2) Carregando IDHM 2010 (Wikipedia)...")
    idh_map = get_idhm_2010()

    # Prepara DataFrame
    df["idhm_2010"] = df["municipio"].map(lambda n: idh_map.get(normalize_name(n), None))

    # Geocodificação de Salvador (origem)
    print("3) Geocodificando Salvador (origem)...")
    key_salvador = normalize_name("Salvador")
    if key_salvador in geocode_cache:
        lat_s, lon_s = geocode_cache[key_salvador]
    else:
        lat_s, lon_s = get_salvador_coords()
        geocode_cache[key_salvador] = [lat_s, lon_s]
        save_json(GEOCODE_CACHE, geocode_cache)
    time.sleep(SLEEP_NOMINATIM)

    # Colunas de saída
    df["dist_km_geodesica_salvador"] = None
    df["dist_km_rodoviaria_salvador"] = None
    df["lat"] = None
    df["lon"] = None

    print("4) Geocodificando sedes municipais e calculando distâncias...")
    it = tqdm(df.itertuples(index=False), total=len(df))
    for row in it:
        nome = getattr(row, "municipio")
        key = normalize_name(nome)

        # Geocodificação (cache → prefeitura → cidade)
        if key in geocode_cache:
            lat, lon = geocode_cache[key]
        else:
            coords = geocode_municipio(nome)
            if coords:
                lat, lon = coords
                geocode_cache[key] = [lat, lon]
                save_json(GEOCODE_CACHE, geocode_cache)
            else:
                lat, lon = None, None
        time.sleep(SLEEP_NOMINATIM)

        # Atualiza DF (coords e geodésica)
        if lat is not None and lon is not None:
            df.loc[df["municipio"] == nome, ["lat", "lon"]] = [lat, lon]
            geo_km = haversine_km(lat_s, lon_s, lat, lon)
            df.loc[df["municipio"] == nome, "dist_km_geodesica_salvador"] = round(geo_km, 1)
        else:
            df.loc[df["municipio"] == nome, "dist_km_geodesica_salvador"] = None

        # Distância rodoviária (OSRM)
        if not args.no_osrm and lat is not None and lon is not None:
            route_key = f"{lat_s:.6f},{lon_s:.6f}->{lat:.6f},{lon:.6f}"
            if route_key in route_cache:
                rod_km = route_cache[route_key]
            else:
                rod_km = osrm_distance_km((lat_s, lon_s), (lat, lon))
                route_cache[route_key] = None if rod_km is None else float(rod_km)
                save_json(ROUTE_CACHE, route_cache)
            time.sleep(SLEEP_OSRM)
            if rod_km is not None:
                df.loc[df["municipio"] == nome, "dist_km_rodoviaria_salvador"] = round(rod_km, 1)

        it.set_description(f"Processando: {nome}")

    # Ordena alfabeticamente para saída
    df["nome_key"] = df["municipio"].map(normalize_name)
    df_out = df.sort_values("nome_key").drop(columns=["nome_key"])

    # Seleciona colunas finais
    df_out = df_out[[
        "municipio",
        "codigo_ibge",
        "idhm_2010",
        "dist_km_geodesica_salvador",
        "dist_km_rodoviaria_salvador"
    ]]

    # Exporta
    out_path = args.out
    df_out.to_csv(out_path, index=False, encoding="utf-8")
    print(f"\nConcluído! CSV gerado em: {out_path}")
    print("Colunas: municipio,codigo_ibge,idhm_2010,dist_km_geodesica_salvador,dist_km_rodoviaria_salvador")

if __name__ == "__main__":
    main()
