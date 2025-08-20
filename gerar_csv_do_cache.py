#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script auxiliar para gerar um CSV parcial usando apenas os dados
já salvos nos arquivos de cache no diretório .cache_ba.

Este script NÃO faz requisições de rede. Ele é útil para recuperar
o progresso de uma execução que foi interrompida.

Uso:
  python gerar_csv_do_cache.py
"""

import json
import math
import os
import re

import pandas as pd

# ---------- Configurações e Constantes ----------
CACHE_DIR = ".cache_ba"
GEOCODE_CACHE = os.path.join(CACHE_DIR, "geocode.json")
ROUTE_CACHE = os.path.join(CACHE_DIR, "route.json")
IDHM_CACHE = os.path.join(CACHE_DIR, "idhm2010.json")
MUNICIPIOS_CACHE = os.path.join(CACHE_DIR, "municipios.json")

# ---------- Funções Utilitárias (copiadas do script principal) ----------
def load_json(path: str) -> dict:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                print(f"Aviso: Arquivo de cache '{path}' está vazio ou corrompido.")
                return {}
    return {}

def normalize_name(name: str) -> str:
    import unicodedata
    s = name.strip().lower()
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
    s = re.sub(r"\s+", " ", s)
    return s

def haversine_km(lat1, lon1, lat2, lon2) -> float:
    R = 6371.0088
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    c = 2*math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def get_municipios_from_cache() -> pd.DataFrame:
    """Retorna a lista base de municípios do IBGE a partir do cache."""
    if not os.path.exists(MUNICIPIOS_CACHE):
        print(f"Erro CRÍTICO: O arquivo de cache de municípios '{MUNICIPIOS_CACHE}' não foi encontrado.")
        print("Execute o script principal 'ba_417_idh_distancias.py' pelo menos uma vez com conexão à internet para criar este arquivo.")
        exit(1)
    
    rows = load_json(MUNICIPIOS_CACHE)
    return pd.DataFrame(rows)

def main():
    print("--- Gerador de CSV a partir do Cache ---")

    # 1. Carregar todos os caches
    print(f"Lendo cache de geocodificação: {GEOCODE_CACHE}")
    geocode_cache = load_json(GEOCODE_CACHE)
    print(f"Lendo cache de rotas: {ROUTE_CACHE}")
    route_cache = load_json(ROUTE_CACHE)
    print(f"Lendo cache de IDHM: {IDHM_CACHE}")
    idhm_cache = load_json(IDHM_CACHE)

    if not geocode_cache and not route_cache and not idhm_cache:
        print("Nenhum arquivo de cache encontrado ou todos estão vazios. Nada a fazer.")
        return

    # 2. Obter a lista base de todos os 417 municípios
    print("Lendo lista de referência de municípios do cache...")
    df = get_municipios_from_cache()

    # 3. Obter coordenadas de Salvador (ponto de partida)
    key_salvador = normalize_name("Salvador")
    if key_salvador not in geocode_cache:
        print("Erro: Coordenadas de Salvador não encontradas no cache. Não é possível calcular distâncias.")
        return
    lat_s, lon_s = geocode_cache[key_salvador]

    # 4. Preencher o DataFrame com os dados do cache
    print("Processando dados do cache...")
    idhm_list, geo_dist_list, route_dist_list = [], [], []

    for _, row in df.iterrows():
        nome_municipio = row["municipio"]
        key = normalize_name(nome_municipio)

        # IDHM
        idhm_list.append(idhm_cache.get(key))

        # Coordenadas e Distâncias
        coords = geocode_cache.get(key)
        if coords:
            lat, lon = coords
            # Distância Geodésica
            geo_dist = round(haversine_km(lat_s, lon_s, lat, lon), 1)
            geo_dist_list.append(geo_dist)
            # Distância Rodoviária
            route_key = f"{lat_s:.6f},{lon_s:.6f}->{lat:.6f},{lon:.6f}"
            route_dist = route_cache.get(route_key)
            route_dist_list.append(round(route_dist, 1) if route_dist else None)
        else:
            geo_dist_list.append(None)
            route_dist_list.append(None)

    df["idhm_2010"] = idhm_list
    df["dist_km_geodesica_salvador"] = geo_dist_list
    df["dist_km_rodoviaria_salvador"] = route_dist_list

    # 5. Salvar o arquivo CSV
    out_path = "distancias_parcial_do_cache.csv"
    df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"\nConcluído! CSV com dados parciais gerado em: {out_path}")

if __name__ == "__main__":
    main()