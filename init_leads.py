# -*- coding: utf-8 -*-
"""
scrape_leads_maps.py
Autor: Antonio / GO-IS
Descripción:
  - Busca empresas con Google Places (Text Search) por consultas definidas.
  - Enriquecimiento: Place Details (web, teléfono, dirección).
  - Scraping de web (home, /contact, /contacto) para emails públicos.
  - Filtros RGPD: emails corporativos (excluye dominios personales).
  - Exporta a leads.xlsx
"""

import os
import time
import json
import math
import re
import random
import logging
from urllib.parse import urljoin, urlparse

import requests
import pandas as pd
from bs4 import BeautifulSoup
import tldextract
from dotenv import load_dotenv
from datetime import datetime

# -----------------------------
# Configuración
# -----------------------------
load_dotenv()
API_KEY = os.getenv("GOOGLE_PLACES_API_KEY", "").strip()

QUERIES = [
    "asesoría fiscal Valencia"
]

# ⚙️ Nuevo límite por query (para controlar coste y tiempo)
LIMIT_RESULTS = 10  # puedes subir a 30 si lo necesitas

# Máximo de resultados por query (Places devuelve 20 por página; con paginación)
MAX_RESULTS_PER_QUERY = 20

# Timeout y headers para scraping web
REQ_TIMEOUT = 5
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; GO-IS-lead-scraper/1.0; +https://landing-leads.go-is.es/)"
}

# Dominios personales a excluir (RGPD)
PERSONAL_DOMAINS = {
    "gmail.com", "hotmail.com", "outlook.com", "yahoo.com", "live.com", "icloud.com", "aol.com", "proton.me",
    "protonmail.com", "gmx.com", "gmx.es", "zoho.com"
}

# Patrón email básico y robusto
EMAIL_REGEX = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)

ROLE_PATTERNS = [
    "ceo",
    "founder",
    "cofounder",
    "fundador",
    "cofundador",
    "director general",
    "managing director",
    "director comercial",
    "responsable comercial",
    "head of sales",
    "sales director",
    "director de operaciones",
    "operations manager",
    "head of operations",
]

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# -----------------------------
# Utilidades
# -----------------------------
def is_corporate_email(email: str) -> bool:
    email = email.strip().lower()
    if not EMAIL_REGEX.fullmatch(email):
        return False
    domain = email.split("@")[-1]
    if domain in PERSONAL_DOMAINS:
        return False
    # Evita subdominios personales populares tipo ".gmail.com" (por si acaso)
    parts = domain.split(".")
    if len(parts) >= 2 and ".".join(parts[-2:]) in PERSONAL_DOMAINS:
        return False
    return True

def classify_email(email: str) -> str:
    if not email:
        return "no_encontrado"
    return "corporativo" if is_corporate_email(email) else "personal"

def clean_phone(phone: str) -> str:
    if not phone:
        return ""
    return re.sub(r"[^\d+]", "", phone)

def polite_sleep(a=0.2, b=0.5):
    time.sleep(random.uniform(a, b))

def fetch_url(url: str) -> str:
    try:
        r = requests.get(url, headers=HEADERS, timeout=REQ_TIMEOUT)
        if r.status_code == 200 and "text/html" in r.headers.get("Content-Type", ""):
            return r.text
    except requests.RequestException:
        return ""
    return ""

def extract_emails_from_html(html: str) -> set:
    emails = set()
    if not html:
        return emails
    # Extraer por mailto:
    for mailto in re.findall(r'href=["\']mailto:([^"\']+)["\']', html, flags=re.IGNORECASE):
        email = mailto.split("?")[0].strip()
        if EMAIL_REGEX.fullmatch(email):
            emails.add(email)
    # Extraer en texto
    for match in EMAIL_REGEX.findall(html):
        emails.add(match.strip())
    return emails

def extract_responsable_from_text(text: str) -> tuple:
    """
    Intenta detectar un nombre y cargo en texto público.
    Devuelve: (nombre, cargo)
    """
    if not text:
        return "", ""

    text = re.sub(r"\s+", " ", text).strip()

    patterns = [
        r"([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+){1,3})\s*[,|\-|–]\s*(CEO|Founder|Cofounder|Fundador|Cofundador|Director General|Managing Director|Director Comercial|Responsable Comercial|Head of Sales|Sales Director|Director de Operaciones|Operations Manager|Head of Operations)",
        r"(CEO|Founder|Cofounder|Fundador|Cofundador|Director General|Managing Director|Director Comercial|Responsable Comercial|Head of Sales|Sales Director|Director de Operaciones|Operations Manager|Head of Operations)\s*[:\-]?\s*([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+){1,3})",
    ]

    for pattern in patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            g1, g2 = m.group(1).strip(), m.group(2).strip()

            if any(role.lower() in g1.lower() for role in ROLE_PATTERNS):
                cargo = g1
                nombre = g2
            else:
                nombre = g1
                cargo = g2

            # 🔥 FILTROS DE CALIDAD (AQUÍ)
            if len(nombre.split()) < 2:
                return "", ""

            if any(word in nombre.lower() for word in ["grupo", "consultores", "empresa"]):
                return "", ""

            return nombre, cargo

    return "", ""

def find_candidate_pages(base_url: str) -> list:
    # Devuelve home + rutas donde suele aparecer equipo o dirección
    paths = [
        "",
        "contacto",
        "contact",
        "sobre-nosotros",
        "nosotros",
        "equipo",
        "team",
        "about",
        "quienes-somos",
    ]
    unique_urls = []
    for p in paths:
        u = urljoin(base_url if base_url.endswith("/") else base_url + "/", p)
        if u not in unique_urls:
            unique_urls.append(u)
    return unique_urls[:5]

def extract_responsable_from_website(base_url: str) -> tuple:
    """
    Recorre páginas públicas de la web y devuelve:
    (nombre, cargo, fuente_url)
    """
    for page_url in find_candidate_pages(base_url):
        polite_sleep(0.2, 0.5)
        html = fetch_url(page_url)
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(separator=" ", strip=True) if soup else ""
        nombre, cargo = extract_responsable_from_text(text)

        if nombre and cargo:
            return nombre, cargo, page_url

    return "", "", ""

def normalize_website(url: str) -> str:
    if not url:
        return ""
    url = url.strip()
    if not url.startswith("http"):
        url = "http://" + url
    # quitar parámetros raros
    parsed = urlparse(url)
    clean = f"{parsed.scheme}://{parsed.netloc}"
    return clean

def score_lead(row: dict) -> float:
    score = 0.0

    if row.get("web"):
        score += 0.25

    if classify_email(row.get("email", "")) == "corporativo":
        score += 0.25

    if row.get("telefono"):
        score += 0.25

    if row.get("responsable_nombre"):
        score += 0.25

    return round(min(score, 1.0), 2)

# -----------------------------
# Google Places API
# -----------------------------
PLACES_TEXTSEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
PLACES_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"

def places_text_search(query: str, api_key: str):
    """Itera paginación de Text Search para una query."""
    results = []
    params = {
        "query": query,
        "key": api_key,
        "language": "es"
    }
    page_count = 0
    while True:
        resp = requests.get(PLACES_TEXTSEARCH_URL, params=params, timeout=20)
        data = resp.json()
        status = data.get("status")
        if status not in ("OK", "ZERO_RESULTS"):
            logging.warning("TextSearch status: %s", status)
        results.extend(data.get("results", []))
        next_token = data.get("next_page_token")
        page_count += 1
        if not next_token or len(results) >= MAX_RESULTS_PER_QUERY or page_count >= 2:
            break
        # Next page necesita esperar unos segundos
        polite_sleep(2.2, 3.1)
        params["pagetoken"] = next_token
    return results[:MAX_RESULTS_PER_QUERY]

def place_details(place_id: str, api_key: str):
    fields = "name,formatted_address,formatted_phone_number,website,types,geometry,address_components"
    params = {
        "place_id": place_id,
        "key": api_key,
        "language": "es",
        "fields": fields
    }
    resp = requests.get(PLACES_DETAILS_URL, params=params, timeout=20)
    data = resp.json()
    if data.get("status") != "OK":
        return {}
    return data.get("result", {})

def address_component(result, typ):
    for c in result.get("address_components", []):
        if typ in c.get("types", []):
            return c.get("long_name", "")
    return ""

# -----------------------------
# Proceso principal
# -----------------------------
def main():
    if not API_KEY:
        raise SystemExit("⚠️ Falta GOOGLE_PLACES_API_KEY en tu entorno (.env).")

    all_rows = []
    seen_domains = set()
    seen_places = set()

    for q in QUERIES:
        logging.info("🔎 Buscando: %s", q)
        # items = places_text_search(q, API_KEY)
        items = places_text_search(q, API_KEY)[:LIMIT_RESULTS]
        logging.info("   → %d candidatos encontrados", len(items))

        for it in items:
            reviews = it.get("user_ratings_total", 0)
            if reviews < 5:
                continue

            place_id = it.get("place_id")
            if not place_id or place_id in seen_places:
                continue
            seen_places.add(place_id)

            polite_sleep()

            details = place_details(place_id, API_KEY)
            if not details:
                continue

            name = details.get("name", "").strip()
            address = details.get("formatted_address", "").strip()
            phone = clean_phone(details.get("formatted_phone_number", ""))
            website_raw = details.get("website", "")
            website = normalize_website(website_raw) if website_raw else ""
            if not website:
                continue

            city = address_component(details, "locality") or address_component(details, "postal_town")
            admin_area = address_component(details, "administrative_area_level_2")
            country = address_component(details, "country")
            types = details.get("types", [])
            if not any(t in types for t in ["point_of_interest", "establishment"]):
                continue
            categoria = ", ".join(types) if types else ""

            # Extraer emails desde la web (si existe)
            email_found = ""
            email_tipo = "no_encontrado"
            fuente = "google_places"
            responsable_nombre = ""
            responsable_cargo = ""
            responsable_fuente = ""

            if website:
                # Evita duplicar por dominio
                domain = tldextract.extract(website).top_domain_under_public_suffix
                if domain and domain in seen_domains:
                    pass
                else:
                    if domain:
                        seen_domains.add(domain)

                    # 1) Buscar posible responsable en la web pública
                    responsable_nombre, responsable_cargo, responsable_fuente = extract_responsable_from_website(website)

                    # 2) Buscar emails públicos
                    emails = set()
                    for page_url in find_candidate_pages(website):
                        polite_sleep(0.2, 0.5)
                        html = fetch_url(page_url)
                        if not html:
                            continue

                        soup = BeautifulSoup(html, "html.parser")
                        text = soup.get_text(separator=" ", strip=True) if soup else ""
                        found = extract_emails_from_html(html) | extract_emails_from_html(text)
                        emails |= found

                        if emails:
                            corp_emails = [e for e in emails if is_corporate_email(e)]
                            if corp_emails:
                                emails = set(corp_emails)
                                break

                    # Selecciona un email
                    if emails:
                        priority = ["info@", "contact", "contacto", "comercial", "ventas", "admin@"]
                        chosen = None
                        for p in priority:
                            for e in emails:
                                if p in e.lower():
                                    chosen = e
                                    break
                            if chosen:
                                break

                        email_found = chosen or sorted(emails)[0]
                        email_tipo = classify_email(email_found)

            row = {
                "empresa": name,
                "web": website,
                "email": email_found,
                "email_tipo": email_tipo,
                "telefono": phone,
                "direccion": address,
                "ciudad": city,
                "provincia": admin_area,
                "pais": country,
                "responsable_nombre": responsable_nombre,
                "responsable_cargo": responsable_cargo,
                "responsable_fuente": responsable_fuente,
                "query": q,
                "fecha_extraccion": pd.Timestamp.utcnow().strftime("%Y-%m-%d"),
            }
            row["score_inicial"] = score_lead(row)
            all_rows.append(row)

    if not all_rows:
        logging.warning("No se encontraron leads. Revisa consultas o API key.")
        return

    df = pd.DataFrame(all_rows)

    # Limpieza final: únicos por (empresa, web) y corporativos primero
    df.sort_values(["score_inicial", "email_tipo"], ascending=[False, True], inplace=True)
    df.drop_duplicates(subset=["empresa", "web"], inplace=True)

    # 🔥 FILTRO DE CALIDAD
    df = df[df["score_inicial"] >= 0.5]

    
    # Poner la fecha al fichero a descargar
    fecha = datetime.now().strftime("%d%m%Y")
    out_file = f"leads_locales_{fecha}.xlsx"
    
    df.to_excel(out_file, index=False)
    logging.info("✅ Exportado: %s (filas: %d)", out_file, len(df))

if __name__ == "__main__":
    main()
