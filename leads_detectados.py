import requests
import re
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (LeadRadar/1.0)"
}

# =========================
# CONFIGURACIÓN – FUENTES
# =========================

SEARCH_URLS = [
    {
        "source": "tecnoempleo_backend",
        "url": "https://www.tecnoempleo.com/ofertas-trabajo/?te=backend"
    },
    {
        "source": "tecnoempleo_integracion",
        "url": "https://www.tecnoempleo.com/ofertas-trabajo/?te=integracion"
    }
]


OUTPUT_FILE = "leads_detectados.xlsx"

# =========================
# SCORING
# =========================

POSITIVE_SIGNALS = [
    (r"\bproceso(s)?\b", 3),
    (r"\bsistema(s)?\b", 3),
    (r"\boperaci(o|ó)n(es)?\b", 3),
    (r"\bflujo(s)?\b", 3),
    (r"\bautomatiza(r|ción)\b", 3),
    (r"\bintegraci(o|ó)n(es)?\b", 5),
    (r"\bapi(s)?\b", 4),
    (r"\bcrm\b", 4),
    (r"\berp\b", 4),
    (r"\bbase de datos\b", 4),
    (r"\bpostgre(s|sql)?\b", 4),
    (r"\bbackend\b", 3),
    (r"\bdato(s)?\b", 3),
    (r"\bmigrar|migración\b", 4),
    (r"\bsistema actual\b", 5),
]

NEGATIVE_SIGNALS = [
    (r"\bwordpress\b", -5),
    (r"\bwix\b", -5),
    (r"\bshopify\b", -4),
    (r"\blanding\b", -4),
    (r"\bchatbot\b", -4),
    (r"\bbot\b", -3),
    (r"\binstagram\b", -3),
    (r"\bredes sociales\b", -3),
    (r"\bseo\b", -3),
    (r"\bmarketing\b", -3),
    (r"\b100\s?€|200\s?€|300\s?€\b", -8),
    (r"\bsolo prompt|prompt\b", -6),
    (r"\bn8n|zapier|make\b", -3),
]

def compute_score(text: str) -> int:
    t = (text or "").lower()
    score = 0
    for pattern, value in POSITIVE_SIGNALS:
        if re.search(pattern, t):
            score += value
    for pattern, value in NEGATIVE_SIGNALS:
        if re.search(pattern, t):
            score += value
    return score

def infer_problem(text: str) -> str:
    t = (text or "").lower()
    if re.search(r"no funcion|fall|reabrimos|mal implementado", t):
        return "Intento previo fallido / mala implementación"
    if re.search(r"integraci|api|webhook", t):
        return "Necesidad de integrar sistemas o datos"
    if re.search(r"excel|manual", t):
        return "Proceso manual crítico / dependencia de Excel"
    if re.search(r"leads|crm|pipeline", t):
        return "Gestión ineficiente de leads o ventas"
    if re.search(r"report|dashboard", t):
        return "Reporting manual o inexistente"
    return "Problema operativo no especificado (requiere revisión)"

# =========================
# SCRAPING RSS (INDEED)
# =========================

def scrape_html(source_name, url):
    rows = []

    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()

        soup = BeautifulSoup(r.text, "html.parser")

        # 🔑 SELECTOR REAL DE TECNOEMPLEO
        offers = soup.select("div.oferta")

        for offer in offers:
            title_el = offer.select_one("a")
            desc_el = offer.select_one("div.descripcion")

            if not title_el:
                continue

            title = title_el.get_text(strip=True)
            description = desc_el.get_text(" ", strip=True) if desc_el else ""

            link = title_el["href"]
            if link.startswith("/"):
                link = "https://www.tecnoempleo.com" + link

            full_text = f"{title} {description}"

            score = compute_score(full_text)
            problem = infer_problem(full_text)

            rows.append({
                "fuente": source_name,
                "fecha_detectada": datetime.utcnow().strftime("%Y-%m-%d"),
                "titulo": title,
                "descripcion": description,
                "problema_detectado": problem,
                "score_dolor": score,
                "empresa": None,
                "url": link
            })

    except Exception as e:
        print(f"[ERROR] {source_name}: {e}")

    return rows


# =========================
# MAIN
# =========================

def main():
    all_rows = []

    for src in SEARCH_URLS:
        print(f"Analizando fuente: {src['source']}")
        rows = scrape_html(src["source"], src["url"])
        all_rows.extend(rows)

    if not all_rows:
        print("No se han detectado oportunidades.")
        return

    df = pd.DataFrame(all_rows)

    # Filtro duro: solo proyectos con señal real
    df = df[df["score_dolor"] >= 5]

    df = df.sort_values(by="score_dolor", ascending=False)

    df.to_excel(OUTPUT_FILE, index=False)
    print(f"Archivo generado: {OUTPUT_FILE} ({len(df)} filas)")

if __name__ == "__main__":
    main()
