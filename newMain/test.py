# product_extractor.py
import re
import time
import json
from typing import List, Dict, Tuple
import wikipediaapi
import wikipedia
import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, Query
from pydantic import BaseModel

wiki_api = wikipediaapi.Wikipedia(user_agent='product-extractor/1.0', language='en')
app = FastAPI(title="Product Name Extractor (wiki-first)")

# ----- Helpers -----
def normalize_name(name: str) -> str:
    name = name.strip()
    # remove excessive whitespace and newlines
    name = re.sub(r'\s+', ' ', name)
    # remove trailing punctuation
    name = name.strip(' -–—:;,.')
    return name

def unique_preserve_order(seq):
    seen = set()
    out = []
    for item in seq:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out

# ----- Wikipedia category traversal -----
def get_category_members_recursive(catpage, max_depth=2, level=0):
    """Return list of page titles under a category (recursively up to max_depth)."""
    results = []
    if level > max_depth:
        return results
    for title, member in catpage.categorymembers.items():
        # ns 0 = main/article pages (products etc.), ns 14 = category
        if member.ns == wikipediaapi.Namespace.MAIN:
            results.append(title)
        elif member.ns == wikipediaapi.Namespace.CATEGORY:
            # recurse into subcategory
            results.extend(get_category_members_recursive(member, max_depth=max_depth, level=level+1))
    return results

def try_wikipedia_category_path(main: str, sub: str = None, subsub: str = None):
    """
    Try several candidate category names built from the path.
    Returns list of found product names and a confidence measure (0-1).
    """
    candidates = []
    # Build common candidate category names
    parts = [p for p in [main, sub, subsub] if p]
    # direct joined as "Category:Main/Sub/Sub"
    joined = "Category:" + "/".join(parts)
    candidates.append(joined)
    # Also try "Category:<subsub>" or "Category:<sub>" etc
    if subsub:
        candidates.append("Category:" + subsub)
    if sub:
        candidates.append("Category:" + sub)
    # Try plural forms, and "List of <subsub>" as fallback
    if subsub:
        candidates.append("Category:" + subsub + "s")
        candidates.append("List of " + subsub)
    if sub:
        candidates.append("List of " + sub)

    found_products = []
    best_conf = 0.0
    for cand in unique_preserve_order(candidates):
        try:
            if cand.startswith("Category:"):
                catname = cand[len("Category:"):]
                catpage = wiki_api.page("Category:" + catname)
                if catpage.exists():
                    names = get_category_members_recursive(catpage, max_depth=2)
                    if names:
                        found_products.extend([(normalize_name(n), "wikipedia_category", cand) for n in names])
                        best_conf = max(best_conf, 0.9)
                        break  # prefer category results first
            else:
                # it's a "List of ..." page candidate — use wikipedia search + parse
                search_results = wikipedia.search(cand, results=5)
                for title in search_results:
                    try:
                        page = wiki_api.page(title)
                        if page.exists():
                            # gather links on the page (often list items)
                            text_links = list(page.links.keys())
                            if len(text_links) >= 3:
                                found_products.extend([(normalize_name(p), "wikipedia_list", title) for p in text_links])
                                best_conf = max(best_conf, 0.85)
                                break
                    except Exception:
                        continue
                if found_products:
                    break
        except Exception:
            continue
    return found_products, best_conf

# ----- Wikipedia search fallback -----
def try_wikipedia_search(path_terms: List[str], max_results=10):
    """
    If category approach fails, search Wikipedia and extract plausible product names from top pages.
    """
    query = " ".join(path_terms)
    titles = wikipedia.search(query, results=max_results)
    out = []
    for t in titles:
        try:
            page = wiki_api.page(t)
            if page.exists():
                # if the page is likely a product (heuristic: contains year/model tokens or short page)
                # collect links (sub-items) and the title itself
                out.append((normalize_name(t), "wikipedia_search", query))
                # also add page links as candidates
                links = list(page.links.keys())[:40]
                out.extend([(normalize_name(l), "wikipedia_search_link", t) for l in links])
        except Exception:
            continue
    return out

# ----- Simple retailer page fallback (lightweight) -----
def simple_retailer_fallback(category_url: str, css_selector_candidates: List[str] = None) -> List[str]:
    """
    A minimal fallback to fetch product names from a provided category URL.
    css_selector_candidates is a list of selectors to try for product titles (e.g. ['.product-title', 'h2 a']).
    This is intentionally small — for production, replace with Scrapy/Playwright spiders with site adapters.
    """
    headers = {"User-Agent": "Mozilla/5.0 (compatible; product-extractor/1.0)"}
    try:
        resp = requests.get(category_url, headers=headers, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        selectors = css_selector_candidates or [
            ".product-title", ".product-name", "h2 a", ".product-card__title", ".s-title"
        ]
        names = []
        for sel in selectors:
            for el in soup.select(sel):
                text = el.get_text(strip=True)
                if text:
                    names.append(normalize_name(text))
        return unique_preserve_order(names)
    except Exception:
        return []

# ----- Main extractor function -----
def extract_products_from_path(main: str, sub: str = None, subsub: str = None, retailer_url: str = None):
    """
    Returns list of dicts: { name, source, source_ref, confidence }
    """
    path_terms = [t for t in [main, sub, subsub] if t]
    results = []

    # 1) Wikipedia category path heuristics
    wiki_items, conf = try_wikipedia_category_path(main, sub, subsub)
    for name, src, ref in wiki_items:
        results.append({"name": name, "source": src, "source_ref": ref, "confidence": 0.9 if src == "wikipedia_category" else 0.85})

    if len(results) < 10:
        # 2) Wikipedia search fallback
        search_items = try_wikipedia_search(path_terms, max_results=5)
        for name, src, ref in search_items:
            results.append({"name": name, "source": src, "source_ref": ref, "confidence": 0.7})

    if len(results) < 10 and retailer_url:
        # 3) Minimal retailer fallback if user provided a category URL
        retailer_names = simple_retailer_fallback(retailer_url)
        for n in retailer_names:
            results.append({"name": n, "source": "retailer_fallback", "source_ref": retailer_url, "confidence": 0.6})

    # Normalize, dedupe, aggregate best confidences
    agg = {}
    for item in results:
        key = item["name"].lower()
        if key not in agg or item["confidence"] > agg[key]["confidence"]:
            agg[key] = {"name": item["name"], "source": item["source"], "source_ref": item["source_ref"], "confidence": item["confidence"]}

    final = sorted(agg.values(), key=lambda x: -x["confidence"])
    return final

# ----- FastAPI models and endpoint -----
class ExtractRequest(BaseModel):
    main: str
    sub: str = None
    subsub: str = None
    retailer_url: str = None  # optional fallback url to try

@app.post("/extract")
def extract(req: ExtractRequest):
    names = extract_products_from_path(req.main, req.sub, req.subsub, req.retailer_url)
    return {"category_path": [req.main, req.sub, req.subsub], "count": len(names), "products": names}

@app.get("/extract")
def extract_get(main: str = Query(...), sub: str = Query(None), subsub: str = Query(None), retailer_url: str = Query(None)):
    names = extract_products_from_path(main, sub, subsub, retailer_url)
    return {"category_path": [main, sub, subsub], "count": len(names), "products": names}

# ----- quick test runner -----
if __name__ == "__main__":
    # quick CLI demo
    import argparse
    from datetime import datetime
    parser = argparse.ArgumentParser()
    parser.add_argument("--main", required=True)
    parser.add_argument("--sub")
    parser.add_argument("--subsub")
    parser.add_argument("--retailer_url")
    parser.add_argument("--output", help="Output JSON file path (default: auto-generated)")
    args = parser.parse_args()
    res = extract_products_from_path(args.main, args.sub, args.subsub, args.retailer_url)
    
    # Generate output filename if not provided
    if args.output:
        output_file = args.output
    else:
        # Create filename from category path and timestamp
        category_parts = [args.main, args.sub, args.subsub]
        category_str = "_".join([p for p in category_parts if p]).replace(" ", "_").lower()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"output_{category_str}_{timestamp}.json"
    
    # Save to JSON file
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(res, f, indent=2, ensure_ascii=False)
    
    print(f"Results saved to: {output_file}")
    print(f"Total products found: {len(res)}")
    print(json.dumps(res, indent=2))
