"""
Quantum Lens — Sustainable Real-Time Regulatory Monitoring System
Version 2.0 — Self-healing, auto-discovering, fully autonomous

THREE LAYERS:
  Layer 1 — GOV.UK Search API    Automatically finds ALL new/updated GOV.UK content
  Layer 2 — Sitemap crawling     Automatically detects changes on ACAS, Shelter, CA, NHS
  Layer 3 — AI URL self-healing  Automatically fixes broken URLs using Claude

Modes:
  python ingest_live.py                 Full ingestion of all sources
  python ingest_live.py --update-only   Check all sources for changes
  python ingest_live.py --monitor       Real-time RSS monitoring (run every 15 min)
  python ingest_live.py --hash-check    Content hash monitoring (run every 4 hours)
  python ingest_live.py --full-monitor  Run both RSS and hash monitoring
  python ingest_live.py --heal-urls     Check and fix all broken URLs

Requirements:
  pip install pinecone openai requests beautifulsoup4 tiktoken python-dotenv feedparser anthropic
"""

import os
import json
import time
import hashlib
import argparse
import requests
import feedparser
import re
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from openai import OpenAI
from pinecone import Pinecone
from dotenv import load_dotenv
import tiktoken
from datetime import datetime, timezone, timedelta

load_dotenv()

openai_client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
pc = Pinecone(api_key=os.getenv('PINECONE_API_KEY'))
index = pc.Index(os.getenv('PINECONE_INDEX_NAME', 'quantum-lens-regulatory'))

SUPABASE_URL = os.getenv('SUPABASE_URL', '')
SUPABASE_SERVICE_KEY = os.getenv('SUPABASE_SERVICE_KEY', '')
ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY', '')

EMBEDDING_MODEL = 'text-embedding-3-small'
CHUNK_SIZE = 400
CHUNK_OVERLAP = 60
BATCH_SIZE = 50
TIMESTAMPS_FILE = 'regulatory_timestamps.json'
MONITOR_STATE_FILE = 'monitor_state.json'
CONTENT_HASHES_FILE = 'content_hashes.json'
BROKEN_URLS_FILE = 'broken_urls.json'
DISCOVERED_URLS_FILE = 'discovered_urls.json'


# ── GOVUK SEARCH TOPICS — Layer 1 ────────────────────────────────────
# These search queries automatically discover ALL relevant GOV.UK content
# No need to manually maintain a list of GOV.UK URLs ever again
GOVUK_SEARCH_QUERIES = [
    {'query': 'employment rights dismissal redundancy',     'lens': 'employment'},
    {'query': 'national minimum wage national living wage', 'lens': 'employment'},
    {'query': 'maternity paternity parental leave',         'lens': 'employment'},
    {'query': 'discrimination equality workplace',          'lens': 'employment'},
    {'query': 'flexible working working time regulations',  'lens': 'employment'},
    {'query': 'skilled worker visa immigration rules',      'lens': 'immigration'},
    {'query': 'right to work sponsor licence',             'lens': 'immigration'},
    {'query': 'indefinite leave to remain settlement',     'lens': 'immigration'},
    {'query': 'student visa graduate visa family visa',    'lens': 'immigration'},
    {'query': 'statement of changes immigration rules',    'lens': 'immigration'},
    {'query': 'private renting eviction tenancy',          'lens': 'housing'},
    {'query': 'section 21 deposit protection landlord',    'lens': 'housing'},
    {'query': 'homelessness housing benefit',              'lens': 'housing'},
    {'query': 'universal credit pip benefits appeal',      'lens': 'benefits'},
    {'query': 'jobseeker allowance employment support',    'lens': 'benefits'},
    {'query': 'income tax national insurance self assessment', 'lens': 'finance'},
    {'query': 'vat corporation tax making tax digital',    'lens': 'finance'},
    {'query': 'employer rates thresholds hmrc',            'lens': 'finance'},
    {'query': 'data protection gdpr ico companies house',  'lens': 'business'},
    {'query': 'auto enrolment workplace pension employer', 'lens': 'business'},
    {'query': 'consumer rights act online selling',        'lens': 'consumer'},
    {'query': 'nhs rights mental health patient',          'lens': 'health'},
    {'query': 'migrant health entitlements nhs',           'lens': 'health'},
]


# ── SITEMAPS TO CRAWL — Layer 2 ───────────────────────────────────────
# Automatically discovers ALL changed pages on these sites
# No need to manually maintain ACAS, Shelter, CA, NHS URL lists
SITEMAPS = [
    {
        'url': 'https://www.acas.org.uk/sitemap.xml',
        'name': 'ACAS',
        'lens': 'employment',
        'keywords': ['dismissal', 'redundancy', 'discrimination', 'maternity',
                     'disciplinary', 'grievance', 'flexible', 'holiday',
                     'sick', 'whistleblowing', 'performance', 'reasonable',
                     'code-of-practice', 'pay', 'contracts', 'working-time'],
    },
    {
        'url': 'https://england.shelter.org.uk/sitemap.xml',
        'name': 'Shelter England',
        'lens': 'housing',
        'keywords': ['eviction', 'renting', 'homelessness', 'repairs',
                     'deposits', 'landlord', 'tenancy', 'housing'],
    },
    {
        'url': 'https://www.citizensadvice.org.uk/sitemap.xml',
        'name': 'Citizens Advice',
        'lens': 'employment',
        'keywords': ['work', 'dismissal', 'housing', 'eviction', 'benefits',
                     'universal-credit', 'immigration', 'consumer', 'health',
                     'debt', 'employment', 'redundancy', 'discrimination'],
    },
    {
        'url': 'https://www.nhs.uk/sitemap.xml',
        'name': 'NHS',
        'lens': 'health',
        'keywords': ['rights', 'services', 'gp', 'mental-health',
                     'complaints', 'nhs-constitution', 'patient'],
    },
    {
        'url': 'https://www.legislation.gov.uk/sitemap.xml',
        'name': 'Legislation.gov.uk',
        'lens': 'employment',
        'keywords': ['employment', 'immigration', 'housing', 'minimum-wage',
                     'equality', 'working-time', 'consumer'],
    },
]


# ── RSS FEEDS — checked every 15 minutes ─────────────────────────────
RSS_FEEDS = [
    {
        'url': 'https://www.gov.uk/government/feed',
        'name': 'GOV.UK — All updates',
        'keywords': ['employment', 'immigration', 'housing', 'benefits',
                     'minimum wage', 'visa', 'skilled worker', 'tenancy',
                     'universal credit', 'national insurance', 'tax',
                     'right to work', 'dismissal', 'redundancy',
                     'statement of changes', 'immigration rules', 'HC ',
                     'leave to remain', 'settlement', 'certificate of sponsorship']
    },
    {
        'url': 'https://www.gov.uk/search/policy-papers-and-consultations.atom',
        'name': 'GOV.UK — Policy papers',
        'keywords': ['employment', 'immigration', 'housing', 'benefits', 'tax']
    },
    {
        'url': 'https://www.gov.uk/search/guidance-and-regulation.atom',
        'name': 'GOV.UK — Guidance and regulation',
        'keywords': ['minimum wage', 'visa', 'tenancy', 'universal credit']
    },
    {
        'url': 'https://www.gov.uk/government/organisations/hm-revenue-customs.atom',
        'name': 'HMRC — Updates',
        'keywords': ['tax', 'national insurance', 'minimum wage', 'self assessment', 'vat']
    },
    {
        'url': 'https://www.gov.uk/government/organisations/home-office.atom',
        'name': 'Home Office — Updates',
        'keywords': ['immigration', 'visa', 'skilled worker', 'right to work', 'asylum',
                     'statement of changes', 'immigration rules', 'HC ',
                     'leave to remain', 'biometric', 'sponsor']
    },
    {
        'url': 'https://www.gov.uk/government/organisations/department-for-work-pensions.atom',
        'name': 'DWP — Benefits and pensions updates',
        'keywords': ['universal credit', 'pip', 'benefits', 'pension',
                     'jobseeker', 'employment support', 'dwp', 'welfare']
    },
    {
        'url': 'https://ico.org.uk/about-the-ico/media-centre/news-and-blogs/rss/',
        'name': 'ICO — Data protection news',
        'keywords': ['gdpr', 'data protection', 'personal data', 'ico']
    },
    {
        'url': 'https://www.hse.gov.uk/press/rssfeed.xml',
        'name': 'HSE — Health and safety',
        'keywords': ['workplace', 'health and safety', 'employer', 'worker']
    },
    {
        'url': 'https://www.fca.org.uk/news/rss.xml',
        'name': 'FCA — Financial regulation',
        'keywords': ['consumer', 'financial', 'insurance', 'credit', 'bank']
    },
    {
        'url': 'https://www.legislation.gov.uk/new/data.feed',
        'name': 'legislation.gov.uk — New legislation',
        'keywords': ['employment', 'immigration', 'housing', 'benefits',
                     'minimum wage', 'tenancy', 'visa', 'tax', 'health']
    },
    {
        'url': 'https://www.legislation.gov.uk/uksi/data.feed',
        'name': 'legislation.gov.uk — Statutory Instruments',
        'keywords': ['employment', 'immigration', 'housing', 'national minimum wage',
                     'working time', 'benefits', 'tax', 'visa']
    },
    {
        'url': 'https://www.equalityhumanrights.com/rss.xml',
        'name': 'EHRC — Equality and Human Rights Commission',
        'keywords': ['discrimination', 'equality', 'race', 'gender',
                     'disability', 'harassment', 'equality act']
    },
    {
        'url': 'https://www.thepensionsregulator.gov.uk/en/news-and-press-releases.rss',
        'name': 'Pensions Regulator — Employer obligations',
        'keywords': ['pension', 'auto-enrolment', 'employer', 'workplace pension']
    },
]


# ── KEYWORD TO LENS MAPPING ───────────────────────────────────────────
KEYWORD_LENS_MAP = {
    'employment': 'employment', 'minimum wage': 'employment',
    'national living wage': 'employment', 'dismissal': 'employment',
    'redundancy': 'employment', 'workplace': 'employment',
    'worker': 'employment', 'employer': 'employment',
    'acas': 'employment', 'tribunal': 'employment',
    'discrimination': 'employment', 'flexible working': 'employment',
    'maternity': 'employment', 'pension': 'employment',
    'immigration': 'immigration', 'visa': 'immigration',
    'skilled worker': 'immigration', 'right to work': 'immigration',
    'asylum': 'immigration', 'home office': 'immigration',
    'ukvi': 'immigration', 'biometric': 'immigration',
    'settled status': 'immigration', 'leave to remain': 'immigration',
    'statement of changes': 'immigration',
    'housing': 'housing', 'tenancy': 'housing',
    'landlord': 'housing', 'eviction': 'housing',
    'rent': 'housing', 'section 21': 'housing', 'shelter': 'housing',
    'benefits': 'benefits', 'universal credit': 'benefits',
    'pip': 'benefits', 'dwp': 'benefits',
    'jobseeker': 'benefits', 'tax credit': 'benefits', 'welfare': 'benefits',
    'tax': 'finance', 'national insurance': 'finance',
    'self assessment': 'finance', 'vat': 'finance', 'hmrc': 'finance',
    'gdpr': 'business', 'data protection': 'business',
    'ico': 'business', 'companies house': 'business',
    'auto-enrolment': 'business',
    'consumer': 'consumer', 'financial': 'consumer', 'fca': 'consumer',
    'nhs': 'health', 'health': 'health',
    'mental health': 'health', 'cqc': 'health',
}


# ── FALLBACK STATIC SOURCES ───────────────────────────────────────────
# Only used if GOV.UK Search API and sitemaps both fail
# Kept minimal — the dynamic discovery handles everything else
STATIC_SOURCES = [
    # Core legislation — always needed
    {'url': 'https://www.legislation.gov.uk/ukpga/1996/18/contents',
     'lens': 'employment', 'topic': 'employment_rights_act_1996',
     'title': 'Employment Rights Act 1996'},
    {'url': 'https://www.legislation.gov.uk/ukpga/2010/15/contents',
     'lens': 'employment', 'topic': 'equality_act_2010',
     'title': 'Equality Act 2010'},
    {'url': 'https://www.legislation.gov.uk/ukpga/2025/15/contents',
     'lens': 'employment', 'topic': 'employment_rights_act_2025',
     'title': 'Employment Rights Act 2025'},
    {'url': 'https://www.legislation.gov.uk/ukpga/2014/22/contents',
     'lens': 'immigration', 'topic': 'immigration_act_2014',
     'title': 'Immigration Act 2014'},
    {'url': 'https://www.legislation.gov.uk/ukpga/1988/50/contents',
     'lens': 'housing', 'topic': 'housing_act_1988',
     'title': 'Housing Act 1988'},

    # GOV.UK visa pages — not always in search results
    {'url': 'https://www.gov.uk/skilled-worker-visa',
     'lens': 'immigration', 'topic': 'skilled_worker_visa',
     'title': 'Skilled Worker visa'},
    {'url': 'https://www.gov.uk/skilled-worker-visa/your-job',
     'lens': 'immigration', 'topic': 'skilled_worker_job',
     'title': 'Skilled Worker visa — your job'},
    {'url': 'https://www.gov.uk/skilled-worker-visa/salary-requirements',
     'lens': 'immigration', 'topic': 'skilled_worker_salary',
     'title': 'Skilled Worker visa — salary requirements'},
    {'url': 'https://www.gov.uk/innovator-founder-visa',
     'lens': 'immigration', 'topic': 'innovator_founder_visa',
     'title': 'Innovator Founder visa'},
    {'url': 'https://www.gov.uk/graduate-visa',
     'lens': 'immigration', 'topic': 'graduate_visa',
     'title': 'Graduate visa'},

    # HC 259 — August 2026 Immigration Rules changes
    {'url': 'https://www.gov.uk/government/publications/statement-of-changes-to-the-immigration-rules-hc-259-9-july-2026/statement-of-changes-to-the-immigration-rules-hc-259-9-july-2026-accessible',
     'lens': 'immigration', 'topic': 'hc_259_aug_2026',
     'title': 'Immigration Rules Statement of Changes HC 259 — August 2026'},

    # Financial Ombudsman — not indexed by GOV.UK search
    {'url': 'https://www.financial-ombudsman.org.uk/consumers/complaints-can-help',
     'lens': 'finance', 'topic': 'financial_ombudsman',
     'title': 'Financial Ombudsman'},

    # ICO — GDPR
    {'url': 'https://ico.org.uk/for-organisations/guide-to-data-protection/',
     'lens': 'business', 'topic': 'ico_gdpr',
     'title': 'ICO — Guide to data protection'},
    {'url': 'https://ico.org.uk/for-organisations/sme-web-hub/',
     'lens': 'business', 'topic': 'ico_sme',
     'title': 'ICO — SME guidance'},
    {'url': 'https://ico.org.uk/your-data-matters/',
     'lens': 'consumer', 'topic': 'ico_consumer',
     'title': 'ICO — Your data matters'},

    # CQC
    {'url': 'https://www.cqc.org.uk/guidance-providers/regulations',
     'lens': 'business', 'topic': 'cqc_regulations',
     'title': 'CQC — Regulations for providers'},

    # Pensions Regulator
    {'url': 'https://www.thepensionsregulator.gov.uk/en/employers',
     'lens': 'business', 'topic': 'pensions_regulator',
     'title': 'Pensions Regulator — Employers'},

    # NHS migrant guide
    {'url': 'https://www.gov.uk/guidance/nhs-entitlements-migrant-health-guide',
     'lens': 'health', 'topic': 'nhs_migrant',
     'title': 'NHS entitlements — Migrant health guide'},
]


# ── HELPER FUNCTIONS ─────────────────────────────────────────────────

def load_json(filename):
    if os.path.exists(filename):
        with open(filename) as f:
            return json.load(f)
    return {}

def save_json(filename, data):
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)

def get_content_hash(text):
    return hashlib.md5(text.encode()).hexdigest()

def detect_lens_from_text(text):
    text_lower = text.lower()
    lens_counts = {}
    for keyword, lens in KEYWORD_LENS_MAP.items():
        if keyword in text_lower:
            lens_counts[lens] = lens_counts.get(lens, 0) + 1
    return max(lens_counts, key=lens_counts.get) if lens_counts else 'general'

def score_impact(text):
    high = ['must', 'required', 'mandatory', 'penalty', 'fine', 'criminal',
            'prosecution', 'illegal', 'prohibited', 'deadline', 'expires', 'immediate']
    medium = ['should', 'recommended', 'updated', 'changed', 'increased',
              'decreased', 'amended', 'revised', 'new guidance']
    text_lower = text.lower()
    if sum(1 for kw in high if kw in text_lower) >= 2:
        return 'High'
    elif any(kw in text_lower for kw in high) or sum(1 for kw in medium if kw in text_lower) >= 2:
        return 'Medium'
    return 'Low'

def push_alert_to_supabase(lens, topic, title, alert_text, source_url='', impact='Medium'):
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        return False
    try:
        r = requests.post(
            SUPABASE_URL + '/rest/v1/regulatory_changes',
            json={'lens': lens, 'topic': topic, 'title': title,
                  'alert_text': alert_text, 'source_url': source_url,
                  'impact': impact, 'is_active': True,
                  'created_at': datetime.now(timezone.utc).isoformat()},
            headers={'apikey': SUPABASE_SERVICE_KEY,
                     'Authorization': 'Bearer ' + SUPABASE_SERVICE_KEY,
                     'Content-Type': 'application/json',
                     'Prefer': 'return=minimal'},
            timeout=10
        )
        if r.status_code in [200, 201]:
            print('  Alert pushed: [' + lens.upper() + '] ' + title)
            return True
        return False
    except Exception as e:
        print('  Supabase error: ' + str(e))
        return False

def get_headers():
    import random
    agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/125.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0',
    ]
    return {
        'User-Agent': random.choice(agents),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-GB,en;q=0.9',
    }

def fetch_govuk_api(path):
    try:
        r = requests.get('https://www.gov.uk/api/content/' + path,
                        headers=get_headers(), timeout=15)
        r.raise_for_status()
        data = r.json()
        title = data.get('title', path)
        updated_at = data.get('updated_at') or data.get('public_updated_at', '')
        details = data.get('details', {})
        body_html = ''
        if isinstance(details, dict):
            body_html = details.get('body', '') or ''
            parts = details.get('parts', [])
            if parts:
                body_html = ' '.join(p.get('body', '') or '' for p in parts)
        if not body_html:
            body_html = data.get('description', '') or ''
        if body_html:
            soup = BeautifulSoup(body_html, 'html.parser')
            text = soup.get_text(separator=' ', strip=True)
        else:
            text = str(title) + '. ' + str(data.get('description', '') or '')
        return re.sub(r'\s+', ' ', text).strip(), title, updated_at
    except Exception as e:
        print('  GOV.UK API error for ' + path + ': ' + str(e))
        return None, None, None

def fetch_web_scrape(url):
    import random
    SITE_SELECTORS = {
        'shelter.org.uk': ['article', '.article-content', 'main'],
        'citizensadvice.org.uk': ['.main-content', 'main article', 'main'],
        'nhs.uk': ['.nhsuk-main-wrapper', 'main', 'article'],
        'acas.org.uk': ['.article-content', 'main', 'article'],
        'hse.gov.uk': ['#content', 'main', 'article'],
        'equalityhumanrights.com': ['main', 'article', '.field-items'],
        'financial-ombudsman.org.uk': ['main', 'article', '.content'],
        'cqc.org.uk': ['main', '#content', 'article'],
        'ico.org.uk': ['main', '.content', 'article'],
        'legislation.gov.uk': ['#content', 'main', '.legContent'],
        'fca.org.uk': ['main', 'article', '.content'],
        'thepensionsregulator.gov.uk': ['main', 'article', '.content'],
        'moneyhelper.org.uk': ['main', 'article', '.content'],
        'gov.uk': ['main', '#content', 'article'],
    }
    try:
        time.sleep(random.uniform(0.3, 1.0))
        r = requests.get(url, headers=get_headers(), timeout=20, allow_redirects=True)
        r.raise_for_status()
        soup = BeautifulSoup(r.content, 'html.parser')
        for tag in soup(['nav', 'header', 'footer', 'script', 'style', 'aside']):
            if hasattr(tag, 'decompose'):
                tag.decompose()
        domain = re.sub(r'^www\.', '', re.sub(r'https?://', '', url).split('/')[0])
        selectors = SITE_SELECTORS.get(domain, []) + ['main', 'article', '#content', 'body']
        main = None
        for sel in selectors:
            found = soup.select_one(sel) if sel.startswith(('.', '#')) else soup.find(sel)
            if found and len(found.get_text(strip=True)) > 100:
                main = found
                break
        text = main.get_text(separator=' ', strip=True) if main else soup.get_text(separator=' ', strip=True)
        text = re.sub(r'\s+', ' ', text).strip()
        if len(text) < 100 or 'enable javascript' in text.lower():
            return None, None
        return text, datetime.now().isoformat()
    except Exception as e:
        print('  Scrape error for ' + url + ': ' + str(e))
        return None, None

def chunk_and_index(text, source_id, lens, topic, title, updated_at):
    enc = tiktoken.get_encoding('cl100k_base')
    tokens = enc.encode(text)
    chunks = []
    start = 0
    idx = 0
    while start < len(tokens):
        end = min(start + CHUNK_SIZE, len(tokens))
        chunk_str = enc.decode(tokens[start:end])
        chunk_id = hashlib.md5((source_id + '_' + str(idx)).encode()).hexdigest()
        chunks.append({
            'id': chunk_id,
            'text': chunk_str,
            'metadata': {
                'source_id': source_id,
                'lens': lens, 'topic': topic, 'title': title,
                'chunk_index': idx,
                'last_updated': updated_at[:10] if updated_at else datetime.now().strftime('%Y-%m-%d'),
                'content': chunk_str[:400]
            }
        })
        start += CHUNK_SIZE - CHUNK_OVERLAP
        idx += 1
    if not chunks:
        return 0
    # Embed
    all_embeddings = []
    texts = [c['text'] for c in chunks]
    for i in range(0, len(texts), 100):
        resp = openai_client.embeddings.create(model=EMBEDDING_MODEL, input=texts[i:i+100])
        all_embeddings.extend([item.embedding for item in resp.data])
        time.sleep(0.3)
    vectors = [{'id': c['id'], 'values': emb, 'metadata': c['metadata']}
               for c, emb in zip(chunks, all_embeddings)]
    for i in range(0, len(vectors), BATCH_SIZE):
        index.upsert(vectors=vectors[i:i+BATCH_SIZE])
        time.sleep(0.3)
    return len(chunks)

def delete_from_pinecone(source_id):
    try:
        index.delete(filter={'source_id': {'$eq': source_id}})
    except Exception as e:
        print('  Warning: delete failed: ' + str(e))


# ── LAYER 1 — GOVUK SEARCH API ────────────────────────────────────────

def discover_govuk_content(days_back=7):
    """
    Automatically discovers ALL relevant GOV.UK content updated in the last N days.
    No need to maintain a list of GOV.UK URLs — this finds everything automatically.
    """
    print('\n' + '=' * 60)
    print('Layer 1 — GOV.UK Search API Discovery')
    print('Finding all GOV.UK content updated in the last ' + str(days_back) + ' days')
    print('=' * 60)

    timestamps = load_json(TIMESTAMPS_FILE)
    discovered = load_json(DISCOVERED_URLS_FILE)
    cutoff_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%dT%H:%M:%SZ')

    total_indexed = 0
    total_found = 0

    for query_config in GOVUK_SEARCH_QUERIES:
        query = query_config['query']
        lens = query_config['lens']

        print('\nSearching: "' + query + '" [' + lens.upper() + ']')

        try:
            # GOV.UK Search API — finds new and updated content automatically
            params = {
                'q': query,
                'order': '-public_timestamp',
                'count': 20,
                'fields[]': ['title', 'link', 'public_timestamp',
                             'description', 'content_store_document_type']
            }

            r = requests.get(
                'https://www.gov.uk/api/search.json',
                params=params,
                headers=get_headers(),
                timeout=15
            )
            r.raise_for_status()
            data = r.json()
            results = data.get('results', [])

            new_count = 0
            for result in results:
                link = result.get('link', '')
                pub_timestamp = result.get('public_timestamp', '')
                title = result.get('title', link)

                if not link or not pub_timestamp:
                    continue

                # Only process if updated recently or not yet seen
                url = 'https://www.gov.uk' + link if link.startswith('/') else link
                source_id = 'govuk_search:' + link
                last_indexed = timestamps.get(source_id, '')

                # Skip if already indexed and not recently updated
                if last_indexed and pub_timestamp <= last_indexed:
                    continue

                # Check if content is within our date window
                if pub_timestamp < cutoff_date and source_id in discovered:
                    continue

                print('  Found: ' + title[:60] + ' (' + pub_timestamp[:10] + ')')
                total_found += 1

                # Fetch and index the content
                text, updated_at = fetch_web_scrape(url)
                if not text or len(text) < 100:
                    continue

                topic = 'govuk_' + lens + '_' + re.sub(r'[^a-z0-9]', '_', link.strip('/'))[:30]

                if source_id in timestamps and last_indexed:
                    delete_from_pinecone(source_id)
                    push_alert_to_supabase(
                        lens=lens, topic=topic,
                        title=title + ' — Updated',
                        alert_text=title + ' has been updated. Review how this change affects your ' + lens + ' rights.',
                        source_url=url, impact=score_impact(text)
                    )

                n_chunks = chunk_and_index(text, source_id, lens, topic, title, pub_timestamp)
                if n_chunks > 0:
                    timestamps[source_id] = pub_timestamp
                    discovered[source_id] = {'url': url, 'title': title,
                                             'lens': lens, 'indexed': datetime.now().isoformat()}
                    total_indexed += n_chunks
                    new_count += 1
                    print('    Indexed: ' + str(n_chunks) + ' chunks')

                time.sleep(0.5)

            if new_count == 0:
                print('  No new content found')

        except Exception as e:
            print('  Search error for "' + query + '": ' + str(e))

        time.sleep(1)

    save_json(TIMESTAMPS_FILE, timestamps)
    save_json(DISCOVERED_URLS_FILE, discovered)

    print('\n' + '=' * 60)
    print('GOV.UK Discovery complete')
    print('Content found: ' + str(total_found))
    print('Chunks indexed: ' + str(total_indexed))
    print('=' * 60)
    return total_indexed


# ── LAYER 2 — SITEMAP CRAWLING ────────────────────────────────────────

def crawl_sitemaps(days_back=7):
    """
    Automatically discovers ALL changed pages on ACAS, Shelter, Citizens Advice, NHS.
    No need to manually maintain URL lists for these sites ever again.
    """
    print('\n' + '=' * 60)
    print('Layer 2 — Sitemap Crawling')
    print('Checking ACAS, Shelter, Citizens Advice, NHS, Legislation')
    print('=' * 60)

    timestamps = load_json(TIMESTAMPS_FILE)
    content_hashes = load_json(CONTENT_HASHES_FILE)
    cutoff_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
    total_indexed = 0

    for sitemap_config in SITEMAPS:
        sitemap_url = sitemap_config['url']
        name = sitemap_config['name']
        default_lens = sitemap_config['lens']
        keywords = sitemap_config['keywords']

        print('\nCrawling sitemap: ' + name)

        try:
            r = requests.get(sitemap_url, headers=get_headers(), timeout=20)
            r.raise_for_status()

            # Parse sitemap XML
            # Handle sitemap index files (which link to other sitemaps)
            root = ET.fromstring(r.content)
            ns = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

            # Check if this is a sitemap index
            sitemaps_in_index = root.findall('.//sm:sitemap/sm:loc', ns)
            urls_to_check = []

            if sitemaps_in_index:
                # This is a sitemap index — fetch child sitemaps
                print('  Sitemap index found — checking ' + str(len(sitemaps_in_index)) + ' child sitemaps')
                for child_loc in sitemaps_in_index[:10]:  # Limit to first 10 child sitemaps
                    try:
                        child_r = requests.get(child_loc.text, headers=get_headers(), timeout=15)
                        child_r.raise_for_status()
                        child_root = ET.fromstring(child_r.content)
                        for url_el in child_root.findall('.//sm:url', ns):
                            loc = url_el.find('sm:loc', ns)
                            lastmod = url_el.find('sm:lastmod', ns)
                            if loc is not None:
                                urls_to_check.append({
                                    'url': loc.text,
                                    'lastmod': lastmod.text[:10] if lastmod is not None and lastmod.text else None
                                })
                        time.sleep(0.5)
                    except Exception as e:
                        print('  Error fetching child sitemap: ' + str(e))
            else:
                # Regular sitemap — parse directly
                for url_el in root.findall('.//sm:url', ns):
                    loc = url_el.find('sm:loc', ns)
                    lastmod = url_el.find('sm:lastmod', ns)
                    if loc is not None:
                        urls_to_check.append({
                            'url': loc.text,
                            'lastmod': lastmod.text[:10] if lastmod is not None and lastmod.text else None
                        })

            print('  Total URLs in sitemap: ' + str(len(urls_to_check)))

            # Filter to relevant URLs only
            relevant_urls = []
            for url_entry in urls_to_check:
                url = url_entry['url']
                url_lower = url.lower()

                # Check if URL contains any relevant keywords
                if any(kw in url_lower for kw in keywords):
                    # Check if recently modified or never seen
                    lastmod = url_entry.get('lastmod')
                    if lastmod and lastmod < cutoff_date and url in timestamps:
                        continue  # Skip if old and already indexed
                    relevant_urls.append(url_entry)

            print('  Relevant URLs to check: ' + str(len(relevant_urls)))

            # Process each relevant URL
            for url_entry in relevant_urls[:50]:  # Limit to 50 per sitemap per run
                url = url_entry['url']
                lastmod = url_entry.get('lastmod')
                source_id = 'sitemap:' + url

                # Check if content has changed using hash
                text, updated_at = fetch_web_scrape(url)
                if not text or len(text) < 100:
                    continue

                current_hash = get_content_hash(text)
                previous_hash = content_hashes.get(url)

                if previous_hash and current_hash == previous_hash:
                    continue  # No change

                # Detect lens from URL and content
                combined = url.lower() + ' ' + text[:500].lower()
                lens = detect_lens_from_text(combined)

                # Generate title from URL
                url_parts = url.rstrip('/').split('/')
                title = name + ' — ' + url_parts[-1].replace('-', ' ').title()
                topic = 'sitemap_' + re.sub(r'[^a-z0-9]', '_', url_parts[-1])[:30]

                print('  Processing: ' + title[:60])

                if previous_hash:
                    # Content changed
                    print('    CHANGED — re-indexing')
                    delete_from_pinecone(source_id)
                    push_alert_to_supabase(
                        lens=lens, topic=topic,
                        title=title + ' — Updated',
                        alert_text=title + ' has been updated. Review how this affects your ' + lens + ' rights.',
                        source_url=url, impact='Medium'
                    )
                else:
                    print('    New page — indexing')

                n_chunks = chunk_and_index(text, source_id, lens, topic, title,
                                          lastmod or datetime.now().isoformat())
                if n_chunks > 0:
                    content_hashes[url] = current_hash
                    timestamps[source_id] = lastmod or datetime.now().isoformat()
                    total_indexed += n_chunks
                    print('    Indexed: ' + str(n_chunks) + ' chunks')

                time.sleep(0.5)

        except Exception as e:
            print('  Sitemap error for ' + name + ': ' + str(e))

    save_json(TIMESTAMPS_FILE, timestamps)
    save_json(CONTENT_HASHES_FILE, content_hashes)

    print('\n' + '=' * 60)
    print('Sitemap crawling complete')
    print('Total chunks indexed: ' + str(total_indexed))
    print('=' * 60)
    return total_indexed


# ── LAYER 3 — AI URL SELF-HEALING ────────────────────────────────────

def heal_broken_urls():
    """
    Checks all static URLs for 404 errors.
    Uses Claude to automatically find replacement URLs.
    Self-heals the source list without manual intervention.
    """
    print('\n' + '=' * 60)
    print('Layer 3 — AI URL Self-Healing')
    print('Checking all static sources for broken URLs')
    print('=' * 60)

    broken_urls = load_json(BROKEN_URLS_FILE)
    healed_count = 0
    broken_count = 0

    for source in STATIC_SOURCES:
        url = source['url']
        title = source.get('title', url)

        try:
            r = requests.head(url, headers=get_headers(), timeout=10, allow_redirects=True)
            if r.status_code == 404:
                print('BROKEN (404): ' + title)
                print('  URL: ' + url)
                broken_count += 1

                # Ask Claude to find the replacement URL
                if ANTHROPIC_API_KEY:
                    try:
                        heal_response = requests.post(
                            'https://api.anthropic.com/v1/messages',
                            json={
                                'model': 'claude-sonnet-4-6',
                                'max_tokens': 200,
                                'messages': [{
                                    'role': 'user',
                                    'content': (
                                        'The following UK government or advice organisation URL is returning a 404 error: ' + url + '\n'
                                        'The page title was: ' + title + '\n'
                                        'Please suggest the most likely current URL for this page on the same website. '
                                        'Return ONLY the URL, nothing else. If you cannot determine a replacement URL return "UNKNOWN".'
                                    )
                                }]
                            },
                            headers={
                                'x-api-key': ANTHROPIC_API_KEY,
                                'anthropic-version': '2023-06-01',
                                'Content-Type': 'application/json'
                            },
                            timeout=15
                        )
                        if heal_response.status_code == 200:
                            suggested_url = heal_response.json()['content'][0]['text'].strip()
                            if suggested_url != 'UNKNOWN' and suggested_url.startswith('http'):
                                # Verify the suggested URL works
                                verify = requests.head(suggested_url, headers=get_headers(),
                                                      timeout=10, allow_redirects=True)
                                if verify.status_code == 200:
                                    print('  HEALED: ' + suggested_url)
                                    broken_urls[url] = {
                                        'original': url,
                                        'replacement': suggested_url,
                                        'title': title,
                                        'healed_at': datetime.now().isoformat()
                                    }
                                    healed_count += 1
                                else:
                                    print('  Suggested URL also broken: ' + suggested_url)
                                    broken_urls[url] = {
                                        'original': url,
                                        'replacement': None,
                                        'title': title,
                                        'checked_at': datetime.now().isoformat()
                                    }
                            else:
                                print('  Claude could not find replacement')
                                broken_urls[url] = {
                                    'original': url,
                                    'replacement': None,
                                    'title': title,
                                    'checked_at': datetime.now().isoformat()
                                }
                    except Exception as e:
                        print('  Claude error: ' + str(e))
                else:
                    broken_urls[url] = {
                        'original': url, 'replacement': None,
                        'title': title,
                        'checked_at': datetime.now().isoformat()
                    }

            elif r.status_code == 200:
                # URL is working — remove from broken list if it was there
                if url in broken_urls:
                    del broken_urls[url]
                    print('RECOVERED: ' + title)

        except Exception as e:
            print('Error checking ' + url + ': ' + str(e))

        time.sleep(0.5)

    save_json(BROKEN_URLS_FILE, broken_urls)

    print('\n' + '=' * 60)
    print('URL healing complete')
    print('Broken URLs found: ' + str(broken_count))
    print('URLs healed: ' + str(healed_count))
    if broken_count > 0:
        print('\nBroken URLs saved to: ' + BROKEN_URLS_FILE)
        print('Review this file to manually fix any URLs Claude could not heal.')
    print('=' * 60)
    return broken_count, healed_count


# ── RSS MONITORING ────────────────────────────────────────────────────

def monitor_rss_feeds():
    print('=' * 60)
    print('Quantum Lens — Real-Time RSS Monitor')
    print('Time: ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print('=' * 60)

    state = load_json(MONITOR_STATE_FILE) or {'seen_entries': []}
    seen_entries = set(state.get('seen_entries', []))
    alerts_pushed = 0
    new_seen = []

    for feed_config in RSS_FEEDS:
        feed_url = feed_config['url']
        feed_name = feed_config['name']
        keywords = feed_config['keywords']
        print('\nChecking: ' + feed_name)
        try:
            feed = feedparser.parse(feed_url)
            new_entries = []
            for entry in feed.entries[:20]:
                entry_id = entry.get('id', entry.get('link', ''))
                if entry_id in seen_entries:
                    continue
                title = entry.get('title', '')
                summary = entry.get('summary', '')
                combined = (title + ' ' + summary).lower()
                matched = [kw for kw in keywords if kw in combined]
                if not matched:
                    new_seen.append(entry_id)
                    continue
                lens = detect_lens_from_text(combined)
                impact = score_impact(combined)
                source_url = entry.get('link', feed_url)
                alert_text = (title + '. This update may affect your ' + lens +
                             ' rights. Review how this change applies to your situation.')
                print('  NEW: ' + title)
                print('  Lens: ' + lens.upper() + ' | Impact: ' + impact)
                if push_alert_to_supabase(
                    lens=lens,
                    topic='rss_' + lens + '_' + datetime.now().strftime('%Y%m%d%H%M'),
                    title=title, alert_text=alert_text,
                    source_url=source_url, impact=impact
                ):
                    alerts_pushed += 1
                new_seen.append(entry_id)
                new_entries.append(entry_id)
            if not new_entries:
                print('  No new relevant updates')
        except Exception as e:
            print('  Error: ' + str(e))

    all_seen = list(seen_entries) + new_seen
    state['seen_entries'] = all_seen[-1000:]
    state['last_check'] = datetime.now().isoformat()
    save_json(MONITOR_STATE_FILE, state)

    print('\n' + '=' * 60)
    print('RSS Monitor complete — ' + str(alerts_pushed) + ' alerts pushed to Supabase')
    print('Next check: run again in 15 minutes')
    print('=' * 60)


# ── HASH MONITORING ───────────────────────────────────────────────────

HASH_MONITORED_PAGES = [
    {'url': 'https://www.acas.org.uk/dismissing-staff',           'lens': 'employment', 'topic': 'acas_dismissal',      'title': 'ACAS — Dismissing staff'},
    {'url': 'https://www.acas.org.uk/disciplinary-procedure-and-the-code', 'lens': 'employment', 'topic': 'acas_disciplinary', 'title': 'ACAS Code of Practice'},
    {'url': 'https://www.acas.org.uk/redundancy',                 'lens': 'employment', 'topic': 'acas_redundancy',     'title': 'ACAS — Redundancy'},
    {'url': 'https://www.acas.org.uk/discrimination-and-the-law', 'lens': 'employment', 'topic': 'acas_discrimination', 'title': 'ACAS — Discrimination'},
    {'url': 'https://www.acas.org.uk/reasonable-adjustments',     'lens': 'employment', 'topic': 'acas_adjustments',    'title': 'ACAS — Reasonable adjustments'},
    {'url': 'https://www.acas.org.uk/holiday-entitlement',        'lens': 'employment', 'topic': 'acas_holiday',        'title': 'ACAS — Holiday entitlement'},
    {'url': 'https://www.acas.org.uk/performance-management',     'lens': 'employment', 'topic': 'acas_performance',    'title': 'ACAS — Performance management'},
    {'url': 'https://www.acas.org.uk/flexible-working',           'lens': 'employment', 'topic': 'acas_flexible',       'title': 'ACAS — Flexible working'},
    {'url': 'https://www.acas.org.uk/whistleblowing-at-work',     'lens': 'employment', 'topic': 'acas_whistleblowing', 'title': 'ACAS — Whistleblowing at work'},
    {'url': 'https://www.acas.org.uk/absence-from-work',          'lens': 'employment', 'topic': 'acas_sick',           'title': 'ACAS — Absence from work'},
    {'url': 'https://england.shelter.org.uk/housing_advice/eviction', 'lens': 'housing', 'topic': 'shelter_eviction',   'title': 'Shelter — Eviction rights'},
    {'url': 'https://england.shelter.org.uk/housing_advice/private_renting', 'lens': 'housing', 'topic': 'shelter_renting', 'title': 'Shelter — Private renting'},
    {'url': 'https://england.shelter.org.uk/housing_advice/homelessness', 'lens': 'housing', 'topic': 'shelter_homelessness', 'title': 'Shelter — Homelessness'},
    {'url': 'https://www.citizensadvice.org.uk/work/',             'lens': 'employment', 'topic': 'ca_employment',      'title': 'Citizens Advice — Work'},
    {'url': 'https://www.citizensadvice.org.uk/housing/renting-a-home/', 'lens': 'housing', 'topic': 'ca_renting',       'title': 'Citizens Advice — Renting'},
    {'url': 'https://www.citizensadvice.org.uk/benefits/',         'lens': 'benefits',   'topic': 'ca_benefits',        'title': 'Citizens Advice — Benefits'},
    {'url': 'https://www.citizensadvice.org.uk/immigration/',      'lens': 'immigration','topic': 'ca_immigration',     'title': 'Citizens Advice — Immigration'},
    {'url': 'https://www.nhs.uk/nhs-services/gps/',               'lens': 'health',     'topic': 'nhs_gp',             'title': 'NHS — GP services'},
    {'url': 'https://ico.org.uk/for-organisations/guide-to-data-protection/', 'lens': 'business', 'topic': 'ico_gdpr',  'title': 'ICO — Data protection guide'},
    {'url': 'https://www.financial-ombudsman.org.uk/consumers/complaints-can-help', 'lens': 'consumer', 'topic': 'fos', 'title': 'Financial Ombudsman'},
    {'url': 'https://www.moneyhelper.org.uk/en/money-troubles/dealing-with-debt', 'lens': 'finance', 'topic': 'moneyhelper', 'title': 'MoneyHelper — Debt advice'},
    {'url': 'https://www.thepensionsregulator.gov.uk/en/employers', 'lens': 'business', 'topic': 'pensions_reg',        'title': 'Pensions Regulator — Employers'},
    {'url': 'https://www.gov.uk/guidance/rates-and-thresholds-for-employers-2026-to-2027', 'lens': 'finance', 'topic': 'hmrc_rates', 'title': 'HMRC — Employer rates 2026-2027'},
    {'url': 'https://www.gov.uk/guidance/immigration-rules/immigration-rules-index', 'lens': 'immigration', 'topic': 'immigration_rules', 'title': 'Immigration Rules Index'},
]

def monitor_hash_pages():
    print('\n' + '-' * 60)
    print('Hash Monitor — ACAS, Shelter, Citizens Advice, NHS, ICO,')
    print('               Financial Ombudsman, MoneyHelper, Pensions Reg')
    print('-' * 60)

    hashes = load_json(CONTENT_HASHES_FILE)
    timestamps = load_json(TIMESTAMPS_FILE)
    changes_detected = 0

    for page in HASH_MONITORED_PAGES:
        url = page['url']
        lens = page['lens']
        topic = page['topic']
        title = page['title']
        print('Checking: ' + title)
        text, _ = fetch_web_scrape(url)
        if not text or len(text) < 100:
            print('  Failed — no content')
            continue
        current_hash = get_content_hash(text)
        previous_hash = hashes.get(url)
        if previous_hash and current_hash != previous_hash:
            print('  CHANGED — re-indexing and pushing alert')
            delete_from_pinecone(url)
            n = chunk_and_index(text, url, lens, topic, title, datetime.now().isoformat())
            push_alert_to_supabase(lens=lens, topic=topic,
                title=title + ' — Updated',
                alert_text=title + ' has been updated. Review how this affects your ' + lens + ' rights.',
                source_url=url, impact='Medium')
            changes_detected += 1
        elif not previous_hash:
            print('  First check — hash stored')
        else:
            print('  No change')
        hashes[url] = current_hash
        time.sleep(0.5)

    save_json(CONTENT_HASHES_FILE, hashes)
    print('\nHash monitor complete — ' + str(changes_detected) + ' changes detected')
    return changes_detected


# ── STATIC SOURCE INGESTION ───────────────────────────────────────────

def ingest_static_sources(update_only=False):
    print('\n' + '=' * 60)
    print('Static Sources — Core legislation and specialist sites')
    print('Sources: ' + str(len(STATIC_SOURCES)))
    print('=' * 60)

    timestamps = load_json(TIMESTAMPS_FILE)
    broken_urls = load_json(BROKEN_URLS_FILE)
    total_chunks = 0
    skipped = 0
    failed = 0

    for i, source in enumerate(STATIC_SOURCES):
        url = source['url']
        lens = source['lens']
        topic = source['topic']
        title = source.get('title', url)
        source_id = url

        print('[' + str(i+1) + '/' + str(len(STATIC_SOURCES)) + '] ' + title)

        # Check if we have a healed replacement URL
        if url in broken_urls and broken_urls[url].get('replacement'):
            actual_url = broken_urls[url]['replacement']
            print('  Using healed URL: ' + actual_url)
        else:
            actual_url = url

        text, updated_at = fetch_web_scrape(actual_url)
        if not text or len(text) < 50:
            print('  FAILED')
            failed += 1
            print()
            continue

        last_updated = timestamps.get(source_id)
        if update_only and last_updated and updated_at:
            content_hash = get_content_hash(text)
            if timestamps.get(source_id + '_hash') == content_hash:
                print('  No change — skipping')
                skipped += 1
                print()
                continue

        if last_updated:
            delete_from_pinecone(source_id)
            push_alert_to_supabase(
                lens=lens, topic=topic,
                title=title + ' — Updated',
                alert_text=title + ' has been updated.',
                source_url=actual_url, impact=score_impact(text)
            )

        n_chunks = chunk_and_index(text, source_id, lens, topic, title, updated_at or '')
        print('  ' + str(len(text)) + ' chars -> ' + str(n_chunks) + ' chunks — Indexed OK')
        timestamps[source_id] = updated_at or datetime.now().isoformat()
        timestamps[source_id + '_hash'] = get_content_hash(text)
        total_chunks += n_chunks
        print()
        time.sleep(0.5)

    save_json(TIMESTAMPS_FILE, timestamps)

    print('=' * 60)
    print('Static ingestion complete')
    print('Chunks indexed: ' + str(total_chunks))
    print('Skipped:        ' + str(skipped))
    print('Failed:         ' + str(failed))
    print('=' * 60)
    return total_chunks


# ── MAIN ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Quantum Lens Regulatory Monitor v2.0')
    parser.add_argument('--monitor',      action='store_true', help='RSS monitoring — run every 15 minutes')
    parser.add_argument('--hash-check',   action='store_true', help='Hash monitoring — run every 4 hours')
    parser.add_argument('--full-monitor', action='store_true', help='Run both RSS and hash monitoring')
    parser.add_argument('--update-only',  action='store_true', help='Check all sources for changes')
    parser.add_argument('--heal-urls',    action='store_true', help='Check and fix all broken URLs using AI')
    parser.add_argument('--days',         type=int, default=7, help='Days back for discovery (default 7)')
    args = parser.parse_args()

    if args.monitor:
        monitor_rss_feeds()

    elif args.hash_check:
        monitor_hash_pages()

    elif args.full_monitor:
        monitor_rss_feeds()
        monitor_hash_pages()

    elif args.heal_urls:
        heal_broken_urls()

    elif args.update_only:
        print('=' * 60)
        print('Quantum Lens — Full Update (all three layers)')
        print('=' * 60)
        # Layer 1 — GOV.UK Search API
        discover_govuk_content(days_back=args.days)
        # Layer 2 — Sitemap crawling
        crawl_sitemaps(days_back=args.days)
        # Layer 3 — Static sources with URL healing
        ingest_static_sources(update_only=True)

        print('\n' + '=' * 60)
        print('Full update complete')
        print('Run --monitor every 15 min for real-time RSS detection')
        print('Run --hash-check every 4 hours for non-RSS site detection')
        print('Run --update-only weekly for full source refresh')
        print('Run --heal-urls monthly to fix any broken URLs')
        print('=' * 60)

    else:
        # Full ingestion — all three layers
        print('=' * 60)
        print('Quantum Lens — Full Ingestion (all three layers)')
        print('=' * 60)
        # Layer 3 first — heal any broken URLs before ingesting
        heal_broken_urls()
        # Layer 1 — GOV.UK Search API
        discover_govuk_content(days_back=30)
        # Layer 2 — Sitemap crawling
        crawl_sitemaps(days_back=30)
        # Static sources
        ingest_static_sources(update_only=False)

        print('\n' + '=' * 60)
        print('Full ingestion complete')
        print('Run --monitor every 15 min for real-time RSS detection')
        print('Run --hash-check every 4 hours for non-RSS site detection')
        print('Run --update-only weekly for full source refresh')
        print('Run --heal-urls monthly to fix any broken URLs')
        print('=' * 60)


if __name__ == '__main__':
    main()
