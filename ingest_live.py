"""
Quantum Lens — Real-Time Regulatory Monitoring System
Monitors 100+ official UK sources and pushes instant alerts to users.

Modes:
  python ingest_live.py --full          Full ingestion of all sources
  python ingest_live.py --monitor       Real-time monitoring (run every 15 min)
  python ingest_live.py --update-only   Weekly full update

Requirements:
  pip install pinecone openai requests beautifulsoup4 tiktoken python-dotenv feedparser
"""

import os
import json
import time
import hashlib
import argparse
import requests
import feedparser
import re
from bs4 import BeautifulSoup
from openai import OpenAI
from pinecone import Pinecone
from dotenv import load_dotenv
import tiktoken
from datetime import datetime, timezone

load_dotenv()

openai_client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
pc = Pinecone(api_key=os.getenv('PINECONE_API_KEY'))
index = pc.Index(os.getenv('PINECONE_INDEX_NAME', 'quantum-lens-regulatory'))

SUPABASE_URL = os.getenv('SUPABASE_URL', '')
SUPABASE_SERVICE_KEY = os.getenv('SUPABASE_SERVICE_KEY', '')

EMBEDDING_MODEL = 'text-embedding-3-small'
CHUNK_SIZE = 400
CHUNK_OVERLAP = 60
BATCH_SIZE = 50
TIMESTAMPS_FILE = 'regulatory_timestamps.json'
MONITOR_STATE_FILE = 'monitor_state.json'

# ── RSS FEEDS — checked every 15 minutes ─────────────────────────────
RSS_FEEDS = [
    # GOV.UK official feeds
    {
        'url': 'https://www.gov.uk/government/feed',
        'name': 'GOV.UK — All updates',
        'keywords': ['employment', 'immigration', 'housing', 'benefits',
                     'minimum wage', 'visa', 'skilled worker', 'tenancy',
                     'universal credit', 'national insurance', 'tax',
                     'right to work', 'dismissal', 'redundancy']
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
    # HMRC
    {
        'url': 'https://www.gov.uk/government/organisations/hm-revenue-customs.atom',
        'name': 'HMRC — Updates',
        'keywords': ['tax', 'national insurance', 'minimum wage', 'self assessment', 'vat']
    },
    # Home Office
    {
        'url': 'https://www.gov.uk/government/organisations/home-office.atom',
        'name': 'Home Office — Updates',
        'keywords': ['immigration', 'visa', 'skilled worker', 'right to work', 'asylum']
    },
    # ICO
    {
        'url': 'https://ico.org.uk/about-the-ico/media-centre/news-and-blogs/rss/',
        'name': 'ICO — Data protection news',
        'keywords': ['gdpr', 'data protection', 'personal data', 'ico']
    },
    # HSE
    {
        'url': 'https://www.hse.gov.uk/press/rssfeed.xml',
        'name': 'HSE — Health and safety',
        'keywords': ['workplace', 'health and safety', 'employer', 'worker']
    },
    # FCA
    {
        'url': 'https://www.fca.org.uk/news/rss.xml',
        'name': 'FCA — Financial regulation',
        'keywords': ['consumer', 'financial', 'insurance', 'credit', 'bank']
    },
]

# ── KEYWORD TO LENS MAPPING ───────────────────────────────────────────
KEYWORD_LENS_MAP = {
    'employment': 'employment',
    'minimum wage': 'employment',
    'nmw': 'employment',
    'national living wage': 'employment',
    'dismissal': 'employment',
    'redundancy': 'employment',
    'workplace': 'employment',
    'worker': 'employment',
    'employer': 'employment',
    'acas': 'employment',
    'tribunal': 'employment',
    'discrimination': 'employment',
    'immigration': 'immigration',
    'visa': 'immigration',
    'skilled worker': 'immigration',
    'right to work': 'immigration',
    'asylum': 'immigration',
    'home office': 'immigration',
    'ukvi': 'immigration',
    'biometric': 'immigration',
    'housing': 'housing',
    'tenancy': 'housing',
    'landlord': 'housing',
    'eviction': 'housing',
    'rent': 'housing',
    'section 21': 'housing',
    'shelter': 'housing',
    'benefits': 'benefits',
    'universal credit': 'benefits',
    'pip': 'benefits',
    'dwp': 'benefits',
    'jobseeker': 'benefits',
    'tax credit': 'benefits',
    'tax': 'finance',
    'national insurance': 'finance',
    'self assessment': 'finance',
    'vat': 'finance',
    'hmrc': 'finance',
    'gdpr': 'business',
    'data protection': 'business',
    'ico': 'business',
    'companies house': 'business',
    'consumer': 'consumer',
    'financial': 'consumer',
    'fca': 'consumer',
    'nhs': 'health',
    'health': 'health',
    'mental health': 'health',
    'cqc': 'health',
}

# ── ALL OFFICIAL SOURCES ──────────────────────────────────────────────
SOURCES = [
    # ── EMPLOYMENT ──────────────────────────────────────────────────
    {'govuk_path': 'national-minimum-wage-rates',                    'lens': 'employment', 'topic': 'national_minimum_wage'},
    {'govuk_path': 'redundancy-your-rights',                         'lens': 'employment', 'topic': 'redundancy'},
    {'govuk_path': 'discrimination-your-rights',                     'lens': 'employment', 'topic': 'discrimination'},
    {'govuk_path': 'maternity-pay-leave',                            'lens': 'employment', 'topic': 'maternity_rights'},
    {'govuk_path': 'paternity-pay-leave',                            'lens': 'employment', 'topic': 'paternity_rights'},
    {'govuk_path': 'shared-parental-leave-and-pay',                  'lens': 'employment', 'topic': 'shared_parental_leave'},
    {'govuk_path': 'employee-rights-flexible-working',               'lens': 'employment', 'topic': 'flexible_working'},
    {'govuk_path': 'employment-status',                              'lens': 'employment', 'topic': 'employment_status'},
    {'govuk_path': 'dismissal',                                      'lens': 'employment', 'topic': 'dismissal'},
    {'govuk_path': 'raise-grievance-at-work',                        'lens': 'employment', 'topic': 'grievance'},
    {'govuk_path': 'workplace-bullying-and-harassment',              'lens': 'employment', 'topic': 'harassment'},
    {'govuk_path': 'reasonable-adjustments-for-disabled-workers',    'lens': 'employment', 'topic': 'reasonable_adjustments'},
    {'govuk_path': 'working-time-regulation',                        'lens': 'employment', 'topic': 'working_time'},
    {'govuk_path': 'tupe-transfers-your-rights',                     'lens': 'employment', 'topic': 'tupe'},
    {'govuk_path': 'taking-sick-leave',                              'lens': 'employment', 'topic': 'sick_leave'},
    {'govuk_path': 'statutory-sick-pay',                             'lens': 'employment', 'topic': 'statutory_sick_pay'},
    {'govuk_path': 'zero-hours-contracts',                           'lens': 'employment', 'topic': 'zero_hours'},
    {'govuk_path': 'employment-tribunals',                           'lens': 'employment', 'topic': 'employment_tribunals'},
    {'govuk_path': 'holiday-entitlement',                            'lens': 'employment', 'topic': 'holiday_entitlement'},
    {'govuk_path': 'notice-period',                                  'lens': 'employment', 'topic': 'notice_period'},
    # ACAS
    {'url': 'https://www.acas.org.uk/dismissal',                     'lens': 'employment', 'topic': 'acas_dismissal',    'title': 'ACAS — Dismissal'},
    {'url': 'https://www.acas.org.uk/disciplinary-procedure-and-the-code', 'lens': 'employment', 'topic': 'acas_disciplinary', 'title': 'ACAS Code — Disciplinary'},
    {'url': 'https://www.acas.org.uk/performance-management',        'lens': 'employment', 'topic': 'acas_performance',  'title': 'ACAS — Performance management'},
    {'url': 'https://www.acas.org.uk/redundancy',                    'lens': 'employment', 'topic': 'acas_redundancy',   'title': 'ACAS — Redundancy'},
    {'url': 'https://www.acas.org.uk/discrimination-and-the-law',    'lens': 'employment', 'topic': 'acas_discrimination','title': 'ACAS — Discrimination'},
    {'url': 'https://www.acas.org.uk/maternity-and-paternity',       'lens': 'employment', 'topic': 'acas_maternity',    'title': 'ACAS — Maternity and paternity'},
    {'url': 'https://www.acas.org.uk/reasonable-adjustments',        'lens': 'employment', 'topic': 'acas_adjustments',  'title': 'ACAS — Reasonable adjustments'},
    {'url': 'https://www.acas.org.uk/flexible-working',              'lens': 'employment', 'topic': 'acas_flexible',     'title': 'ACAS — Flexible working'},
    {'url': 'https://www.acas.org.uk/holiday-entitlement',           'lens': 'employment', 'topic': 'acas_holiday',      'title': 'ACAS — Holiday entitlement'},
    {'url': 'https://www.acas.org.uk/sick-leave-and-fit-notes',      'lens': 'employment', 'topic': 'acas_sick',         'title': 'ACAS — Sick leave'},
    {'url': 'https://www.acas.org.uk/whistleblowing',                'lens': 'employment', 'topic': 'acas_whistleblowing','title': 'ACAS — Whistleblowing'},
    # EHRC
    {'url': 'https://www.equalityhumanrights.com/equality/equality-act-2010/your-rights-under-equality-act-2010', 'lens': 'employment', 'topic': 'equality_act', 'title': 'EHRC — Equality Act rights'},
    # HSE
    {'url': 'https://www.hse.gov.uk/workers/index.htm',              'lens': 'employment', 'topic': 'hse_workers',       'title': 'HSE — Worker health and safety'},
    # Legislation
    {'url': 'https://www.legislation.gov.uk/ukpga/1996/18/contents', 'lens': 'employment', 'topic': 'employment_rights_act', 'title': 'Employment Rights Act 1996'},
    {'url': 'https://www.legislation.gov.uk/ukpga/2010/15/contents', 'lens': 'employment', 'topic': 'equality_act_2010', 'title': 'Equality Act 2010'},
    # Citizens Advice
    {'url': 'https://www.citizensadvice.org.uk/work/rights-at-work/', 'lens': 'employment', 'topic': 'ca_employment',   'title': 'Citizens Advice — Rights at work'},
    {'url': 'https://www.citizensadvice.org.uk/work/dismissal/',      'lens': 'employment', 'topic': 'ca_dismissal',     'title': 'Citizens Advice — Dismissal'},

    # ── IMMIGRATION ─────────────────────────────────────────────────
    {'govuk_path': 'skilled-worker-visa',                            'lens': 'immigration', 'topic': 'skilled_worker_visa'},
    {'govuk_path': 'skilled-worker-visa/your-job',                   'lens': 'immigration', 'topic': 'skilled_worker_job'},
    {'govuk_path': 'skilled-worker-visa/salary-requirements',        'lens': 'immigration', 'topic': 'skilled_worker_salary'},
    {'govuk_path': 'biometric-residence-permits',                    'lens': 'immigration', 'topic': 'brp'},
    {'govuk_path': 'sponsor-workers',                                'lens': 'immigration', 'topic': 'sponsor_licence'},
    {'govuk_path': 'check-applicant-right-to-work',                  'lens': 'immigration', 'topic': 'right_to_work'},
    {'govuk_path': 'innovator-founder-visa',                         'lens': 'immigration', 'topic': 'innovator_founder_visa'},
    {'govuk_path': 'indefinite-leave-to-remain-skilled-worker',      'lens': 'immigration', 'topic': 'ilr_skilled_worker'},
    {'govuk_path': 'student-visa',                                   'lens': 'immigration', 'topic': 'student_visa'},
    {'govuk_path': 'graduate-visa',                                  'lens': 'immigration', 'topic': 'graduate_visa'},
    {'govuk_path': 'family-visa',                                    'lens': 'immigration', 'topic': 'family_visa'},
    {'govuk_path': 'settled-status-eu-citizens-families',            'lens': 'immigration', 'topic': 'eu_settled_status'},
    {'url': 'https://www.gov.uk/guidance/immigration-rules/immigration-rules-index', 'lens': 'immigration', 'topic': 'immigration_rules', 'title': 'Home Office Immigration Rules'},
    {'url': 'https://www.legislation.gov.uk/ukpga/2014/22/contents', 'lens': 'immigration', 'topic': 'immigration_act_2014', 'title': 'Immigration Act 2014'},
    {'url': 'https://www.citizensadvice.org.uk/immigration/',        'lens': 'immigration', 'topic': 'ca_immigration',  'title': 'Citizens Advice — Immigration'},

    # ── HOUSING ─────────────────────────────────────────────────────
    {'govuk_path': 'private-renting',                                'lens': 'housing', 'topic': 'private_renting'},
    {'govuk_path': 'eviction-section-21',                            'lens': 'housing', 'topic': 'section_21'},
    {'govuk_path': 'eviction-section-8',                             'lens': 'housing', 'topic': 'section_8'},
    {'govuk_path': 'tenancy-agreements',                             'lens': 'housing', 'topic': 'tenancy_agreements'},
    {'govuk_path': 'deposit-protection-schemes-and-landlord-responsibilities', 'lens': 'housing', 'topic': 'deposit_protection'},
    {'govuk_path': 'housing-benefit',                                'lens': 'housing', 'topic': 'housing_benefit'},
    {'govuk_path': 'local-housing-allowance',                        'lens': 'housing', 'topic': 'lha'},
    {'govuk_path': 'homelessness-help-from-council',                 'lens': 'housing', 'topic': 'homelessness'},
    {'url': 'https://england.shelter.org.uk/housing_advice/eviction','lens': 'housing', 'topic': 'shelter_eviction',   'title': 'Shelter — Eviction rights'},
    {'url': 'https://england.shelter.org.uk/housing_advice/private_renting', 'lens': 'housing', 'topic': 'shelter_renting', 'title': 'Shelter — Private renting'},
    {'url': 'https://england.shelter.org.uk/housing_advice/repairs_and_bad_conditions', 'lens': 'housing', 'topic': 'shelter_repairs', 'title': 'Shelter — Repairs'},
    {'url': 'https://england.shelter.org.uk/housing_advice/homelessness', 'lens': 'housing', 'topic': 'shelter_homeless', 'title': 'Shelter — Homelessness'},
    {'url': 'https://www.citizensadvice.org.uk/housing/renting-a-home/', 'lens': 'housing', 'topic': 'ca_renting',    'title': 'Citizens Advice — Renting'},
    {'url': 'https://www.citizensadvice.org.uk/housing/eviction/',   'lens': 'housing', 'topic': 'ca_eviction',        'title': 'Citizens Advice — Eviction'},
    {'url': 'https://www.legislation.gov.uk/ukpga/1988/50/contents', 'lens': 'housing', 'topic': 'housing_act_1988',  'title': 'Housing Act 1988'},

    # ── BENEFITS ────────────────────────────────────────────────────
    {'govuk_path': 'universal-credit',                               'lens': 'benefits', 'topic': 'universal_credit'},
    {'govuk_path': 'universal-credit/what-youll-get',                'lens': 'benefits', 'topic': 'uc_amounts'},
    {'govuk_path': 'pip',                                            'lens': 'benefits', 'topic': 'pip'},
    {'govuk_path': 'child-benefit',                                  'lens': 'benefits', 'topic': 'child_benefit'},
    {'govuk_path': 'mandatory-reconsideration',                      'lens': 'benefits', 'topic': 'mandatory_reconsideration'},
    {'govuk_path': 'employment-support-allowance',                   'lens': 'benefits', 'topic': 'esa'},
    {'govuk_path': 'carers-allowance',                               'lens': 'benefits', 'topic': 'carers_allowance'},
    {'govuk_path': 'attendance-allowance',                           'lens': 'benefits', 'topic': 'attendance_allowance'},
    {'govuk_path': 'appeal-benefit-decision',                        'lens': 'benefits', 'topic': 'benefit_appeal'},
    {'url': 'https://www.citizensadvice.org.uk/benefits/',           'lens': 'benefits', 'topic': 'ca_benefits',       'title': 'Citizens Advice — Benefits'},
    {'url': 'https://www.citizensadvice.org.uk/benefits/universal-credit/', 'lens': 'benefits', 'topic': 'ca_uc',     'title': 'Citizens Advice — Universal Credit'},

    # ── FINANCE ─────────────────────────────────────────────────────
    {'govuk_path': 'self-assessment-tax-returns',                    'lens': 'finance', 'topic': 'self_assessment'},
    {'govuk_path': 'national-insurance',                             'lens': 'finance', 'topic': 'national_insurance'},
    {'govuk_path': 'pay-self-employment-ni',                         'lens': 'finance', 'topic': 'self_employed_ni'},
    {'govuk_path': 'vat-businesses',                                 'lens': 'finance', 'topic': 'vat'},
    {'govuk_path': 'income-tax',                                     'lens': 'finance', 'topic': 'income_tax'},
    {'govuk_path': 'corporation-tax',                                'lens': 'finance', 'topic': 'corporation_tax'},
    {'url': 'https://www.gov.uk/guidance/rates-and-thresholds-for-employers-2025-to-2026', 'lens': 'finance', 'topic': 'hmrc_rates', 'title': 'HMRC — Employer rates 2025-2026'},
    {'url': 'https://www.financial-ombudsman.org.uk/consumers/complaints-can-help', 'lens': 'finance', 'topic': 'financial_ombudsman', 'title': 'Financial Ombudsman'},
    {'url': 'https://www.fca.org.uk/consumers',                      'lens': 'finance', 'topic': 'fca_consumer',      'title': 'FCA — Consumer guidance'},

    # ── BUSINESS COMPLIANCE ─────────────────────────────────────────
    {'govuk_path': 'set-up-limited-company',                         'lens': 'business', 'topic': 'company_formation'},
    {'govuk_path': 'running-a-limited-company',                      'lens': 'business', 'topic': 'running_company'},
    {'govuk_path': 'set-up-self-employed',                           'lens': 'business', 'topic': 'self_employed_setup'},
    {'url': 'https://ico.org.uk/for-organisations/guide-to-data-protection/', 'lens': 'business', 'topic': 'ico_gdpr', 'title': 'ICO — Data protection guide'},
    {'url': 'https://ico.org.uk/for-organisations/guide-to-data-protection/guide-to-the-general-data-protection-regulation-gdpr/', 'lens': 'business', 'topic': 'ico_uk_gdpr', 'title': 'ICO — UK GDPR guide'},
    {'url': 'https://ico.org.uk/for-organisations/sme-web-hub/',     'lens': 'business', 'topic': 'ico_sme',          'title': 'ICO — SME guidance'},
    {'url': 'https://www.cqc.org.uk/guidance-providers/regulations', 'lens': 'business', 'topic': 'cqc_regulations',  'title': 'CQC — Regulations for providers'},
    {'url': 'https://www.acas.org.uk/acas-code-of-practice-for-disciplinary-and-grievance-procedures', 'lens': 'business', 'topic': 'acas_code', 'title': 'ACAS Code of Practice'},

    # ── CONSUMER AND DIGITAL RIGHTS ─────────────────────────────────
    {'govuk_path': 'consumer-rights-act-2015',                       'lens': 'consumer', 'topic': 'consumer_rights_act'},
    {'govuk_path': 'make-court-claim-for-money',                     'lens': 'consumer', 'topic': 'small_claims'},
    {'url': 'https://www.citizensadvice.org.uk/consumer/',           'lens': 'consumer', 'topic': 'ca_consumer',      'title': 'Citizens Advice — Consumer rights'},

    # ── HEALTH NAVIGATION ───────────────────────────────────────────
    {'govuk_path': 'rights-mental-health',                           'lens': 'health', 'topic': 'mental_health_rights'},
    {'govuk_path': 'nhs-services',                                   'lens': 'health', 'topic': 'nhs_services'},
    {'url': 'https://www.nhs.uk/nhs-services/gps/',                  'lens': 'health', 'topic': 'nhs_gp',             'title': 'NHS — GP services'},
    {'url': 'https://www.nhs.uk/using-the-nhs/about-the-nhs/nhs-constitution/', 'lens': 'health', 'topic': 'nhs_constitution', 'title': 'NHS Constitution'},
    {'url': 'https://www.citizensadvice.org.uk/health/',             'lens': 'health', 'topic': 'ca_health',          'title': 'Citizens Advice — Health rights'},
]


# ── Helper functions ──────────────────────────────────────────────────

def load_timestamps():
    if os.path.exists(TIMESTAMPS_FILE):
        with open(TIMESTAMPS_FILE) as f:
            return json.load(f)
    return {}

def save_timestamps(timestamps):
    with open(TIMESTAMPS_FILE, 'w') as f:
        json.dump(timestamps, f, indent=2)

def load_monitor_state():
    if os.path.exists(MONITOR_STATE_FILE):
        with open(MONITOR_STATE_FILE) as f:
            return json.load(f)
    return {'last_rss_check': {}, 'seen_entries': []}

def save_monitor_state(state):
    with open(MONITOR_STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)

def detect_lens_from_text(text):
    """Detect which regulatory lens an update belongs to."""
    text_lower = text.lower()
    lens_counts = {}
    for keyword, lens in KEYWORD_LENS_MAP.items():
        if keyword in text_lower:
            lens_counts[lens] = lens_counts.get(lens, 0) + 1
    if lens_counts:
        return max(lens_counts, key=lens_counts.get)
    return 'general'

def score_impact(text):
    """Score impact as High, Medium, or Low."""
    high = ['must', 'required', 'mandatory', 'penalty', 'fine', 'criminal',
            'prosecution', 'illegal', 'prohibited', 'deadline', 'expires', 'immediate']
    medium = ['should', 'recommended', 'updated', 'changed', 'increased',
              'decreased', 'amended', 'revised', 'new guidance']
    text_lower = text.lower()
    high_count = sum(1 for kw in high if kw in text_lower)
    medium_count = sum(1 for kw in medium if kw in text_lower)
    if high_count >= 2:
        return 'High'
    elif high_count >= 1 or medium_count >= 2:
        return 'Medium'
    return 'Low'

def push_alert_to_supabase(lens, topic, title, alert_text, source_url='', impact='Medium'):
    """Push a regulatory change alert to Supabase in real time."""
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        print(f'  Supabase not configured — alert not pushed: {title}')
        return False
    try:
        response = requests.post(
            f"{SUPABASE_URL}/rest/v1/regulatory_changes",
            json={
                'lens': lens,
                'topic': topic,
                'title': title,
                'alert_text': alert_text,
                'source_url': source_url,
                'impact': impact,
                'is_active': True,
                'created_at': datetime.now(timezone.utc).isoformat()
            },
            headers={
                'apikey': SUPABASE_SERVICE_KEY,
                'Authorization': f'Bearer {SUPABASE_SERVICE_KEY}',
                'Content-Type': 'application/json',
                'Prefer': 'return=minimal'
            },
            timeout=10
        )
        if response.status_code in [200, 201]:
            print(f'  ✓ Alert pushed to Supabase: [{lens.upper()}] {title}')
            return True
        else:
            print(f'  ✗ Failed to push alert: {response.status_code} — {response.text}')
            return False
    except Exception as e:
        print(f'  ✗ Supabase error: {e}')
        return False

def fetch_govuk_api(path):
    url = f'https://www.gov.uk/api/content/{path}'
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()
        title = data.get('title', path)
        updated_at = data.get('updated_at') or data.get('public_updated_at', '')
        body_html = ''
        details = data.get('details', {})
        if isinstance(details, dict):
            body_html = details.get('body', '')
            parts = details.get('parts', [])
            if parts:
                body_html = ' '.join(p.get('body', '') for p in parts)
        if not body_html:
            body_html = data.get('description', '')
        if body_html:
            soup = BeautifulSoup(body_html, 'html.parser')
            text = soup.get_text(separator=' ', strip=True)
        else:
            text = f'{title}. {data.get("description", "")}'
        text = re.sub(r'\s+', ' ', text).strip()
        return text, title, updated_at
    except Exception as e:
        print(f'  GOV.UK API error for {path}: {e}')
        return None, None, None

def fetch_web_scrape(url):
    import random
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0',
    ]
    headers = {
        'User-Agent': random.choice(user_agents),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-GB,en;q=0.9',
    }
    site_selectors = {
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
    }
    try:
        time.sleep(random.uniform(0.3, 1.0))
        response = requests.get(url, headers=headers, timeout=20, allow_redirects=True)
        if response.status_code == 403:
            headers['User-Agent'] = user_agents[0]
            response = requests.get(url, headers=headers, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        for tag in soup(['nav', 'header', 'footer', 'script', 'style', 'aside',
                         '.cookie-banner', '.cookie-notice', '.breadcrumb', '.related-content']):
            if hasattr(tag, 'decompose'):
                tag.decompose()
        main = None
        domain = re.sub(r'^www\.', '', re.sub(r'https?://', '', url).split('/')[0])
        selectors = site_selectors.get(domain, []) + ['main', 'article', '#content', 'body']
        for selector in selectors:
            found = soup.select_one(selector) if selector.startswith(('.', '#')) else soup.find(selector)
            if found and len(found.get_text(strip=True)) > 100:
                main = found
                break
        text = main.get_text(separator=' ', strip=True) if main else soup.get_text(separator=' ', strip=True)
        text = re.sub(r'\s+', ' ', text).strip()
        if len(text) < 100 or 'enable javascript' in text.lower():
            return None, None
        return text, datetime.now().isoformat()
    except Exception as e:
        print(f'  Scrape error for {url}: {e}')
        return None, None

def chunk_text(text, source_id, lens, topic, title, updated_at):
    enc = tiktoken.get_encoding('cl100k_base')
    tokens = enc.encode(text)
    chunks = []
    start = 0
    chunk_index = 0
    while start < len(tokens):
        end = min(start + CHUNK_SIZE, len(tokens))
        chunk_text_str = enc.decode(tokens[start:end])
        chunk_id = hashlib.md5(f'{source_id}_{chunk_index}'.encode()).hexdigest()
        chunks.append({
            'id': chunk_id,
            'text': chunk_text_str,
            'metadata': {
                'source_id': source_id, 'lens': lens, 'topic': topic,
                'title': title, 'chunk_index': chunk_index,
                'last_updated': updated_at[:10] if updated_at else datetime.now().strftime('%Y-%m-%d'),
                'content': chunk_text_str[:400]
            }
        })
        start += CHUNK_SIZE - CHUNK_OVERLAP
        chunk_index += 1
    return chunks

def embed_texts(texts):
    embeddings = []
    for i in range(0, len(texts), 100):
        batch = texts[i:i+100]
        response = openai_client.embeddings.create(model=EMBEDDING_MODEL, input=batch)
        embeddings.extend([item.embedding for item in response.data])
        time.sleep(0.3)
    return embeddings

def upsert_chunks(chunks):
    if not chunks:
        return
    texts = [c['text'] for c in chunks]
    embeddings = embed_texts(texts)
    vectors = [{'id': c['id'], 'values': emb, 'metadata': c['metadata']}
               for c, emb in zip(chunks, embeddings)]
    for i in range(0, len(vectors), BATCH_SIZE):
        index.upsert(vectors=vectors[i:i+BATCH_SIZE])
        time.sleep(0.3)

def delete_source_from_pinecone(source_id):
    try:
        index.delete(filter={'source_id': {'$eq': source_id}})
    except Exception as e:
        print(f'  Warning: could not delete old chunks: {e}')


# ── REAL-TIME RSS MONITORING ──────────────────────────────────────────

def monitor_rss_feeds():
    """
    Check all RSS feeds for new entries.
    Run every 15 minutes via GitHub Actions or cron.
    Pushes instant alerts to Supabase when relevant updates found.
    """
    print('═' * 60)
    print(f'Quantum Lens — Real-Time RSS Monitor')
    print(f'Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print('═' * 60)

    state = load_monitor_state()
    seen_entries = set(state.get('seen_entries', []))
    alerts_pushed = 0
    new_seen = []

    for feed_config in RSS_FEEDS:
        feed_url = feed_config['url']
        feed_name = feed_config['name']
        keywords = feed_config['keywords']

        print(f'\nChecking: {feed_name}')

        try:
            feed = feedparser.parse(feed_url)
            new_entries = []

            for entry in feed.entries[:20]:  # Check last 20 entries
                entry_id = entry.get('id', entry.get('link', ''))
                if entry_id in seen_entries:
                    continue

                # Check if any keywords match
                title = entry.get('title', '')
                summary = entry.get('summary', '')
                combined = f'{title} {summary}'.lower()

                matched_keywords = [kw for kw in keywords if kw in combined]
                if not matched_keywords:
                    new_seen.append(entry_id)
                    continue

                # This is a relevant update
                lens = detect_lens_from_text(combined)
                impact = score_impact(combined)
                source_url = entry.get('link', feed_url)
                published = entry.get('published', datetime.now().isoformat())

                alert_text = (
                    f"{title}. "
                    f"This update may affect your {lens} rights. "
                    f"Review how this change applies to your situation."
                )

                print(f'  NEW RELEVANT UPDATE: {title}')
                print(f'  Lens: {lens.upper()} | Impact: {impact}')

                # Push to Supabase immediately
                pushed = push_alert_to_supabase(
                    lens=lens,
                    topic=f'rss_{lens}_{datetime.now().strftime("%Y%m%d%H%M")}',
                    title=title,
                    alert_text=alert_text,
                    source_url=source_url,
                    impact=impact
                )

                if pushed:
                    alerts_pushed += 1

                # Re-index the source page in Pinecone
                if source_url and 'gov.uk' in source_url:
                    path = source_url.replace('https://www.gov.uk/', '')
                    text, page_title, updated_at = fetch_govuk_api(path)
                    if text and len(text) > 100:
                        source_id = f'govuk:{path}'
                        delete_source_from_pinecone(source_id)
                        chunks = chunk_text(text, source_id, lens, f'rss_{lens}', page_title or title, updated_at or '')
                        upsert_chunks(chunks)
                        print(f'  Re-indexed in Pinecone: {len(chunks)} chunks')

                new_seen.append(entry_id)
                new_entries.append(entry_id)

            if not new_entries:
                print(f'  No new relevant updates')

        except Exception as e:
            print(f'  Error checking {feed_name}: {e}')

    # Update state
    all_seen = list(seen_entries) + new_seen
    # Keep only last 1000 seen entries to prevent file growing too large
    state['seen_entries'] = all_seen[-1000:]
    state['last_check'] = datetime.now().isoformat()
    save_monitor_state(state)

    print(f'\n{"═" * 60}')
    print(f'Monitor complete — {alerts_pushed} alerts pushed to Supabase')
    print(f'Next check: run again in 15 minutes')
    print(f'{"═" * 60}')


# ── FULL INGESTION ────────────────────────────────────────────────────

def full_ingestion(update_only=False):
    print('═' * 60)
    print('Quantum Lens — Full Regulatory Knowledge Base')
    print('═' * 60)
    print(f'Mode: {"Update only" if update_only else "Full ingestion"}')
    print(f'Sources: {len(SOURCES)}')
    print()

    timestamps = load_timestamps()
    changed_sources = []
    total_chunks = 0
    skipped = 0
    failed = 0

    for i, source in enumerate(SOURCES):
        if 'govuk_path' in source:
            source_id = f'govuk:{source["govuk_path"]}'
        else:
            source_id = source['url']

        lens = source['lens']
        topic = source['topic']

        if 'govuk_path' in source:
            text, title, updated_at = fetch_govuk_api(source['govuk_path'])
        else:
            title = source.get('title', source_id)
            text, updated_at = fetch_web_scrape(source['url'])

        print(f'[{i+1}/{len(SOURCES)}] {title or source_id}')

        if not text or len(text) < 50:
            print(f'  FAILED')
            failed += 1
            print()
            continue

        last_updated = timestamps.get(source_id)
        if update_only and last_updated and updated_at and last_updated == updated_at:
            print(f'  No change — skipping')
            skipped += 1
            print()
            continue

        if last_updated and updated_at and last_updated != updated_at:
            print(f'  CHANGED — re-indexing and pushing alert')
            changed_sources.append({
                'lens': lens, 'topic': topic,
                'title': title or topic, 'updated_at': updated_at,
                'url': source.get('url', f'https://www.gov.uk/{source.get("govuk_path","")}'),
                'text': text[:500]
            })
            delete_source_from_pinecone(source_id)
            # Push alert immediately
            push_alert_to_supabase(
                lens=lens, topic=topic,
                title=f'{title or topic} — Updated',
                alert_text=f'{title or topic} has been updated. Review how this change affects your situation.',
                source_url=source.get('url', f'https://www.gov.uk/{source.get("govuk_path","")}'),
                impact=score_impact(text)
            )
        else:
            print(f'  New source — indexing')

        chunks = chunk_text(text, source_id, lens, topic, title or topic, updated_at or '')
        print(f'  {len(text):,} chars → {len(chunks)} chunks')
        upsert_chunks(chunks)
        total_chunks += len(chunks)
        timestamps[source_id] = updated_at or datetime.now().isoformat()
        save_timestamps(timestamps)
        print(f'  Indexed ✓')
        print()
        time.sleep(0.5)

    print('═' * 60)
    print(f'Ingestion complete')
    print(f'Chunks indexed: {total_chunks}')
    print(f'Sources skipped: {skipped}')
    print(f'Sources failed: {failed}')
    if changed_sources:
        print(f'\nRegulatory changes detected: {len(changed_sources)}')
        for s in changed_sources:
            print(f'  [{s["lens"].upper()}] {s["title"]}')
    else:
        print('\nNo regulatory changes detected since last run.')
    print()
    print('Run --monitor every 15 minutes for real-time detection')
    print('Run --update-only weekly for full source refresh')


# ── MAIN ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--monitor',      action='store_true', help='RSS monitoring every 15 minutes')
    parser.add_argument('--hash-check',   action='store_true', help='Hash monitoring for non-RSS sites')
    parser.add_argument('--full-monitor', action='store_true', help='Run both RSS and hash monitoring')
    parser.add_argument('--update-only',  action='store_true', help='Weekly full source update')
    parser.add_argument('--full',         action='store_true', help='Full ingestion of all sources')
    args = parser.parse_args()

    if args.monitor:
        monitor_rss_feeds()
    elif args.hash_check:
        monitor_hash_pages()
    elif args.full_monitor:
        monitor_rss_feeds()
        monitor_hash_pages()
    elif args.update_only:
        full_ingestion(update_only=True)
    else:
        full_ingestion(update_only=False)

if __name__ == '__main__':
    main()

# ── ADDITIONAL RSS FEEDS ──────────────────────────────────────────────
ADDITIONAL_RSS_FEEDS = [
    # legislation.gov.uk — new Acts and Statutory Instruments
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
    # CQC
    {
        'url': 'https://www.cqc.org.uk/news/rss',
        'name': 'CQC — News and guidance',
        'keywords': ['provider', 'regulation', 'inspection', 'care', 'guidance', 'registration']
    },
    # Financial Ombudsman
    {
        'url': 'https://www.financial-ombudsman.org.uk/news/rss',
        'name': 'Financial Ombudsman — News',
        'keywords': ['consumer', 'complaint', 'financial', 'insurance', 'bank', 'credit']
    },
]

# ── PAGES TO MONITOR BY CONTENT HASH ─────────────────────────────────
# These sites have no RSS — we detect changes by comparing page content hashes
HASH_MONITORED_PAGES = [
    # ACAS — key guidance pages
    {'url': 'https://www.acas.org.uk/dismissal',
     'lens': 'employment', 'topic': 'acas_dismissal',
     'title': 'ACAS — Dismissal guidance'},
    {'url': 'https://www.acas.org.uk/disciplinary-procedure-and-the-code',
     'lens': 'employment', 'topic': 'acas_disciplinary',
     'title': 'ACAS Code of Practice — Disciplinary and Grievance'},
    {'url': 'https://www.acas.org.uk/redundancy',
     'lens': 'employment', 'topic': 'acas_redundancy',
     'title': 'ACAS — Redundancy guidance'},
    {'url': 'https://www.acas.org.uk/discrimination-and-the-law',
     'lens': 'employment', 'topic': 'acas_discrimination',
     'title': 'ACAS — Discrimination and the law'},
    {'url': 'https://www.acas.org.uk/reasonable-adjustments',
     'lens': 'employment', 'topic': 'acas_adjustments',
     'title': 'ACAS — Reasonable adjustments'},
    {'url': 'https://www.acas.org.uk/holiday-entitlement',
     'lens': 'employment', 'topic': 'acas_holiday',
     'title': 'ACAS — Holiday entitlement'},
    {'url': 'https://www.acas.org.uk/performance-management',
     'lens': 'employment', 'topic': 'acas_performance',
     'title': 'ACAS — Performance management'},
    {'url': 'https://www.acas.org.uk/flexible-working',
     'lens': 'employment', 'topic': 'acas_flexible',
     'title': 'ACAS — Flexible working'},
    {'url': 'https://www.acas.org.uk/whistleblowing',
     'lens': 'employment', 'topic': 'acas_whistleblowing',
     'title': 'ACAS — Whistleblowing'},
    {'url': 'https://www.acas.org.uk/sick-leave-and-fit-notes',
     'lens': 'employment', 'topic': 'acas_sick',
     'title': 'ACAS — Sick leave and fit notes'},

    # Shelter — key housing guidance pages
    {'url': 'https://england.shelter.org.uk/housing_advice/eviction',
     'lens': 'housing', 'topic': 'shelter_eviction',
     'title': 'Shelter — Eviction rights'},
    {'url': 'https://england.shelter.org.uk/housing_advice/private_renting',
     'lens': 'housing', 'topic': 'shelter_renting',
     'title': 'Shelter — Private renting advice'},
    {'url': 'https://england.shelter.org.uk/housing_advice/repairs_and_bad_conditions',
     'lens': 'housing', 'topic': 'shelter_repairs',
     'title': 'Shelter — Repairs and bad conditions'},
    {'url': 'https://england.shelter.org.uk/housing_advice/homelessness',
     'lens': 'housing', 'topic': 'shelter_homelessness',
     'title': 'Shelter — Homelessness advice'},

    # Citizens Advice — key guidance pages
    {'url': 'https://www.citizensadvice.org.uk/work/rights-at-work/',
     'lens': 'employment', 'topic': 'ca_employment',
     'title': 'Citizens Advice — Rights at work'},
    {'url': 'https://www.citizensadvice.org.uk/work/dismissal/',
     'lens': 'employment', 'topic': 'ca_dismissal',
     'title': 'Citizens Advice — Dismissal'},
    {'url': 'https://www.citizensadvice.org.uk/housing/renting-a-home/',
     'lens': 'housing', 'topic': 'ca_renting',
     'title': 'Citizens Advice — Renting a home'},
    {'url': 'https://www.citizensadvice.org.uk/housing/eviction/',
     'lens': 'housing', 'topic': 'ca_eviction',
     'title': 'Citizens Advice — Eviction'},
    {'url': 'https://www.citizensadvice.org.uk/benefits/',
     'lens': 'benefits', 'topic': 'ca_benefits',
     'title': 'Citizens Advice — Benefits'},
    {'url': 'https://www.citizensadvice.org.uk/benefits/universal-credit/',
     'lens': 'benefits', 'topic': 'ca_uc',
     'title': 'Citizens Advice — Universal Credit'},
    {'url': 'https://www.citizensadvice.org.uk/immigration/',
     'lens': 'immigration', 'topic': 'ca_immigration',
     'title': 'Citizens Advice — Immigration'},
    {'url': 'https://www.citizensadvice.org.uk/consumer/',
     'lens': 'consumer', 'topic': 'ca_consumer',
     'title': 'Citizens Advice — Consumer rights'},
    {'url': 'https://www.citizensadvice.org.uk/health/',
     'lens': 'health', 'topic': 'ca_health',
     'title': 'Citizens Advice — Health rights'},

    # NHS — patient rights
    {'url': 'https://www.nhs.uk/nhs-services/gps/',
     'lens': 'health', 'topic': 'nhs_gp',
     'title': 'NHS — GP services and your rights'},
    {'url': 'https://www.nhs.uk/using-the-nhs/about-the-nhs/nhs-constitution/',
     'lens': 'health', 'topic': 'nhs_constitution',
     'title': 'NHS Constitution — Patient rights'},
    {'url': 'https://www.nhs.uk/using-the-nhs/about-the-nhs/your-choices-in-the-nhs/',
     'lens': 'health', 'topic': 'nhs_choices',
     'title': 'NHS — Your choices'},

    # EHRC
    {'url': 'https://www.equalityhumanrights.com/equality/equality-act-2010/your-rights-under-equality-act-2010',
     'lens': 'employment', 'topic': 'ehrc_equality_act',
     'title': 'EHRC — Your rights under the Equality Act 2010'},

    # ICO — additional pages
    {'url': 'https://ico.org.uk/for-organisations/guide-to-data-protection/',
     'lens': 'business', 'topic': 'ico_gdpr',
     'title': 'ICO — Guide to data protection'},
    {'url': 'https://ico.org.uk/for-organisations/sme-web-hub/',
     'lens': 'business', 'topic': 'ico_sme',
     'title': 'ICO — SME guidance'},

    # Financial Ombudsman
    {'url': 'https://www.financial-ombudsman.org.uk/consumers/complaints-can-help',
     'lens': 'consumer', 'topic': 'fos_complaints',
     'title': 'Financial Ombudsman — Complaints we can help with'},

    # MoneyHelper
    {'url': 'https://www.moneyhelper.org.uk/en/money-troubles/dealing-with-debt',
     'lens': 'finance', 'topic': 'moneyhelper_debt',
     'title': 'MoneyHelper — Dealing with debt'},
]

CONTENT_HASHES_FILE = 'content_hashes.json'

def load_content_hashes():
    if os.path.exists(CONTENT_HASHES_FILE):
        with open(CONTENT_HASHES_FILE) as f:
            return json.load(f)
    return {}

def save_content_hashes(hashes):
    with open(CONTENT_HASHES_FILE, 'w') as f:
        json.dump(hashes, f, indent=2)

def get_content_hash(text):
    """Generate a hash of page content to detect changes."""
    return hashlib.md5(text.encode()).hexdigest()

def monitor_hash_pages():
    """
    Monitor pages that have no RSS feed using content hash comparison.
    Detects any change to the page content — text additions, removals, updates.
    Run every few hours for near-real-time detection.
    """
    print('\n' + '─' * 60)
    print('Content Hash Monitor — Non-RSS Sites')
    print('Checking ACAS, Shelter, Citizens Advice, NHS, EHRC, ICO...')
    print('─' * 60)

    hashes = load_content_hashes()
    changes_detected = 0

    for page in HASH_MONITORED_PAGES:
        url = page['url']
        lens = page['lens']
        topic = page['topic']
        title = page['title']

        text, _ = fetch_web_scrape(url)
        if not text or len(text) < 100:
            continue

        current_hash = get_content_hash(text)
        previous_hash = hashes.get(url)

        if previous_hash and current_hash != previous_hash:
            print(f'\n  CHANGED: {title}')
            print(f'  Lens: {lens.upper()}')

            # Re-index in Pinecone
            source_id = url
            delete_source_from_pinecone(source_id)
            chunks = chunk_text(text, source_id, lens, topic, title,
                                datetime.now().isoformat())
            upsert_chunks(chunks)
            print(f'  Re-indexed: {len(chunks)} chunks')

            # Push alert to Supabase
            push_alert_to_supabase(
                lens=lens,
                topic=topic,
                title=f'{title} — Updated',
                alert_text=(f'{title} has been updated. '
                           f'Review how this change affects your {lens} rights.'),
                source_url=url,
                impact='Medium'
            )
            changes_detected += 1

        elif not previous_hash:
            print(f'  First check — storing hash: {title}')

        hashes[url] = current_hash
        time.sleep(0.5)

    save_content_hashes(hashes)
    print(f'\n  Hash monitor complete — {changes_detected} changes detected')
    return changes_detected

