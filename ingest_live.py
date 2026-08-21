"""
Quantum Lens — Real-Time Regulatory Monitoring System
Version 3.0 — Sustainable alert generation with Claude-powered content

Modes:
  python ingest_live.py                 Full ingestion of all sources
  python ingest_live.py --update-only   Check all sources for changes
  python ingest_live.py --monitor       Real-time RSS monitoring (run every 15 min)
  python ingest_live.py --hash-check    Content hash monitoring (run every 4 hours)
  python ingest_live.py --full-monitor  Run both RSS and hash monitoring
  python ingest_live.py --heal-urls     Check and fix broken URLs using AI

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
ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY', '')

EMBEDDING_MODEL = 'text-embedding-3-small'
CHUNK_SIZE = 400
CHUNK_OVERLAP = 60
BATCH_SIZE = 50
TIMESTAMPS_FILE = 'regulatory_timestamps.json'
MONITOR_STATE_FILE = 'monitor_state.json'
CONTENT_HASHES_FILE = 'content_hashes.json'
BROKEN_URLS_FILE = 'broken_urls.json'


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


# ── ALL OFFICIAL SOURCES ──────────────────────────────────────────────
SOURCES = [

    # ── EMPLOYMENT ──────────────────────────────────────────────────
    {'govuk_path': 'national-minimum-wage-rates',               'lens': 'employment', 'topic': 'national_minimum_wage'},
    {'govuk_path': 'redundancy-your-rights',                    'lens': 'employment', 'topic': 'redundancy'},
    {'govuk_path': 'discrimination-your-rights',                'lens': 'employment', 'topic': 'discrimination'},
    {'govuk_path': 'maternity-pay-leave',                       'lens': 'employment', 'topic': 'maternity_rights'},
    {'govuk_path': 'paternity-pay-leave',                       'lens': 'employment', 'topic': 'paternity_rights'},
    {'govuk_path': 'shared-parental-leave-and-pay',             'lens': 'employment', 'topic': 'shared_parental_leave'},
    {'govuk_path': 'employment-status',                         'lens': 'employment', 'topic': 'employment_status'},
    {'govuk_path': 'dismissal',                                 'lens': 'employment', 'topic': 'dismissal'},
    {'govuk_path': 'raise-grievance-at-work',                   'lens': 'employment', 'topic': 'grievance'},
    {'govuk_path': 'workplace-bullying-and-harassment',         'lens': 'employment', 'topic': 'harassment'},
    {'govuk_path': 'reasonable-adjustments-for-disabled-workers','lens': 'employment', 'topic': 'reasonable_adjustments'},
    {'govuk_path': 'taking-sick-leave',                         'lens': 'employment', 'topic': 'sick_leave'},
    {'govuk_path': 'statutory-sick-pay',                        'lens': 'employment', 'topic': 'statutory_sick_pay'},
    {'govuk_path': 'employment-tribunals',                      'lens': 'employment', 'topic': 'employment_tribunals'},
    {'url': 'https://www.gov.uk/holiday-entitlement-rights',    'lens': 'employment', 'topic': 'holiday_entitlement',  'title': 'Holiday entitlement rights'},
    {'url': 'https://www.gov.uk/notice-period',                 'lens': 'employment', 'topic': 'notice_period',        'title': 'Notice period'},
    {'url': 'https://www.gov.uk/maximum-weekly-working-hours',  'lens': 'employment', 'topic': 'working_time',         'title': 'Maximum weekly working hours'},
    {'url': 'https://www.gov.uk/flexible-working',              'lens': 'employment', 'topic': 'flexible_working',     'title': 'Flexible working'},
    {'url': 'https://www.gov.uk/zero-hours-contracts',          'lens': 'employment', 'topic': 'zero_hours',           'title': 'Zero hours contracts'},
    {'url': 'https://www.gov.uk/transfers-of-a-business',       'lens': 'employment', 'topic': 'tupe',                 'title': 'Transfers of a business TUPE'},
    {'url': 'https://www.gov.uk/definition-of-disability-under-equality-act-2010', 'lens': 'employment', 'topic': 'disability_definition', 'title': 'Definition of disability under Equality Act 2010'},
    # ACAS — corrected URLs
    {'url': 'https://www.acas.org.uk/dismissing-staff',         'lens': 'employment', 'topic': 'acas_dismissal',       'title': 'ACAS — Dismissing staff'},
    {'url': 'https://www.acas.org.uk/disciplinary-procedure-and-the-code', 'lens': 'employment', 'topic': 'acas_disciplinary', 'title': 'ACAS Code of Practice'},
    {'url': 'https://www.acas.org.uk/performance-management',  'lens': 'employment', 'topic': 'acas_performance',     'title': 'ACAS — Performance management'},
    {'url': 'https://www.acas.org.uk/redundancy',               'lens': 'employment', 'topic': 'acas_redundancy',      'title': 'ACAS — Redundancy'},
    {'url': 'https://www.acas.org.uk/discrimination-and-the-law','lens': 'employment', 'topic': 'acas_discrimination', 'title': 'ACAS — Discrimination'},
    {'url': 'https://www.acas.org.uk/maternity-paternity-and-adoption', 'lens': 'employment', 'topic': 'acas_maternity', 'title': 'ACAS — Maternity paternity and adoption'},
    {'url': 'https://www.acas.org.uk/reasonable-adjustments',  'lens': 'employment', 'topic': 'acas_adjustments',     'title': 'ACAS — Reasonable adjustments'},
    {'url': 'https://www.acas.org.uk/flexible-working',         'lens': 'employment', 'topic': 'acas_flexible',        'title': 'ACAS — Flexible working'},
    {'url': 'https://www.acas.org.uk/holiday-entitlement',      'lens': 'employment', 'topic': 'acas_holiday',         'title': 'ACAS — Holiday entitlement'},
    {'url': 'https://www.acas.org.uk/absence-from-work',        'lens': 'employment', 'topic': 'acas_sick',            'title': 'ACAS — Absence from work'},
    {'url': 'https://www.acas.org.uk/whistleblowing-at-work',   'lens': 'employment', 'topic': 'acas_whistleblowing',  'title': 'ACAS — Whistleblowing at work'},
    {'url': 'https://www.acas.org.uk/acas-code-of-practice-1-disciplinary-and-grievance-procedures', 'lens': 'employment', 'topic': 'acas_code_full', 'title': 'ACAS Full Code of Practice'},
    {'url': 'https://www.legislation.gov.uk/ukpga/2025/15/contents', 'lens': 'employment', 'topic': 'employment_rights_act_2025', 'title': 'Employment Rights Act 2025'},
    {'url': 'https://www.hse.gov.uk/workers/',                  'lens': 'employment', 'topic': 'hse_workers',          'title': 'HSE — Worker rights'},
    {'url': 'https://www.legislation.gov.uk/ukpga/1996/18/contents', 'lens': 'employment', 'topic': 'employment_rights_act_1996', 'title': 'Employment Rights Act 1996'},
    {'url': 'https://www.legislation.gov.uk/ukpga/2010/15/contents', 'lens': 'employment', 'topic': 'equality_act_2010', 'title': 'Equality Act 2010'},
    {'url': 'https://www.citizensadvice.org.uk/work/',          'lens': 'employment', 'topic': 'ca_employment',        'title': 'Citizens Advice — Work and employment'},
    {'url': 'https://www.citizensadvice.org.uk/work/dismissal/','lens': 'employment', 'topic': 'ca_dismissal',         'title': 'Citizens Advice — Dismissal'},

    # ── IMMIGRATION ─────────────────────────────────────────────────
    {'govuk_path': 'skilled-worker-visa',                       'lens': 'immigration', 'topic': 'skilled_worker_visa'},
    {'govuk_path': 'skilled-worker-visa/your-job',              'lens': 'immigration', 'topic': 'skilled_worker_job'},
    {'govuk_path': 'skilled-worker-visa/salary-requirements',   'lens': 'immigration', 'topic': 'skilled_worker_salary'},
    {'govuk_path': 'biometric-residence-permits',               'lens': 'immigration', 'topic': 'brp'},
    {'govuk_path': 'legal-right-work-uk',                       'lens': 'immigration', 'topic': 'right_to_work_docs'},
    {'govuk_path': 'innovator-founder-visa',                    'lens': 'immigration', 'topic': 'innovator_founder_visa'},
    {'govuk_path': 'indefinite-leave-to-remain',                'lens': 'immigration', 'topic': 'ilr_main'},
    {'govuk_path': 'student-visa',                              'lens': 'immigration', 'topic': 'student_visa'},
    {'govuk_path': 'graduate-visa',                             'lens': 'immigration', 'topic': 'graduate_visa'},
    {'govuk_path': 'settled-status-eu-citizens-families',       'lens': 'immigration', 'topic': 'eu_settled_status'},
    {'url': 'https://www.gov.uk/family-visa',                   'lens': 'immigration', 'topic': 'family_visa',          'title': 'Family visa'},
    {'url': 'https://www.gov.uk/indefinite-leave-to-remain-skilled-worker', 'lens': 'immigration', 'topic': 'ilr_skilled_worker', 'title': 'ILR — Skilled Worker'},
    {'url': 'https://www.gov.uk/sponsor-skilled-workers',       'lens': 'immigration', 'topic': 'sponsor_licence',      'title': 'Sponsor skilled workers'},
    {'url': 'https://www.gov.uk/check-job-applicant-right-to-work', 'lens': 'immigration', 'topic': 'right_to_work', 'title': 'Check a job applicant right to work'},
    {'url': 'https://www.gov.uk/government/publications/right-to-work-checks-employers-guide', 'lens': 'immigration', 'topic': 'right_to_work_guide', 'title': 'Right to work checks — Employer guide'},
    {'url': 'https://www.gov.uk/guidance/immigration-rules/immigration-rules-index', 'lens': 'immigration', 'topic': 'immigration_rules', 'title': 'Home Office Immigration Rules'},
    {'url': 'https://www.gov.uk/government/collections/immigration-rules-statement-of-changes', 'lens': 'immigration', 'topic': 'immigration_rules_all_changes', 'title': 'Immigration Rules — All Statements of Changes'},
    {'url': 'https://www.gov.uk/government/publications/statement-of-changes-to-the-immigration-rules-hc-259-9-july-2026/statement-of-changes-to-the-immigration-rules-hc-259-9-july-2026-accessible', 'lens': 'immigration', 'topic': 'hc_259_aug_2026', 'title': 'Immigration Rules Statement of Changes HC 259 — August 2026'},
    {'url': 'https://www.legislation.gov.uk/ukpga/2014/22/contents', 'lens': 'immigration', 'topic': 'immigration_act_2014', 'title': 'Immigration Act 2014'},
    {'url': 'https://www.citizensadvice.org.uk/immigration/',   'lens': 'immigration', 'topic': 'ca_immigration',       'title': 'Citizens Advice — Immigration'},

    # ── HOUSING ─────────────────────────────────────────────────────
    {'govuk_path': 'private-renting',                           'lens': 'housing', 'topic': 'private_renting'},
    {'govuk_path': 'homelessness-help-from-council',            'lens': 'housing', 'topic': 'homelessness'},
    {'govuk_path': 'renting-out-a-property',                    'lens': 'housing', 'topic': 'landlord_obligations'},
    {'govuk_path': 'housing-benefit',                           'lens': 'housing', 'topic': 'housing_benefit'},
    {'url': 'https://www.gov.uk/private-renting/evictions',     'lens': 'housing', 'topic': 'section_21',              'title': 'Private renting — Evictions'},
    {'url': 'https://www.gov.uk/tenancy-agreements',            'lens': 'housing', 'topic': 'tenancy_agreements',      'title': 'Tenancy agreements'},
    {'url': 'https://www.gov.uk/tenancy-deposit-protection',    'lens': 'housing', 'topic': 'deposit_protection',      'title': 'Tenancy deposit protection'},
    {'url': 'https://england.shelter.org.uk/housing_advice/eviction', 'lens': 'housing', 'topic': 'shelter_eviction', 'title': 'Shelter — Eviction rights'},
    {'url': 'https://england.shelter.org.uk/housing_advice/private_renting', 'lens': 'housing', 'topic': 'shelter_renting', 'title': 'Shelter — Private renting'},
    {'url': 'https://england.shelter.org.uk/housing_advice/homelessness', 'lens': 'housing', 'topic': 'shelter_homelessness', 'title': 'Shelter — Homelessness'},
    {'url': 'https://www.citizensadvice.org.uk/housing/renting-a-home/', 'lens': 'housing', 'topic': 'ca_renting',    'title': 'Citizens Advice — Renting'},
    {'url': 'https://www.citizensadvice.org.uk/housing/eviction/', 'lens': 'housing', 'topic': 'ca_eviction',         'title': 'Citizens Advice — Eviction'},
    {'url': 'https://www.legislation.gov.uk/ukpga/1988/50/contents', 'lens': 'housing', 'topic': 'housing_act_1988', 'title': 'Housing Act 1988'},
    {'url': 'https://www.gov.uk/government/collections/renters-reform-bill', 'lens': 'housing', 'topic': 'renters_reform_all', 'title': 'Renters Reform — All updates'},

    # ── BENEFITS ────────────────────────────────────────────────────
    {'govuk_path': 'universal-credit',                          'lens': 'benefits', 'topic': 'universal_credit'},
    {'govuk_path': 'universal-credit/what-youll-get',           'lens': 'benefits', 'topic': 'uc_amounts'},
    {'govuk_path': 'pip',                                       'lens': 'benefits', 'topic': 'pip'},
    {'govuk_path': 'child-benefit',                             'lens': 'benefits', 'topic': 'child_benefit'},
    {'govuk_path': 'mandatory-reconsideration',                 'lens': 'benefits', 'topic': 'mandatory_reconsideration'},
    {'govuk_path': 'employment-support-allowance',              'lens': 'benefits', 'topic': 'esa'},
    {'govuk_path': 'carers-allowance',                          'lens': 'benefits', 'topic': 'carers_allowance'},
    {'govuk_path': 'attendance-allowance',                      'lens': 'benefits', 'topic': 'attendance_allowance'},
    {'govuk_path': 'appeal-benefit-decision',                   'lens': 'benefits', 'topic': 'benefit_appeal'},
    {'url': 'https://www.gov.uk/working-tax-credit',            'lens': 'benefits', 'topic': 'tax_credits',            'title': 'Working Tax Credit'},
    {'url': 'https://www.gov.uk/pip/how-to-claim',              'lens': 'benefits', 'topic': 'pip_how_to_claim',       'title': 'PIP — How to claim'},
    {'url': 'https://www.citizensadvice.org.uk/benefits/',      'lens': 'benefits', 'topic': 'ca_benefits',            'title': 'Citizens Advice — Benefits'},
    {'url': 'https://www.citizensadvice.org.uk/benefits/universal-credit/', 'lens': 'benefits', 'topic': 'ca_uc',      'title': 'Citizens Advice — Universal Credit'},
    {'url': 'https://www.citizensadvice.org.uk/benefits/benefits-introduction/what-benefits-can-i-get/', 'lens': 'benefits', 'topic': 'ca_what_benefits', 'title': 'Citizens Advice — What benefits can I get'},

    # ── FINANCE ─────────────────────────────────────────────────────
    {'govuk_path': 'self-assessment-tax-returns',               'lens': 'finance', 'topic': 'self_assessment'},
    {'govuk_path': 'national-insurance',                        'lens': 'finance', 'topic': 'national_insurance'},
    {'govuk_path': 'income-tax',                                'lens': 'finance', 'topic': 'income_tax'},
    {'govuk_path': 'corporation-tax',                           'lens': 'finance', 'topic': 'corporation_tax'},
    {'url': 'https://www.gov.uk/self-employed-national-insurance-rates', 'lens': 'finance', 'topic': 'self_employed_ni', 'title': 'Self-employed National Insurance rates'},
    {'url': 'https://www.gov.uk/vat-registration',              'lens': 'finance', 'topic': 'vat',                    'title': 'VAT registration'},
    {'url': 'https://www.gov.uk/guidance/making-tax-digital',   'lens': 'finance', 'topic': 'making_tax_digital',     'title': 'Making Tax Digital'},
    {'url': 'https://www.gov.uk/set-up-self-employed',          'lens': 'finance', 'topic': 'self_employed_setup',    'title': 'Set up as self-employed'},
    {'url': 'https://www.gov.uk/guidance/rates-and-thresholds-for-employers-2025-to-2026', 'lens': 'finance', 'topic': 'hmrc_rates_2025_26', 'title': 'HMRC — Employer rates 2025-2026'},
    {'url': 'https://www.gov.uk/guidance/rates-and-thresholds-for-employers-2026-to-2027', 'lens': 'finance', 'topic': 'hmrc_rates_2026_27', 'title': 'HMRC — Employer rates 2026-2027'},
    {'url': 'https://www.financial-ombudsman.org.uk/consumers/complaints-can-help', 'lens': 'finance', 'topic': 'financial_ombudsman', 'title': 'Financial Ombudsman'},
    {'url': 'https://www.fca.org.uk/consumers',                 'lens': 'finance', 'topic': 'fca_consumer',           'title': 'FCA — Consumer guidance'},
    {'url': 'https://www.fca.org.uk/consumers/complaints-and-compensation', 'lens': 'finance', 'topic': 'fca_complaints', 'title': 'FCA — Complaints and compensation'},

    # ── BUSINESS COMPLIANCE ─────────────────────────────────────────
    {'govuk_path': 'set-up-limited-company',                    'lens': 'business', 'topic': 'company_formation'},
    {'govuk_path': 'running-a-limited-company',                 'lens': 'business', 'topic': 'running_company'},
    {'govuk_path': 'employers-liability-insurance',             'lens': 'business', 'topic': 'employers_liability'},
    {'govuk_path': 'register-for-vat',                          'lens': 'business', 'topic': 'vat_registration'},
    {'govuk_path': 'file-your-company-annual-accounts',         'lens': 'business', 'topic': 'companies_house_filing'},
    {'url': 'https://ico.org.uk/for-organisations/guide-to-data-protection/', 'lens': 'business', 'topic': 'ico_gdpr', 'title': 'ICO — Guide to data protection'},
    {'url': 'https://ico.org.uk/for-organisations/guide-to-data-protection/guide-to-the-general-data-protection-regulation-gdpr/', 'lens': 'business', 'topic': 'ico_uk_gdpr', 'title': 'ICO — UK GDPR guide'},
    {'url': 'https://ico.org.uk/for-organisations/sme-web-hub/','lens': 'business', 'topic': 'ico_sme',               'title': 'ICO — SME guidance'},
    {'url': 'https://ico.org.uk/for-organisations/uk-gdpr-guidance-and-resources/', 'lens': 'business', 'topic': 'ico_gdpr_resources_2026', 'title': 'ICO — UK GDPR guidance and resources 2026'},
    {'url': 'https://www.cqc.org.uk/guidance-providers/regulations', 'lens': 'business', 'topic': 'cqc_regulations', 'title': 'CQC — Regulations for providers'},
    {'url': 'https://www.gov.uk/government/organisations/companies-house', 'lens': 'business', 'topic': 'companies_house', 'title': 'Companies House'},
    {'url': 'https://www.thepensionsregulator.gov.uk/en/employers', 'lens': 'business', 'topic': 'pensions_regulator', 'title': 'Pensions Regulator — Employers'},
    {'url': 'https://www.gov.uk/workplace-pensions-employers',  'lens': 'business', 'topic': 'auto_enrolment',        'title': 'Auto-enrolment — Workplace pension obligations'},
    {'url': 'https://www.hse.gov.uk/simple-health-safety/',     'lens': 'business', 'topic': 'hse_employer_obligations', 'title': 'HSE — Health and safety basics for employers'},

    # ── CONSUMER AND DIGITAL RIGHTS ─────────────────────────────────
    {'govuk_path': 'consumer-rights-act-2015',                  'lens': 'consumer', 'topic': 'consumer_rights_act'},
    {'govuk_path': 'make-court-claim-for-money',                'lens': 'consumer', 'topic': 'small_claims'},
    {'url': 'https://www.citizensadvice.org.uk/consumer/',      'lens': 'consumer', 'topic': 'ca_consumer',            'title': 'Citizens Advice — Consumer rights'},
    {'url': 'https://ico.org.uk/your-data-matters/',            'lens': 'consumer', 'topic': 'ico_your_data_matters',  'title': 'ICO — Your data matters (consumer rights)'},

    # ── HEALTH NAVIGATION ───────────────────────────────────────────
    {'url': 'https://www.gov.uk/mental-health-helplines',       'lens': 'health', 'topic': 'mental_health_rights',    'title': 'Mental health helplines and support'},
    {'url': 'https://www.nhs.uk/nhs-services/',                 'lens': 'health', 'topic': 'nhs_services',            'title': 'NHS services'},
    {'url': 'https://www.nhs.uk/nhs-services/gps/',             'lens': 'health', 'topic': 'nhs_gp',                  'title': 'NHS — GP services'},
    {'url': 'https://www.nhs.uk/nhs-services/mental-health-services/', 'lens': 'health', 'topic': 'nhs_mental_health', 'title': 'NHS — Mental health services'},
    {'url': 'https://www.gov.uk/guidance/nhs-entitlements-migrant-health-guide', 'lens': 'health', 'topic': 'nhs_migrant_entitlements', 'title': 'NHS entitlements — Migrant health guide'},
    {'url': 'https://www.citizensadvice.org.uk/health/',        'lens': 'health', 'topic': 'ca_health',               'title': 'Citizens Advice — Health rights'},
    {'url': 'https://www.cqc.org.uk/guidance-providers',        'lens': 'health', 'topic': 'cqc_patient',             'title': 'CQC — Guidance for providers and patients'},
]


# ── PAGES MONITORED BY CONTENT HASH ──────────────────────────────────
HASH_MONITORED_PAGES = [
    {'url': 'https://www.acas.org.uk/dismissing-staff',          'lens': 'employment', 'topic': 'acas_dismissal',    'title': 'ACAS — Dismissing staff'},
    {'url': 'https://www.acas.org.uk/disciplinary-procedure-and-the-code', 'lens': 'employment', 'topic': 'acas_disciplinary', 'title': 'ACAS Code of Practice'},
    {'url': 'https://www.acas.org.uk/redundancy',                'lens': 'employment', 'topic': 'acas_redundancy',   'title': 'ACAS — Redundancy'},
    {'url': 'https://www.acas.org.uk/discrimination-and-the-law','lens': 'employment', 'topic': 'acas_discrimination','title': 'ACAS — Discrimination'},
    {'url': 'https://www.acas.org.uk/reasonable-adjustments',   'lens': 'employment', 'topic': 'acas_adjustments',  'title': 'ACAS — Reasonable adjustments'},
    {'url': 'https://www.acas.org.uk/holiday-entitlement',      'lens': 'employment', 'topic': 'acas_holiday',      'title': 'ACAS — Holiday entitlement'},
    {'url': 'https://www.acas.org.uk/performance-management',   'lens': 'employment', 'topic': 'acas_performance',  'title': 'ACAS — Performance management'},
    {'url': 'https://www.acas.org.uk/flexible-working',         'lens': 'employment', 'topic': 'acas_flexible',     'title': 'ACAS — Flexible working'},
    {'url': 'https://www.acas.org.uk/whistleblowing-at-work',   'lens': 'employment', 'topic': 'acas_whistleblowing','title': 'ACAS — Whistleblowing at work'},
    {'url': 'https://www.acas.org.uk/absence-from-work',        'lens': 'employment', 'topic': 'acas_sick',         'title': 'ACAS — Absence from work'},
    {'url': 'https://england.shelter.org.uk/housing_advice/eviction', 'lens': 'housing', 'topic': 'shelter_eviction', 'title': 'Shelter — Eviction rights'},
    {'url': 'https://england.shelter.org.uk/housing_advice/private_renting', 'lens': 'housing', 'topic': 'shelter_renting', 'title': 'Shelter — Private renting'},
    {'url': 'https://england.shelter.org.uk/housing_advice/homelessness', 'lens': 'housing', 'topic': 'shelter_homelessness', 'title': 'Shelter — Homelessness'},
    {'url': 'https://www.citizensadvice.org.uk/work/',          'lens': 'employment', 'topic': 'ca_employment',     'title': 'Citizens Advice — Work'},
    {'url': 'https://www.citizensadvice.org.uk/housing/renting-a-home/', 'lens': 'housing', 'topic': 'ca_renting',  'title': 'Citizens Advice — Renting'},
    {'url': 'https://www.citizensadvice.org.uk/benefits/',      'lens': 'benefits',   'topic': 'ca_benefits',       'title': 'Citizens Advice — Benefits'},
    {'url': 'https://www.citizensadvice.org.uk/immigration/',   'lens': 'immigration','topic': 'ca_immigration',    'title': 'Citizens Advice — Immigration'},
    {'url': 'https://www.citizensadvice.org.uk/consumer/',      'lens': 'consumer',   'topic': 'ca_consumer',       'title': 'Citizens Advice — Consumer rights'},
    {'url': 'https://www.citizensadvice.org.uk/health/',        'lens': 'health',     'topic': 'ca_health',         'title': 'Citizens Advice — Health rights'},
    {'url': 'https://www.nhs.uk/nhs-services/gps/',            'lens': 'health',     'topic': 'nhs_gp',            'title': 'NHS — GP services'},
    {'url': 'https://ico.org.uk/for-organisations/guide-to-data-protection/', 'lens': 'business', 'topic': 'ico_gdpr', 'title': 'ICO — Data protection guide'},
    {'url': 'https://ico.org.uk/for-organisations/sme-web-hub/', 'lens': 'business', 'topic': 'ico_sme',           'title': 'ICO — SME guidance'},
    {'url': 'https://www.financial-ombudsman.org.uk/consumers/complaints-can-help', 'lens': 'consumer', 'topic': 'fos_complaints', 'title': 'Financial Ombudsman'},
    {'url': 'https://www.thepensionsregulator.gov.uk/en/employers', 'lens': 'business', 'topic': 'pensions_regulator', 'title': 'Pensions Regulator — Employers'},
    {'url': 'https://www.gov.uk/guidance/rates-and-thresholds-for-employers-2026-to-2027', 'lens': 'finance', 'topic': 'hmrc_rates_2026_27', 'title': 'HMRC — Employer rates 2026-2027'},
    {'url': 'https://www.gov.uk/guidance/immigration-rules/immigration-rules-index', 'lens': 'immigration', 'topic': 'immigration_rules', 'title': 'Immigration Rules Index'},
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


def generate_alert_text(title, lens, changed_text=''):
    """
    Use Claude to generate proper actionable alert text with imperative sentences.
    Falls back to a basic template if Claude is unavailable.
    """
    if not ANTHROPIC_API_KEY:
        return (title + ' has been updated. '
                'Check how this change affects your ' + lens + ' situation. '
                'Review the official source for full details.')
    try:
        prompt = (
            'Write a 3-sentence UK regulatory alert for advisers and affected individuals. '
            'Sentence 1: What changed and when (be specific, cite the law or body if known). '
            'Sentence 2: Who is affected and why it matters. '
            'Sentence 3: ONE specific action starting with an imperative verb such as Check, Review, Update, Verify, Ensure, Apply, Register, Confirm, Report, Audit, Diarise, Seek, Calculate, Claim. '
            'Topic: ' + title + '. Lens/area: ' + lens + '. '
            + ('Context: ' + changed_text[:500] if changed_text else '') +
            ' Be factual, specific, and concise. No generic phrases.'
        )
        response = requests.post(
            'https://api.anthropic.com/v1/messages',
            json={
                'model': 'claude-sonnet-4-6',
                'max_tokens': 200,
                'messages': [{'role': 'user', 'content': prompt}]
            },
            headers={
                'x-api-key': ANTHROPIC_API_KEY,
                'anthropic-version': '2023-06-01',
                'Content-Type': 'application/json'
            },
            timeout=20
        )
        if response.status_code == 200:
            return response.json()['content'][0]['text'].strip()
    except Exception as e:
        print('  Claude alert generation failed: ' + str(e))

    return (title + ' has been updated. '
            'Check how this change affects your ' + lens + ' situation. '
            'Review the official source and update your records accordingly.')


def push_alert_to_supabase(lens, topic, title, alert_text, source_url='', impact='Medium'):
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        print('  Supabase not configured — alert not pushed')
        return False
    try:
        r = requests.post(
            SUPABASE_URL + '/rest/v1/regulatory_changes',
            json={
                'lens': lens, 'topic': topic, 'title': title,
                'alert_text': alert_text, 'source_url': source_url,
                'impact': impact, 'is_active': True,
                'created_at': datetime.now(timezone.utc).isoformat()
            },
            headers={
                'apikey': SUPABASE_SERVICE_KEY,
                'Authorization': 'Bearer ' + SUPABASE_SERVICE_KEY,
                'Content-Type': 'application/json',
                'Prefer': 'resolution=merge-duplicates,return=minimal'
            },
            timeout=10
        )
        if r.status_code in [200, 201]:
            print('  Alert pushed: [' + lens.upper() + '] ' + title)
            return True
        print('  Failed to push alert: ' + str(r.status_code) + ' ' + r.text[:100])
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
                'source_id': source_id, 'lens': lens, 'topic': topic,
                'title': title, 'chunk_index': idx,
                'last_updated': updated_at[:10] if updated_at else datetime.now().strftime('%Y-%m-%d'),
                'content': chunk_str[:400]
            }
        })
        start += CHUNK_SIZE - CHUNK_OVERLAP
        idx += 1
    if not chunks:
        return 0
    texts = [c['text'] for c in chunks]
    all_embeddings = []
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
                if not any(kw in combined for kw in keywords):
                    new_seen.append(entry_id)
                    continue
                lens = detect_lens_from_text(combined)
                impact = score_impact(combined)
                source_url = entry.get('link', feed_url)
                # Use Claude to generate proper actionable alert text
                alert_text = generate_alert_text(title, lens, summary[:500])
                print('  NEW: ' + title)
                print('  Lens: ' + lens.upper() + ' | Impact: ' + impact)
                topic = 'rss_' + lens + '_' + hashlib.md5(entry_id.encode()).hexdigest()[:8]
                if push_alert_to_supabase(lens=lens, topic=topic, title=title,
                                          alert_text=alert_text, source_url=source_url,
                                          impact=impact):
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
    print('=' * 60)


# ── HASH MONITORING ───────────────────────────────────────────────────

def monitor_hash_pages():
    print('\n' + '-' * 60)
    print('Hash Monitor — ACAS, Shelter, Citizens Advice, NHS, ICO')
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
            chunk_and_index(text, url, lens, topic, title, datetime.now().isoformat())
            # Generate proper actionable alert text using Claude
            alert_text = generate_alert_text(title, lens, text[:500])
            push_alert_to_supabase(
                lens=lens, topic=topic,
                title=title + ' — Updated',
                alert_text=alert_text,
                source_url=url, impact='Medium'
            )
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

def full_ingestion(update_only=False):
    print('=' * 60)
    print('Quantum Lens — Full Regulatory Knowledge Base')
    print('Mode: ' + ('Update only' if update_only else 'Full ingestion'))
    print('Sources: ' + str(len(SOURCES)))
    print('=' * 60)

    timestamps = load_json(TIMESTAMPS_FILE)
    total_chunks = 0
    skipped = 0
    failed = 0

    for i, source in enumerate(SOURCES):
        if 'govuk_path' in source:
            source_id = 'govuk:' + source['govuk_path']
            text, title, updated_at = fetch_govuk_api(source['govuk_path'])
        else:
            source_id = source['url']
            title = source.get('title', source_id)
            text, updated_at = fetch_web_scrape(source['url'])

        lens = source['lens']
        topic = source['topic']
        print('[' + str(i+1) + '/' + str(len(SOURCES)) + '] ' + str(title or source_id))

        if not text or len(text) < 50:
            print('  FAILED')
            failed += 1
            print()
            continue

        last_updated = timestamps.get(source_id)
        if update_only and last_updated:
            content_hash = get_content_hash(text)
            if timestamps.get(source_id + '_hash') == content_hash:
                print('  No change — skipping')
                skipped += 1
                print()
                continue

        if last_updated:
            delete_from_pinecone(source_id)
            # Generate proper alert text using Claude
            alert_text = generate_alert_text(str(title or topic), lens, text[:500])
            push_alert_to_supabase(
                lens=lens, topic=topic,
                title=str(title or topic) + ' — Updated',
                alert_text=alert_text,
                source_url=source.get('url', 'https://www.gov.uk/' + source.get('govuk_path', '')),
                impact=score_impact(text)
            )

        n_chunks = chunk_and_index(text, source_id, lens, topic, str(title or topic), updated_at or '')
        print('  ' + str(len(text)) + ' chars -> ' + str(n_chunks) + ' chunks — OK')
        timestamps[source_id] = updated_at or datetime.now().isoformat()
        timestamps[source_id + '_hash'] = get_content_hash(text)
        total_chunks += n_chunks
        save_json(TIMESTAMPS_FILE, timestamps)
        print()
        time.sleep(0.5)

    print('=' * 60)
    print('Ingestion complete')
    print('Chunks indexed: ' + str(total_chunks))
    print('Skipped:        ' + str(skipped))
    print('Failed:         ' + str(failed))
    print()
    print('Run --monitor every 15 min for real-time RSS detection')
    print('Run --hash-check every 4 hours for non-RSS site detection')
    print('Run --update-only weekly for full source refresh')


# ── URL SELF-HEALING ─────────────────────────────────────────────────

def heal_broken_urls():
    print('=' * 60)
    print('Layer 3 — AI URL Self-Healing')
    print('Checking all static sources for broken URLs')
    print('=' * 60)

    broken_urls = load_json(BROKEN_URLS_FILE)
    healed_count = 0
    broken_count = 0

    for source in SOURCES:
        url = source.get('url')
        if not url:
            continue
        title = source.get('title', url)
        try:
            r = requests.head(url, headers=get_headers(), timeout=10, allow_redirects=True)
            if r.status_code == 404:
                print('BROKEN (404): ' + title)
                broken_count += 1
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
                                        'The following UK government or advice organisation URL returns 404: ' + url + '\n'
                                        'Page title: ' + title + '\n'
                                        'Suggest the most likely current URL on the same website. '
                                        'Return ONLY the URL, nothing else. If unknown return "UNKNOWN".'
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
                            suggested = heal_response.json()['content'][0]['text'].strip()
                            if suggested != 'UNKNOWN' and suggested.startswith('http'):
                                verify = requests.head(suggested, headers=get_headers(), timeout=10, allow_redirects=True)
                                if verify.status_code == 200:
                                    print('  HEALED: ' + suggested)
                                    broken_urls[url] = {'original': url, 'replacement': suggested, 'title': title}
                                    healed_count += 1
                                else:
                                    broken_urls[url] = {'original': url, 'replacement': None, 'title': title}
                    except Exception as e:
                        print('  Claude error: ' + str(e))
            elif r.status_code == 200 and url in broken_urls:
                del broken_urls[url]
                print('RECOVERED: ' + title)
        except Exception as e:
            print('Error checking ' + url + ': ' + str(e))
        time.sleep(0.3)

    save_json(BROKEN_URLS_FILE, broken_urls)
    print('\n' + '=' * 60)
    print('URL healing complete — Broken: ' + str(broken_count) + ', Healed: ' + str(healed_count))
    print('=' * 60)


# ── MAIN ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Quantum Lens Regulatory Monitor v3.0')
    parser.add_argument('--monitor',      action='store_true', help='RSS monitoring — run every 15 minutes')
    parser.add_argument('--hash-check',   action='store_true', help='Hash monitoring — run every 4 hours')
    parser.add_argument('--full-monitor', action='store_true', help='Run both RSS and hash monitoring')
    parser.add_argument('--update-only',  action='store_true', help='Check all sources for changes')
    parser.add_argument('--heal-urls',    action='store_true', help='Check and fix broken URLs using AI')
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
        full_ingestion(update_only=True)
    else:
        full_ingestion(update_only=False)


if __name__ == '__main__':
    main()
