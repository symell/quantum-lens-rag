import os, json, time, hashlib, requests, re
from bs4 import BeautifulSoup
from dotenv import load_dotenv
load_dotenv()

CONTENT_HASHES_FILE = 'content_hashes.json'
SUPABASE_URL = os.getenv('SUPABASE_URL', '')
SUPABASE_SERVICE_KEY = os.getenv('SUPABASE_SERVICE_KEY', '')

PAGES = [
    {'url': 'https://www.acas.org.uk/dismissal', 'lens': 'employment', 'title': 'ACAS Dismissal'},
    {'url': 'https://www.acas.org.uk/redundancy', 'lens': 'employment', 'title': 'ACAS Redundancy'},
    {'url': 'https://www.acas.org.uk/disciplinary-procedure-and-the-code', 'lens': 'employment', 'title': 'ACAS Code of Practice'},
    {'url': 'https://england.shelter.org.uk/housing_advice/eviction', 'lens': 'housing', 'title': 'Shelter Eviction'},
    {'url': 'https://england.shelter.org.uk/housing_advice/private_renting', 'lens': 'housing', 'title': 'Shelter Private Renting'},
    {'url': 'https://www.citizensadvice.org.uk/work/rights-at-work/', 'lens': 'employment', 'title': 'Citizens Advice Employment'},
    {'url': 'https://www.citizensadvice.org.uk/housing/renting-a-home/', 'lens': 'housing', 'title': 'Citizens Advice Housing'},
    {'url': 'https://www.citizensadvice.org.uk/benefits/', 'lens': 'benefits', 'title': 'Citizens Advice Benefits'},
    {'url': 'https://www.citizensadvice.org.uk/immigration/', 'lens': 'immigration', 'title': 'Citizens Advice Immigration'},
    {'url': 'https://www.nhs.uk/nhs-services/gps/', 'lens': 'health', 'title': 'NHS GP Services'},
    {'url': 'https://ico.org.uk/for-organisations/guide-to-data-protection/', 'lens': 'business', 'title': 'ICO Data Protection'},
    {'url': 'https://www.financial-ombudsman.org.uk/consumers/complaints-can-help', 'lens': 'consumer', 'title': 'Financial Ombudsman'},
]

def get_text(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/125.0.0.0'}
    try:
        time.sleep(0.5)
        r = requests.get(url, headers=headers, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.content, 'html.parser')
        for t in soup(['nav','header','footer','script','style']):
            t.decompose()
        main = soup.find('main') or soup.find('article') or soup.body
        text = main.get_text(separator=' ', strip=True) if main else ''
        return re.sub(r'\s+', ' ', text).strip()
    except Exception as e:
        print('  Error: ' + str(e))
        return None

def push(lens, title, url):
    if not SUPABASE_URL:
        return
    try:
        requests.post(SUPABASE_URL + '/rest/v1/regulatory_changes',
            json={'lens': lens, 'topic': 'hash_' + lens,
                  'title': title + ' Updated',
                  'alert_text': title + ' has been updated. Review your ' + lens + ' rights.',
                  'source_url': url, 'impact': 'Medium', 'is_active': True},
            headers={'apikey': SUPABASE_SERVICE_KEY,
                     'Authorization': 'Bearer ' + SUPABASE_SERVICE_KEY,
                     'Content-Type': 'application/json'},
            timeout=10)
        print('  Alert pushed to Supabase')
    except Exception as e:
        print('  Error: ' + str(e))

hashes = {}
if os.path.exists(CONTENT_HASHES_FILE):
    with open(CONTENT_HASHES_FILE) as f:
        hashes = json.load(f)

print('=' * 50)
print('Hash Monitor - ACAS Shelter Citizens Advice NHS ICO')
print('=' * 50)

changes = 0
for p in PAGES:
    url, lens, title = p['url'], p['lens'], p['title']
    print('Checking: ' + title)
    text = get_text(url)
    if not text or len(text) < 100:
        print('  Failed')
        continue
    h = hashlib.md5(text.encode()).hexdigest()
    prev = hashes.get(url)
    if prev and h != prev:
        print('  CHANGED - pushing alert')
        push(lens, title, url)
        changes += 1
    elif not prev:
        print('  First check - hash stored')
    else:
        print('  No change')
    hashes[url] = h

with open(CONTENT_HASHES_FILE, 'w') as f:
    json.dump(hashes, f, indent=2)

print('')
print('Complete - ' + str(changes) + ' changes detected')
print('content_hashes.json saved')