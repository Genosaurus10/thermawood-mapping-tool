"""
Process GHL Contacts CSV into Mapping Tool data files.

Input:  Thermawood Contacts All (all Fields).csv
Output: data/leads_by_postcode.json  (aggregated lead data per postcode)
        data/territories.json         (updated territory definitions with state mapping)

Run with:
  python process_ghl_contacts.py
"""

import csv
import json
import re
import os
from collections import defaultdict
from datetime import datetime

INPUT_CSV = os.path.join(os.path.dirname(__file__), 'Thermawood Contacts All (all Fields).csv')
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'data')

# Territory colors — bright, high-contrast for dark basemap
TERRITORY_COLORS = {
    # VIC
    "Yarra": "#d35400",
    "Gippsland": "#e74c3c",
    "Merri-bek": "#e67e22",
    "Boroondara": "#f1c40f",
    "Maribyrnong": "#00e5ff",
    "Monash": "#27ae60",
    "Port Phillip": "#ff6b9d",
    "Geelong": "#2980b9",
    "Toorak": "#2ecc71",
    "Eastern Suburbs": "#16a085",
    "Peninsula": "#e91e63",
    "Moonee Valley": "#00bcd4",
    "Inner North": "#e67e22",
    "Bayside": "#ff9800",
    "Hobson Bay": "#8bc34a",
    # NSW
    "Inner West": "#ff5722",
    "Inner West Sydney": "#9b59b6",
    "Northern Beaches": "#2ecc71",
    "City South": "#e74c3c",
    "New Castle": "#c0392b",
    "Lower North Shore": "#9b59b6",
    "Sutherland": "#3498db",
    "Southern Suburbs": "#1abc9c",
    "Southern Suburbs/ Sutherland": "#f39c12",
    "Southern Highlands": "#cddc39",
    "Illawarra": "#ff7043",
    # QLD
    "Brisbane North": "#ffab40",
    "Queensland - Master Franchise": "#f39c12",
    # SA
    "South Australia - Master Franchise": "#3498db",
    # TAS
    "Tasmania - Master Franchise": "#1abc9c",
    # ACT
    "ACT": "#8e44ad",
}

# State mapping for territories
TERRITORY_STATE = {
    "Yarra": "VIC", "Gippsland": "VIC", "Merri-bek": "VIC", "Boroondara": "VIC",
    "Maribyrnong": "VIC", "Monash": "VIC", "Port Phillip": "VIC", "Geelong": "VIC",
    "Toorak": "VIC", "Peninsula": "VIC", "Moonee Valley": "VIC", "Inner North": "VIC",
    "Bayside": "VIC", "Hobson Bay": "VIC",
    "Inner West": "NSW", "Inner West Sydney": "NSW", "Northern Beaches": "NSW",
    "Eastern Suburbs": "NSW", "City South": "NSW", "New Castle": "NSW",
    "Lower North Shore": "NSW", "Sutherland": "NSW", "Southern Suburbs": "NSW",
    "Southern Suburbs/ Sutherland": "NSW", "Southern Highlands": "NSW", "Illawarra": "NSW",
    "Brisbane North": "QLD", "Queensland - Master Franchise": "QLD",
    "South Australia - Master Franchise": "SA",
    "Tasmania - Master Franchise": "TAS",
    "ACT": "ACT",
}


def parse_opportunity(opp_str):
    """Parse opportunity string like 'open Boroondara NEW LEAD' into components."""
    if not opp_str:
        return None, None, None

    # Pattern: status territory STAGE
    # Stages: NEW LEAD, LEAD CONTACTED, QUOTE SENT, WON, LOST
    stages = ['NEW LEAD', 'LEAD CONTACTED', 'QUOTE SENT', 'WON', 'LOST',
              'QUOTE REQUESTED', 'JOB BOOKED', 'JOB COMPLETED']

    status = None
    stage = None
    territory = None

    opp = opp_str.strip()

    # Extract status (open/won/lost/abandoned)
    for s in ['open', 'won', 'lost', 'abandoned']:
        if opp.lower().startswith(s + ' '):
            status = s
            opp = opp[len(s)+1:]
            break

    # Extract stage (from the end)
    opp_upper = opp.upper()
    for st in sorted(stages, key=len, reverse=True):
        if opp_upper.endswith(st):
            stage = st.title()
            territory = opp[:len(opp)-len(st)].strip()
            break

    if not territory:
        territory = opp.strip()

    return status, territory, stage


def normalize_state(state_str):
    """Normalize state values."""
    if not state_str:
        return None
    s = state_str.strip().upper()
    mapping = {
        'VIC': 'VIC', 'VICTORIA': 'VIC',
        'NSW': 'NSW', 'NEW SOUTH WALES': 'NSW',
        'QLD': 'QLD', 'QUEENSLAND': 'QLD',
        'SA': 'SA', 'SOUTH AUSTRALIA': 'SA',
        'TAS': 'TAS', 'TASMANIA': 'TAS',
        'ACT': 'ACT',
        'WA': 'WA', 'WESTERN AUSTRALIA': 'WA',
        'NT': 'NT', 'NORTHERN TERRITORY': 'NT',
    }
    return mapping.get(s, None)


def get_franchise_territory(row):
    """Get franchise territory from the franchise assignment fields."""
    for field in ['Franchise Assigned - VIC', 'Franchise Assigned - NSW', 'Franchise Assigned - QLD']:
        val = row.get(field, '').strip()
        if val and val not in ('Undefined', 'Rework', '3. Supplier / Resource', 'Standard Job'):
            # Normalize common misspellings
            if val == 'Merribek':
                val = 'Merri-bek'
            elif val == 'Boorondara':
                val = 'Boroondara'
            elif val == 'Hobson Bay':
                val = 'Hobson Bay'
            return val
    return None


def parse_tags(tags_str):
    """Parse comma-separated tags into categorized dict."""
    if not tags_str:
        return {}

    result = {
        'source_tags': [],
        'lead_type_tags': [],
        'other_tags': [],
    }

    for tag in tags_str.split(','):
        tag = tag.strip().lower()
        if not tag:
            continue

        if tag.startswith('how did you hear'):
            result['source_tags'].append(tag.replace('how did you hear: ', ''))
        elif tag in ('quote requested', 'quote requested - online form', 'e-book download',
                      'quote follow up - longterm', 'transition quote'):
            result['lead_type_tags'].append(tag)
        elif tag in ('cold outreach', 'hubspot imported', 'servicem8 created'):
            result['source_tags'].append(tag)
        elif tag.startswith('window type:') or tag == 'none timber window':
            pass  # covered by window frames field
        else:
            result['other_tags'].append(tag)

    return result


def parse_date(date_str):
    """Parse ISO date string to date components."""
    if not date_str:
        return None
    try:
        # Handle "2026-03-13T09:13:08+10:00" format
        dt = datetime.fromisoformat(date_str)
        return {
            'year': dt.year,
            'month': dt.month,
            'quarter': f"Q{(dt.month - 1) // 3 + 1}",
            'date': dt.strftime('%Y-%m-%d'),
        }
    except:
        return None


def main():
    print("Reading CSV...")
    contacts = []
    with open(INPUT_CSV, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            contacts.append(row)

    print(f"  Total contacts: {len(contacts)}")

    # ── Build leads_by_postcode ──
    print("\nProcessing leads by postcode...")
    postcode_data = defaultdict(lambda: {
        'total': 0,
        'contacts': [],
        'state': None,
        'territory': None,
        'opportunities': {
            'new_lead': 0,
            'lead_contacted': 0,
            'quote_sent': 0,
            'quote_requested': 0,
            'won': 0,
            'lost': 0,
            'job_booked': 0,
            'job_completed': 0,
        },
        'lifecycle': {
            'lead': 0,
            'mql': 0,
            'sql': 0,
            'opportunity': 0,
            'customer': 0,
        },
        'sources': defaultdict(int),
        'heard_about': defaultdict(int),
        'window_types': defaultdict(int),
        'lead_types': defaultdict(int),
        'created_dates': [],
        'franchise_territory': None,
    })

    skipped_no_postcode = 0
    processed = 0
    territories_found = set()
    territory_postcodes = defaultdict(set)
    territory_states = {}

    for row in contacts:
        postcode = row.get('Postal Code', '').strip()
        if not postcode or not re.match(r'^\d{4}$', postcode):
            skipped_no_postcode += 1
            continue

        processed += 1
        pc = postcode_data[postcode]
        pc['total'] += 1

        # State
        state = normalize_state(row.get('State', '') or row.get('*TW - State*', ''))
        if state and not pc['state']:
            pc['state'] = state

        # Franchise territory
        franchise = get_franchise_territory(row)
        if franchise:
            territories_found.add(franchise)
            territory_postcodes[franchise].add(postcode)
            if not pc['franchise_territory']:
                pc['franchise_territory'] = franchise
            # Track territory → state mapping
            if state and franchise not in territory_states:
                territory_states[franchise] = state

        # Opportunity parsing
        opp_str = row.get('Opportunities', '').strip()
        if opp_str:
            # Can have multiple opportunities separated by comma
            for opp_part in opp_str.split(','):
                opp_part = opp_part.strip()
                status, terr, stage = parse_opportunity(opp_part)
                if stage:
                    stage_key = stage.lower().replace(' ', '_')
                    if stage_key in pc['opportunities']:
                        pc['opportunities'][stage_key] += 1

        # Lifecycle
        lifecycle = row.get('Customer Life Cycle', '').strip().lower()
        lc_map = {
            'lead': 'lead',
            'marketing qualified lead': 'mql',
            'sales qualified lead': 'sql',
            'opportunity': 'opportunity',
            'customer': 'customer',
        }
        if lifecycle in lc_map:
            pc['lifecycle'][lc_map[lifecycle]] += 1

        # Source
        source = row.get('Source', '').strip()
        if source:
            # Normalize source names (case variations)
            source_lower = source.lower()
            if 'request a quote' in source_lower:
                source = 'Quote Form'
            elif 'decision stage' in source_lower:
                source = 'Ebook - Decision Stage'
            elif 'awareness stage' in source_lower:
                source = 'Ebook - Awareness Stage'
            elif 'consideration stage' in source_lower:
                source = 'Ebook - Consideration Stage'
            elif 'hiring' in source_lower:
                source = 'Hiring Form'
            elif 'measure sheet' in source_lower:
                source = 'Customer Measure Sheet'
            pc['sources'][source] += 1

        # How heard about us
        heard = row.get('*How did you hear about us?*', '').strip()
        if heard:
            # Can have multiple comma-separated values
            for h in heard.split(','):
                h = h.strip()
                if h:
                    pc['heard_about'][h] += 1

        # Window frames
        window = row.get('*What are your window frames made of?*', '').strip()
        if window:
            pc['window_types'][window] += 1

        # Lead type from tags
        tags = row.get('Tags', '').strip()
        if tags:
            tag_lower = tags.lower()
            if 'quote requested' in tag_lower:
                pc['lead_types']['Quote Requested'] += 1
            if 'e-book download' in tag_lower:
                pc['lead_types']['Ebook Download'] += 1
            if 'cold outreach' in tag_lower:
                pc['lead_types']['Cold Outreach'] += 1
            if 'hubspot imported' in tag_lower:
                pc['lead_types']['Hubspot Import'] += 1
            if 'servicem8 created' in tag_lower:
                pc['lead_types']['ServiceM8'] += 1

        # Created date
        created = parse_date(row.get('Created', ''))
        if created:
            pc['created_dates'].append(created['date'])

    print(f"  Processed: {processed}")
    print(f"  Skipped (no postcode): {skipped_no_postcode}")
    print(f"  Unique postcodes: {len(postcode_data)}")
    print(f"  Unique territories: {len(territories_found)}")

    # ── Serialize leads_by_postcode ──
    print("\nBuilding output JSON...")
    output = {}
    for postcode, data in postcode_data.items():
        # Convert defaultdicts to regular dicts
        entry = {
            'total': data['total'],
            'state': data['state'],
            'territory': data['franchise_territory'],
            'opportunities': data['opportunities'],
            'lifecycle': data['lifecycle'],
            'sources': dict(data['sources']),
            'heard_about': dict(data['heard_about']),
            'window_types': dict(data['window_types']),
            'lead_types': dict(data['lead_types']),
        }

        # Add date histogram (leads per month)
        if data['created_dates']:
            monthly = defaultdict(int)
            for d in data['created_dates']:
                month_key = d[:7]  # YYYY-MM
                monthly[month_key] += 1
            entry['monthly'] = dict(sorted(monthly.items()))
            entry['earliest'] = min(data['created_dates'])
            entry['latest'] = max(data['created_dates'])

        output[postcode] = entry

    leads_path = os.path.join(OUTPUT_DIR, 'leads_by_postcode.json')
    with open(leads_path, 'w') as f:
        json.dump(output, f, separators=(',', ':'))
    print(f"  Written: {leads_path} ({len(output)} postcodes)")

    # ── Build territories.json ──
    print("\nBuilding territories...")

    # Load existing territories for postcode lists
    existing_territories_path = os.path.join(OUTPUT_DIR, 'territories.json')
    existing = {}
    if os.path.exists(existing_territories_path):
        with open(existing_territories_path, 'r') as f:
            existing = json.load(f)

    territories_output = {
        'territories': {},
        'metadata': {
            'total_contacts': len(contacts),
            'processed_contacts': processed,
            'unique_postcodes': len(output),
            'generated': datetime.now().isoformat(),
        }
    }

    # Merge existing territory data with new data
    all_territory_names = territories_found | set(existing.get('territories', {}).keys())

    for name in sorted(all_territory_names):
        if name in ('Undefined', 'Rework', '3. Supplier / Resource', 'Standard Job'):
            continue

        existing_terr = existing.get('territories', {}).get(name, {})

        # Determine state
        state = TERRITORY_STATE.get(name) or territory_states.get(name) or existing_terr.get('state')

        # Get postcodes from both existing and new data
        postcodes = set(existing_terr.get('postcodes', []))
        postcodes |= territory_postcodes.get(name, set())

        territories_output['territories'][name] = {
            'name': name,
            'state': state,
            'color': TERRITORY_COLORS.get(name, existing_terr.get('color', '#999999')),
            'postcodes': sorted(postcodes),
            'contact_count': sum(1 for pc in postcodes if pc in output),
            'lead_count': sum(output.get(pc, {}).get('total', 0) for pc in postcodes),
        }

    terr_path = os.path.join(OUTPUT_DIR, 'territories.json')
    with open(terr_path, 'w') as f:
        json.dump(territories_output, f, indent=2)
    print(f"  Written: {terr_path} ({len(territories_output['territories'])} territories)")

    # ── Summary stats ──
    print("\n=== SUMMARY ===")
    print(f"Total contacts in CSV:    {len(contacts):,}")
    print(f"With valid postcode:      {processed:,}")
    print(f"Unique postcodes:         {len(output):,}")
    print(f"Territories:              {len(territories_output['territories'])}")
    print()

    total_opps = sum(sum(v['opportunities'].values()) for v in output.values())
    total_won = sum(v['opportunities']['won'] for v in output.values())
    total_lost = sum(v['opportunities']['lost'] for v in output.values())
    print(f"Total opportunities:      {total_opps:,}")
    print(f"Won:                      {total_won:,}")
    print(f"Lost:                     {total_lost:,}")
    print()

    by_state = defaultdict(int)
    for pc_data in output.values():
        if pc_data['state']:
            by_state[pc_data['state']] += pc_data['total']
    print("Leads by state:")
    for s, c in sorted(by_state.items(), key=lambda x: -x[1]):
        print(f"  {s}: {c:,}")


if __name__ == '__main__':
    main()
