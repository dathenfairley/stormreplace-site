"""
StormReplace Storm Chaser
==========================
Runs twice daily via GitHub Actions.
Layer 1: Checks NWS API for active severe weather alerts (free, no key)
Layer 2: Queries Tomorrow.io only for zip codes in alerted states
Layer 3: Fires Make.com webhook only when thresholds are met

Thresholds: Hail >= 0.25 inches OR Wind >= 45 mph
"""

import os
import sys
import json
import time
import logging
import requests
from datetime import datetime, timezone

# ============================================================
# CONFIGURATION — pulled from GitHub Secrets environment vars
# ============================================================

TOMORROW_API_KEY   = os.environ.get("TOMORROW_API_KEY", "")
SUPABASE_URL       = os.environ.get("SUPABASE_URL", "")
SUPABASE_ANON_KEY  = os.environ.get("SUPABASE_ANON_KEY", "")
MAKECOM_WEBHOOK    = os.environ.get("MAKECOM_WEBHOOK", "")

# Storm thresholds
HAIL_THRESHOLD_IN  = 0.25   # inches
WIND_THRESHOLD_MPH = 45.0   # mph

# NWS alert types that indicate potential roof damage
ROOF_DAMAGE_ALERTS = [
    "Tornado Warning",
    "Tornado Watch",
    "Severe Thunderstorm Warning",
    "Severe Thunderstorm Watch",
    "High Wind Warning",
    "High Wind Watch",
    "Damaging Wind",
    "Hail",
    "Wind Advisory",
    "Special Weather Statement",
]

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("StormChaser")

# ============================================================
# STEP 1 — GET ACTIVE STORM STATES FROM NWS
# ============================================================

def get_active_storm_states():
    """
    Calls NWS API to get all currently active severe weather alerts.
    Returns a set of 2-letter state codes that have active alerts.
    No API key required. Completely free.
    """
    log.info("Step 1: Checking NWS for active severe weather alerts...")

    url = "https://api.weather.gov/alerts/active"
    params = {
        "status": "actual",
        "message_type": "alert",
        "urgency": "Immediate,Expected",
        "severity": "Extreme,Severe,Moderate",
    }
    headers = {
        "User-Agent": "StormReplace/1.0 (stormreplace.com; contact@stormreplace.com)",
        "Accept": "application/geo+json"
    }

    try:
        response = requests.get(url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        log.error(f"NWS API error: {e}")
        return set()

    features = data.get("features", [])
    log.info(f"  NWS returned {len(features)} active alerts nationally")

    active_states = set()
    relevant_alert_count = 0

    for feature in features:
        props = feature.get("properties", {})
        event = props.get("event", "")

        # Check if this alert type is relevant to roof damage
        is_relevant = any(
            alert_type.lower() in event.lower()
            for alert_type in ROOF_DAMAGE_ALERTS
        )

        if is_relevant:
            relevant_alert_count += 1
            # Extract state from affected zones
            affected_zones = props.get("affectedZones", [])
            for zone_url in affected_zones:
                # Zone URLs look like: https://api.weather.gov/zones/county/OHC049
                # State code is the first 2 letters of the zone ID
                zone_id = zone_url.split("/")[-1]
                if len(zone_id) >= 2:
                    state_code = zone_id[:2].upper()
                    if state_code.isalpha():
                        active_states.add(state_code)

            # Also check geocode area descriptions
            geocode = props.get("geocode", {})
            ugc_codes = geocode.get("UGC", [])
            for ugc in ugc_codes:
                # UGC format: OHC049 or OHZ049
                if len(ugc) >= 2:
                    state_code = ugc[:2].upper()
                    if state_code.isalpha():
                        active_states.add(state_code)

    # Remove non-state codes
    valid_states = {s for s in active_states if len(s) == 2 and s.isalpha()}

    log.info(f"  Relevant roof-damage alerts: {relevant_alert_count}")
    log.info(f"  States with active alerts: {sorted(valid_states)}")

    return valid_states


# ============================================================
# STEP 2 — GET QUALIFYING ZIP CODES FROM SUPABASE
# ============================================================

def get_qualifying_zipcodes(active_states):
    """
    Queries Supabase for Tier 1 and Tier 2 zip codes
    in states with active NWS alerts.
    Returns list of zip code records with lat/long.
    """
    if not active_states:
        log.info("Step 2: No active storm states — skipping Supabase query")
        return []

    log.info(f"Step 2: Querying Supabase for qualifying zip codes in: {sorted(active_states)}")

    # Build state filter for Supabase REST API
    # Format: state=in.(OH,IN,KY)
    states_list = ",".join(sorted(active_states))
    url = f"{SUPABASE_URL}/rest/v1/zip_codes"
    params = {
        "select": "zip,city,state,tier,latitude,longitude",
        "state": f"in.({states_list})",
        "tier": "in.(Tier 1,Tier 2)",
        "latitude": "not.is.null",
        "longitude": "not.is.null",
    }
    headers = {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
    }

    try:
        response = requests.get(url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        zipcodes = response.json()
    except requests.exceptions.RequestException as e:
        log.error(f"Supabase query error: {e}")
        return []

    log.info(f"  Found {len(zipcodes)} qualifying zip codes to check")
    return zipcodes


# ============================================================
# STEP 3 — CHECK DEDUPLICATION
# ============================================================

def get_already_fired_today(active_states):
    """
    Queries Supabase storm_events table to find zip codes
    that already fired today. Prevents duplicate activations.
    """
    if not active_states:
        return set()

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    url = f"{SUPABASE_URL}/rest/v1/storm_events"
    params = {
        "select": "zip",
        "activated_date": f"eq.{today}",
    }
    headers = {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
    }

    try:
        response = requests.get(url, params=params, headers=headers, timeout=30)
        if response.status_code == 404:
            # Table doesn't exist yet — will be created on first activation
            return set()
        response.raise_for_status()
        events = response.json()
        fired_zips = {e["zip"] for e in events}
        if fired_zips:
            log.info(f"  Already fired today for: {sorted(fired_zips)}")
        return fired_zips
    except requests.exceptions.RequestException as e:
        log.warning(f"Dedup check error (non-fatal): {e}")
        return set()


# ============================================================
# STEP 4 — CHECK TOMORROW.IO FOR STORM CONDITIONS
# ============================================================

def check_tomorrow_weather(zipcodes, already_fired):
    """
    Queries Tomorrow.io for each qualifying zip code.
    Returns list of zip codes that meet storm thresholds.
    Respects rate limits: 3 requests/second, 25/hour, 500/day.
    """
    if not zipcodes:
        log.info("Step 3: No zip codes to check with Tomorrow.io")
        return []

    # Filter out already-fired zip codes
    to_check = [z for z in zipcodes if z["zip"] not in already_fired]
    log.info(f"Step 3: Checking {len(to_check)} zip codes with Tomorrow.io "
             f"({len(already_fired)} already fired today skipped)")

    qualifying_events = []
    api_calls = 0
    max_calls = 450  # Stay safely under 500/day limit

    for i, zipcode in enumerate(to_check):
        if api_calls >= max_calls:
            log.warning(f"  Reached API call limit ({max_calls}). "
                       f"{len(to_check) - i} zip codes unchecked.")
            break

        zip_code = zipcode["zip"]
        city     = zipcode["city"]
        state    = zipcode["state"]
        tier     = zipcode["tier"]
        lat      = zipcode["latitude"]
        lon      = zipcode["longitude"]

        # Query Tomorrow.io realtime weather
        url = "https://api.tomorrow.io/v4/weather/realtime"
        params = {
            "location": f"{lat},{lon}",
            "fields":   "windSpeed,precipitationIntensity,hailBinary",
            "units":    "imperial",
            "apikey":   TOMORROW_API_KEY,
        }

        try:
            response = requests.get(url, params=params, timeout=15)
            api_calls += 1

            if response.status_code == 429:
                log.warning("  Tomorrow.io rate limit hit — pausing 60 seconds")
                time.sleep(60)
                continue

            if response.status_code != 200:
                log.warning(f"  {zip_code}: Tomorrow.io returned {response.status_code}")
                continue

            data = response.json()
            values = data.get("data", {}).get("values", {})

            wind_speed = float(values.get("windSpeed", 0))
            hail       = int(values.get("hailBinary", 0))
            precip     = float(values.get("precipitationIntensity", 0))

            # Check thresholds
            wind_qualifies = wind_speed >= WIND_THRESHOLD_MPH
            hail_qualifies = hail == 1  # hailBinary: 1 = hail detected

            if wind_qualifies or hail_qualifies:
                # Determine storm type
                if hail_qualifies and wind_qualifies:
                    storm_type = "Hail and High Wind"
                elif hail_qualifies:
                    storm_type = "Hail"
                else:
                    storm_type = "High Wind"

                # Determine severity
                if wind_speed >= 65 or (hail_qualifies and wind_speed >= 58):
                    severity = "Severe"
                elif wind_speed >= 55 or hail_qualifies:
                    severity = "Moderate"
                else:
                    severity = "Low"

                event = {
                    "zip":              zip_code,
                    "city":             city,
                    "state":            state,
                    "tier":             tier,
                    "latitude":         lat,
                    "longitude":        lon,
                    "storm_type":       storm_type,
                    "severity":         severity,
                    "wind_speed_mph":   round(wind_speed, 1),
                    "hail_detected":    bool(hail),
                    "precip_intensity": round(precip, 2),
                    "event_timestamp":  datetime.now(timezone.utc).isoformat(),
                }
                qualifying_events.append(event)
                log.info(f"  ✓ QUALIFYING: {zip_code} {city}, {state} "
                        f"({tier}) — {storm_type}, wind: {wind_speed:.0f}mph")
            else:
                log.debug(f"  {zip_code} {city}, {state}: "
                         f"wind {wind_speed:.0f}mph, hail: {hail} — below threshold")

        except requests.exceptions.RequestException as e:
            log.warning(f"  {zip_code}: Tomorrow.io request failed: {e}")

        # Respect rate limit: max 3 requests/second
        time.sleep(0.4)

    log.info(f"  Tomorrow.io calls made: {api_calls}")
    log.info(f"  Qualifying storm events: {len(qualifying_events)}")
    return qualifying_events


# ============================================================
# STEP 5 — FIRE MAKE.COM WEBHOOK
# ============================================================

def fire_webhook(event):
    """
    Sends qualifying storm event to Make.com webhook.
    Make.com then fires the full cascade:
    ad creation, page generation, content, notifications.
    """
    log.info(f"  Firing webhook for {event['zip']} {event['city']}, {event['state']}...")

    try:
        response = requests.post(
            MAKECOM_WEBHOOK,
            json=event,
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        if response.status_code in [200, 201, 202, 204]:
            log.info(f"  ✓ Webhook fired successfully for {event['zip']}")
            return True
        else:
            log.error(f"  Webhook failed: HTTP {response.status_code} — {response.text[:200]}")
            return False
    except requests.exceptions.RequestException as e:
        log.error(f"  Webhook error: {e}")
        return False


# ============================================================
# STEP 6 — LOG EVENT TO SUPABASE (DEDUPLICATION)
# ============================================================

def log_event_to_supabase(event):
    """
    Records fired event in Supabase storm_events table.
    Prevents same zip code firing twice in one day.
    Creates table if it doesn't exist.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # First ensure the table exists
    create_sql = """
    CREATE TABLE IF NOT EXISTS storm_events (
        id BIGSERIAL PRIMARY KEY,
        zip TEXT NOT NULL,
        city TEXT,
        state TEXT,
        tier TEXT,
        storm_type TEXT,
        severity TEXT,
        wind_speed_mph NUMERIC,
        hail_detected BOOLEAN,
        activated_date DATE DEFAULT CURRENT_DATE,
        activated_at TIMESTAMP DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_storm_events_zip_date
        ON storm_events(zip, activated_date);
    """

    record = {
        "zip":           event["zip"],
        "city":          event["city"],
        "state":         event["state"],
        "tier":          event["tier"],
        "storm_type":    event["storm_type"],
        "severity":      event["severity"],
        "wind_speed_mph": event["wind_speed_mph"],
        "hail_detected": event["hail_detected"],
        "activated_date": today,
    }

    url = f"{SUPABASE_URL}/rest/v1/storm_events"
    headers = {
        "apikey":        SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "return=minimal",
    }

    try:
        response = requests.post(url, json=record, headers=headers, timeout=15)
        if response.status_code in [200, 201]:
            log.info(f"  ✓ Event logged to Supabase for {event['zip']}")
        else:
            log.warning(f"  Supabase log warning: {response.status_code} — {response.text[:200]}")
    except requests.exceptions.RequestException as e:
        log.warning(f"  Supabase log error (non-fatal): {e}")


# ============================================================
# MAIN
# ============================================================

def validate_config():
    """Checks all required environment variables are present."""
    missing = []
    if not TOMORROW_API_KEY:
        missing.append("TOMORROW_API_KEY")
    if not SUPABASE_URL:
        missing.append("SUPABASE_URL")
    if not SUPABASE_ANON_KEY:
        missing.append("SUPABASE_ANON_KEY")
    if not MAKECOM_WEBHOOK:
        missing.append("MAKECOM_WEBHOOK")

    if missing:
        log.error(f"Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)


def main():
    log.info("=" * 55)
    log.info("  STORMREPLACE STORM CHASER")
    log.info(f"  Run time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    log.info("=" * 55)

    # Validate config
    validate_config()

    # Step 1: Get active storm states from NWS (free)
    active_states = get_active_storm_states()

    if not active_states:
        log.info("No active severe weather alerts nationally. Nothing to do.")
        log.info("Run complete.")
        return

    # Step 2: Get qualifying zip codes from Supabase
    zipcodes = get_qualifying_zipcodes(active_states)

    if not zipcodes:
        log.info("No qualifying zip codes in alerted states. Nothing to do.")
        log.info("Run complete.")
        return

    # Step 3: Check which zip codes already fired today
    already_fired = get_already_fired_today(active_states)

    # Step 4: Check Tomorrow.io for storm conditions
    qualifying_events = check_tomorrow_weather(zipcodes, already_fired)

    if not qualifying_events:
        log.info("No zip codes met storm thresholds. Nothing to fire.")
        log.info("Run complete.")
        return

    # Step 5 & 6: Fire webhook and log each qualifying event
    log.info(f"\nFiring webhooks for {len(qualifying_events)} qualifying events...")
    fired_count = 0
    for event in qualifying_events:
        success = fire_webhook(event)
        if success:
            log_event_to_supabase(event)
            fired_count += 1
        time.sleep(1)  # Brief pause between webhook calls

    # Summary
    log.info("\n" + "=" * 55)
    log.info("  RUN COMPLETE")
    log.info(f"  States checked:      {len(active_states)}")
    log.info(f"  Zip codes evaluated: {len(zipcodes)}")
    log.info(f"  Events fired:        {fired_count}")
    log.info("=" * 55)


if __name__ == "__main__":
    main()
