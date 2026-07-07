"""Regressions-Netz für services.ratelimit (Audit-Befunde #2 + #4).

#2 (hoch): Das Rate-Limit-Keying nutzte den client-kontrollierten linkesten
X-Forwarded-For-Eintrag → per Request rotierbar → Limit umgehbar → Kosten-/
DoS-Amplifikation. Jetzt: X-Real-IP (vom Trusted-Host-nginx gesetzt, nicht
fälschbar).
#4 (mittel): Der Store löschte Keys nie → OOM. Jetzt: opportunistische
Räumung stale Keys, beschränkte Größe.

Dependency-light: pure stdlib, läuft in der GitHub-CI.
"""
from services.ratelimit import get_client_ip, RateLimiter


# --- get_client_ip: X-Real-IP ist die vertrauenswürdige Quelle -----------

def test_x_real_ip_preferred_over_xff():
    # Der Angreifer setzt einen gefälschten XFF; X-Real-IP (vom Host-nginx)
    # muss gewinnen.
    headers = {"x-forwarded-for": "6.6.6.6", "x-real-ip": "1.2.3.4"}
    assert get_client_ip(headers, "10.0.0.1") == "1.2.3.4"


def test_xff_is_never_trusted():
    # Ohne X-Real-IP wird NICHT der spoofbare XFF genommen, sondern der Peer.
    headers = {"x-forwarded-for": "6.6.6.6"}
    assert get_client_ip(headers, "10.0.0.1") == "10.0.0.1"


def test_fallback_to_peer_then_unknown():
    assert get_client_ip({}, "10.0.0.1") == "10.0.0.1"
    assert get_client_ip({}, None) == "unknown"


def test_real_ip_whitespace_stripped():
    assert get_client_ip({"x-real-ip": "  1.2.3.4  "}, None) == "1.2.3.4"


# --- RateLimiter: Keying schließt den Spoofing-Bypass --------------------

def test_rotating_xff_same_real_ip_is_one_bucket():
    # Der Kern des Bugs: 12 Requests, rotierender XFF, gleiche X-Real-IP.
    # Vor dem Fix: 12 Keys, nie geblockt. Jetzt: 1 Key, nach 10 geblockt.
    rl = RateLimiter(limit=10, window=60)
    allowed = 0
    for i in range(12):
        ip = get_client_ip(
            {"x-forwarded-for": f"{i}.{i}.{i}.{i}", "x-real-ip": "9.9.9.9"}, None)
        if rl.allow(ip, now=1000.0):
            allowed += 1
    assert allowed == 10
    assert len(rl) == 1


def test_limit_blocks_then_window_expiry_allows_again():
    rl = RateLimiter(limit=3, window=60)
    assert rl.allow("1.2.3.4", now=100.0) is True
    assert rl.allow("1.2.3.4", now=101.0) is True
    assert rl.allow("1.2.3.4", now=102.0) is True
    assert rl.allow("1.2.3.4", now=103.0) is False   # 4. im Fenster → blockiert
    # 61 s später ist das Fenster durch → wieder erlaubt
    assert rl.allow("1.2.3.4", now=164.0) is True


def test_distinct_ips_are_independent():
    rl = RateLimiter(limit=1, window=60)
    assert rl.allow("1.1.1.1", now=100.0) is True
    assert rl.allow("1.1.1.1", now=100.0) is False
    assert rl.allow("2.2.2.2", now=100.0) is True     # andere IP unbetroffen


def test_store_evicts_stale_keys_bounded_memory():
    # #4: Store darf nicht unbegrenzt wachsen. Nach max_keys werden Keys
    # ohne Request im Fenster geräumt.
    rl = RateLimiter(limit=5, window=60, max_keys=100)
    # 200 alte Keys (t=0), dann ein Request weit später → Sweep räumt alle
    # stale Keys, sobald der Store max_keys überschreitet.
    for i in range(150):
        rl.allow(f"old-{i}", now=0.0)
    assert len(rl) == 150
    rl.allow("fresh", now=1000.0)   # > max_keys → Sweep: alle t=0-Keys stale
    assert len(rl) == 1             # nur der frische Key überlebt
