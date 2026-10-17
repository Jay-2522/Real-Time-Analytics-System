"""Explicit endpoint inventory. New routes are denied until classified here."""
import re

READ_PATHS = (
    r'/analytics/summary', r'/events/recent', r'/predictions/[^/]+', r'/risks/high', r'/search/events',
    r'/dashboard/(overview|system-health|risk-trends|event-volume|severity-distribution|event-type-distribution|top-entities)',
    r'/alerts/(recent|high|critical|stats|search)', r'/incidents/[^/]+',
    r'/monitoring/(overview|services|pipeline|errors|metrics-summary)',
    r'/threat-intel/(overview|iocs|entities|mitre|attack-timeline|risk-heatmap|threat-graph|search)',
    r'/detections/(rules|rules/[^/]+|rule-stats)', r'/entities/[^/]+', r'/mitre/(tactics|techniques)',
    r'/auth/me', r'/openapi.json',
)


def permission(path, method, prefix='/api/v1'):
    if method == 'GET' and path in ('/docs', '/redoc'):
        return 'viewer'
    if not path.startswith(prefix + '/'):
        return None
    path = path[len(prefix):]
    if path in ('/auth/users', '/auth/audit') and method == 'GET':
        return 'administrator'
    if path == '/auth/users' and method == 'POST':
        return 'administrator'
    if re.fullmatch(r'/auth/users/[^/]+', path) and method == 'PATCH':
        return 'administrator'
    if method == 'GET' and any(re.fullmatch(pattern, path) for pattern in READ_PATHS):
        return 'viewer'
    if path == '/auth/logout' and method == 'POST':
        return 'viewer'
    if method == 'PATCH' and re.fullmatch(r'/(alerts|incidents)/[^/]+/status', path):
        return 'analyst'
    return None
