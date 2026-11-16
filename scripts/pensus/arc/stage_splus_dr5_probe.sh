#!/bin/bash
# Probe S-PLUS DR5 access (splus.cloud). Bulk DR5 typically needs an account.
set -euo pipefail
KP=/arc/projects/k-pop/pensus
HOST=/arc/projects/mlao
STAGE=$HOST/pensus/staging/splus_dr5
LOG=$KP/logs/splus_dr5_probe_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is)"
mkdir -p "$STAGE" "$KP/inputs/splus_dr5_hats"
python3 - <<'PY'
import json, pathlib, datetime, urllib.request, ssl
ctx=ssl.create_default_context()
notes=[]
for url in [
  'https://splus.cloud/',
  'https://splus.cloud/api',
  'https://splus.cloud/public/',
  'https://datalab.noirlab.edu/tap/sync?REQUEST=doQuery&LANG=ADQL&FORMAT=csv&QUERY=SELECT%20TOP%205%20table_name%20FROM%20tap_schema.tables%20WHERE%20table_name%20LIKE%20%27%25splus%25%27',
]:
  try:
    with urllib.request.urlopen(url, timeout=45, context=ctx) as r:
      body=r.read(400).decode('utf-8','replace')
      notes.append({'url':url,'status':r.status,'head':body[:200]})
  except Exception as e:
    notes.append({'url':url,'error':f'{type(e).__name__}: {e}'})
ptr=pathlib.Path('/arc/projects/k-pop/pensus/inputs/splus_dr5_hats/SOURCE.json')
ptr.write_text(json.dumps({
  'alias':'splus_dr5_hats',
  'survey':'splus',
  'release':'dr5',
  'planned_hats_path':'/arc/projects/hats/splus/dr5',
  'stage_root':'/arc/projects/mlao/pensus/staging/splus_dr5',
  'exists':False,
  'hats_ready':False,
  'blocker':'splus.cloud DR5 bulk likely needs account credentials; no anonymous HATS found',
  'fallback_note':'Data Lab splus_dr4 available if DR5 credentials arrive; do not silently substitute',
  'probe_notes':notes,
  'probed_at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),
}, indent=2, sort_keys=True)+'\n')
print(json.dumps(notes, indent=2))
print('wrote', ptr)
PY
echo "done $(date -Is) LOG=$LOG"
