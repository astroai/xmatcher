#!/bin/bash
# Probe + bootstrap KiDS DR5 staging paths (ESO / Astro-WISE). Multi-band cat wget is currently 404 on Leiden.
set -euo pipefail
KP=/arc/projects/k-pop/pensus
HOST=/arc/projects/skyai
STAGE=$HOST/pensus/staging/kids_dr5
LOG=$KP/logs/kids_dr5_probe_$(date -u +%Y%m%dT%H%M%SZ).log
exec > >(tee -a "$LOG") 2>&1
echo "start $(date -Is)"
mkdir -p "$STAGE/raw" "$KP/inputs/kids_dr5_hats"
Q=$(getfattr --only-values -n ceph.quota.max_bytes /arc/projects/skyai)
U=$(getfattr --only-values -n ceph.dir.rbytes /arc/projects/skyai)
echo "skyai free_gib=$(( (Q-U)/2**30 ))"
(( Q-U > 800*2**30 )) || { echo FAIL skyai free; exit 1; }

python3 - <<'PY'
import json, pathlib, datetime, urllib.request, ssl, re
stage=pathlib.Path('/arc/projects/skyai/pensus/staging/kids_dr5')
ctx=ssl.create_default_context()
notes=[]
# Leiden multi-band cat wget status
for name in ['kids_dr5.0_cat_wget.sh','kids_dr5.0_src_wget.sh']:
  url=f'https://kids.strw.leidenuniv.nl/DR5/{name}'
  try:
    with urllib.request.urlopen(url, timeout=30, context=ctx) as r:
      notes.append({'url':url,'status':r.status,'bytes':r.headers.get('Content-Length')})
      if name.endswith('src_wget.sh'):
        body=r.read(2000).decode('utf-8','replace')
        (stage/'raw'/name).write_text(body if len(body)<500000 else body[:500000])
  except Exception as e:
    notes.append({'url':url,'error':f'{type(e).__name__}: {e}'})

# Sample Astro-WISE single-band source list (not multi-band catalogue)
sample='http://ds.astro.rug.astro-wise.org:8000/KiDS_DR5.0_136.4_2.5_r_src.fits'
try:
  req=urllib.request.Request(sample, method='HEAD')
  with urllib.request.urlopen(req, timeout=30, context=ctx) as r:
    notes.append({'sample_src':sample,'status':r.status,'length':r.headers.get('Content-Length')})
except Exception as e:
  notes.append({'sample_src':sample,'error':str(e)})

# ESO programmatic page hint
notes.append({
  'eso_programmatic':'https://archive.eso.org/programmatic/#TAP',
  'safcat_table':'KiDS_DR5_0_ugriZYJHKs_cat_fits',
  'blocker':'Leiden multi-band cat wget (kids_dr5.0_cat_wget.sh) returns 404; ESO QI/safcat needs authenticated access from this environment'
})
ptr=pathlib.Path('/arc/projects/k-pop/pensus/inputs/kids_dr5_hats/SOURCE.json')
ptr.write_text(json.dumps({
  'alias':'kids_dr5_hats',
  'survey':'kids',
  'release':'dr5',
  'planned_hats_path':'/arc/projects/hats/kids/dr5',
  'stage_root':str(stage),
  'exists':False,
  'hats_ready':False,
  'blocker':'multi-band catalogue wget not published (404); ESO safcat needs login',
  'probe_notes':notes,
  'probed_at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),
}, indent=2, sort_keys=True)+'\n')
print(json.dumps(notes, indent=2))
print('wrote', ptr)
PY
echo "done $(date -Is) LOG=$LOG"
