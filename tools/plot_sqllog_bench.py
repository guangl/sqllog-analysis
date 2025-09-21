import json
from pathlib import Path
import matplotlib.pyplot as plt

root = Path('target/criterion/sqllog_from_file_1m')
if not root.exists():
    raise SystemExit('Criterion output directory not found: ' + str(root))

# Estimates are under base/estimates.json
p = root / 'base' / 'estimates.json'
if not p.exists():
    p = root / 'new' / 'benchmark.json'
if not p.exists():
    raise SystemExit('No estimates found in ' + str(root))

data = json.loads(p.read_text(encoding='utf-8'))
median = None
if isinstance(data, dict):
    if 'median' in data and isinstance(data['median'], dict):
        median = data['median'].get('point_estimate')
    elif 'mean' in data and isinstance(data['mean'], dict):
        median = data['mean'].get('point_estimate')
    else:
        for v in data.values():
            if isinstance(v, dict) and 'point_estimate' in v:
                median = v['point_estimate']
                break
if median is None:
    raise SystemExit('Could not find median in estimates')

# normalize
t = float(median)
if t > 1e6:
    t = t / 1e9
elif t > 1e3:
    t = t / 1e6

out_dir = Path('docs/bench_results')
out_dir.mkdir(parents=True, exist_ok=True)
plt.figure(figsize=(6,4))
plt.bar(['sqllog_from_file_1m'], [t])
plt.ylabel('Median time (s)')
plt.title('sqllog_from_file_1m')
plt.tight_layout()
out = out_dir / 'sqllog_from_file_1m.png'
plt.savefig(out, dpi=150)
print('Wrote', out)
