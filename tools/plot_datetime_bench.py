import json
from pathlib import Path
import matplotlib.pyplot as plt

root = Path('target/criterion/datetime_validation')
if not root.exists():
    raise SystemExit('Criterion output directory not found: ' + str(root))

# find all sub-bench folders (e.g., custom_valid, regex_valid...)
bench_names = [p.name for p in root.iterdir() if p.is_dir() and p.name != 'report']
bench_names.sort()

results = {}
for name in bench_names:
    # estimates.json is under base/estimates.json for these benches
    p = root / name / 'base' / 'estimates.json'
    if not p.exists():
        p = root / name / 'new' / 'benchmark.json'
    if not p.exists():
        print('Missing estimates for', name)
        continue
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
        print('Could not find median for', name)
        continue
    # normalize units (Criterion often uses nanoseconds)
    t = float(median)
    if t > 1e6:
        t = t / 1e9
    elif t > 1e3:
        t = t / 1e6
    results[name] = t

# plot
out_dir = Path('docs/bench_results')
out_dir.mkdir(parents=True, exist_ok=True)
plt.figure(figsize=(8,4))
names = list(results.keys())
vals = [results[n] for n in names]
plt.bar(names, vals)
plt.ylabel('Median time (s)')
plt.title('datetime_validation benchmarks (median)')
plt.xticks(rotation=45)
plt.tight_layout()
out = out_dir / 'datetime_validation.png'
plt.savefig(out, dpi=150)
print('Wrote', out)
