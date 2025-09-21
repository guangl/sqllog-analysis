import json
from pathlib import Path
import matplotlib.pyplot as plt

root = Path('target/criterion')
if not root.exists():
    raise SystemExit('Criterion output directory not found: ' + str(root))

def extract_series(group_name):
    base = root / group_name
    if not base.exists():
        print('No group', group_name)
        return None
    sizes = []
    vals = []
    for d in sorted(base.iterdir(), key=lambda p: p.name):
        if not d.is_dir():
            continue
        est = d / 'base' / 'estimates.json'
        if not est.exists():
            est = d / 'new' / 'benchmark.json'
        if not est.exists():
            continue
        data = json.loads(est.read_text(encoding='utf-8'))
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
            continue
        t = float(median)
        if t > 1e6:
            t = t / 1e9
        elif t > 1e3:
            t = t / 1e6
        try:
            sizes.append(int(d.name))
        except:
            # if folder names aren't numeric, try to parse like '100000'
            sizes.append(len(sizes))
        vals.append(t)
    return sizes, vals

out_dir = Path('docs/bench_results')
out_dir.mkdir(parents=True, exist_ok=True)

for group in ['sqllog_write_file', 'sqllog_parse_file']:
    series = extract_series(group)
    if not series:
        continue
    sizes, vals = series
    plt.figure(figsize=(8,4))
    plt.plot(sizes, vals, marker='o')
    plt.xscale('log')
    plt.xlabel('Number of records')
    plt.ylabel('Median time (s)')
    plt.title(group)
    plt.grid(True, which='both', ls='--', alpha=0.4)
    out = out_dir / f'{group}.png'
    plt.savefig(out, dpi=150)
    plt.close()
    print('Wrote', out)
