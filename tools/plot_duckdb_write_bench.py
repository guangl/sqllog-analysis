# Small script to parse Criterion JSON outputs and plot a comparison PNG
import json
from pathlib import Path
import matplotlib.pyplot as plt

root = Path('target/criterion/duckdb_write_modes')
if not root.exists():
    raise SystemExit('Criterion output directory not found: ' + str(root))

modes = ['appender_direct', 'in_memory_ctas', 'csv_copy']
sizes = [10000, 50000, 200000]

# We'll collect median times (in seconds) from base/estimates.json (or new/estimates.json)
results = {m: {} for m in modes}

for m in modes:
    for s in sizes:
        path = root / m / str(s) / 'base' / 'estimates.json'
        if not path.exists():
            path = root / m / str(s) / 'new' / 'benchmark.json'
        if not path.exists():
            print('Missing file for', m, s)
            continue
        data = json.loads(path.read_text(encoding='utf-8'))
        # Heuristic: try to find a median/point estimate in common places
        median = None
        if isinstance(data, dict):
            # Criterion's estimates.json typically has 'median' as an object
            if 'median' in data and isinstance(data['median'], dict):
                median = data['median'].get('point_estimate')
            elif 'mean' in data and isinstance(data['mean'], dict):
                median = data['mean'].get('point_estimate')
            else:
                # fallback: search nested dicts for point_estimate
                for v in data.values():
                    if isinstance(v, dict) and 'point_estimate' in v:
                        median = v['point_estimate']
                        break
        if median is None:
            print('Could not find median for', m, s)
            continue
        # median may be in nanoseconds depending on file - detect and normalize
        # Criterion usually reports seconds as a float in estimates.json
        results[m][s] = float(median)

# Plot: convert times to seconds if they're in nanoseconds (detect large numbers)
for m in modes:
    for s, t in list(results[m].items()):
        if t > 1e6:
            # assume nanoseconds
            results[m][s] = t / 1e9
        elif t > 1e3:
            # assume microseconds
            results[m][s] = t / 1e6

# Draw
out_dir = Path('docs/bench_results')
out_dir.mkdir(parents=True, exist_ok=True)
plt.figure(figsize=(8,5))
for m in modes:
    xs = sorted(results[m].keys())
    ys = [results[m][x] for x in xs]
    plt.plot(xs, ys, marker='o', label=m)

plt.xscale('log')
plt.xlabel('Number of records')
plt.ylabel('Median time (s)')
plt.title('DuckDB write modes - median time')
plt.legend()
plt.grid(True, which='both', ls='--', alpha=0.4)
out_path = out_dir / 'duckdb_write_modes.png'
plt.savefig(out_path, dpi=150)
print('Wrote', out_path)
