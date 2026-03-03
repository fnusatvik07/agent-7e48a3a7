#!/bin/bash
echo "============================================"
echo "  CHATBOT PROJECT — Bootstrap"
echo "============================================"
echo ""

echo "Project structure:"
ls -la 2>/dev/null
echo ""
[ -d backend ] && echo "Backend files:" && ls backend/ 2>/dev/null && echo ""
[ -d frontend ] && echo "Frontend files:" && ls frontend/src/ 2>/dev/null && echo ""

echo "Screenshots:"
ls screenshots/ 2>/dev/null || echo "  (none yet)"
echo ""

echo "Feature status:"
python3 -c "
import json, os
if not os.path.exists('feature_list.json'):
    print('  feature_list.json not found!')
else:
    with open('feature_list.json') as f:
        features = json.load(f)
    total = len(features)
    done = sum(1 for f in features if f.get('passes'))
    print(f'  {done}/{total} features passing')
    for f in features:
        status = 'PASS' if f.get('passes') else 'TODO'
        print(f'  [{status}] Feature {f[\"id\"]}: {f[\"description\"][:60]}')
"
echo ""
echo "============================================"
echo "  Bootstrap complete. Begin coding."
echo "============================================"
