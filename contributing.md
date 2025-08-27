# Contributing to Edge Dynamics

Thanks for helping make **Edge Dynamics** better! This project welcomes issues and pull requests.

---

## License & IP
- **Code:** Apache-2.0  
- **Docs & charts:** CC-BY-4.0  
- By contributing, you agree your changes are licensed under the same terms.
- Add SPDX headers to new source files:
  ```
  # SPDX-License-Identifier: Apache-2.0
  ```

> **No CLA for now.** We use the DCO for contributor certification (see below). If policy changes, we’ll update this page.

---

## Inbound rights: DCO (no CLA)
We use the **Developer Certificate of Origin (DCO)**. By signing off your commits, you assert you wrote the code or have the right to contribute it.  
DCO text: https://developercertificate.org/

### How to sign your commits
Create commits with a `Signed-off-by:` trailer:
```bash
git commit -s -m "feat(agent): add per-topic zstd dict trainer"
```

Quality-of-life alias:
```bash
git config --global alias.cis 'commit -s'
# usage: git cis -m "message"
```

Forgot to sign existing commits in your branch?
```bash
git rebase --signoff origin/main
git push --force-with-lease
```

> Using GPG/SSH commit signing? Keep it on—DCO adds a trailer; it does not replace cryptographic signatures.

### Maintainer enforcement
- Enable a DCO check in CI (e.g., install the GitHub App **probot/dco**).
- Protect `main` to require the DCO status to pass before merge.

---

## How to contribute
1. **Discuss** – Open an issue for bugs, design proposals, or significant changes. Attach minimal repro/stats if relevant (see *Testing & Repro*).
2. **Branch** – Create a feature branch off `main` (e.g., `feat/agent-adaptive-batching`).
3. **Code** – Keep PRs small and focused. Add comments where behavior is non‑obvious.
4. **Tests/Smoke** – Include a quick smoke script or commands to validate changes.
5. **Docs** – Update README/usage notes when behavior or interfaces change.
6. **Sign off** – Ensure every commit is DCO‑signed (`-s`).

### Commit messages
Conventional commits are encouraged (not required):
```
feat(agent): add adaptive flush based on p95 latency
fix(collector): handle unknown dict_id with fallback path
docs: document frame header and MQTT transport
```

---

## Project scope (quick)
- Edge batching & dictionary compression (zstd preferred; zlib/LZ4 optional for comparisons).
- Per‑topic dict lifecycle (train → version → rotate; dict_id tagging).
- Framed transport (TCP/MQTT/HTTP POST) with compact JSON header `{v, topic, dict_id, count, raw_len, comp_len, level}`.
- Metrics (bytes saved, latency budget, CPU/memory cost) and reproducible charts.

---

## Code style
- **Python ≥ 3.10.** Keep modules small and composable.
- Prefer `ujson` for speed where available; fall back to stdlib `json`.
- Use `logging` over `print` in non‑demo code (`INFO` default; `DEBUG` for benchmarks).
- Avoid global state; pass configuration explicitly.
- Keep CLI tools idempotent and non‑interactive by default.
- Add type hints where practical; don’t block contributions on 100% typing.

### Lint/format (suggested)
- `ruff` or `flake8` for lint; `black` for format.  
- Keep imports deterministic; prefer standard library first, then third‑party, then local.

---

## Testing & Repro
Include enough to reproduce your claim:
- **Inputs:** topic(s), dict size, batch policy (e.g., `BATCH_MAX=100`, `BATCH_MS=250`).
- **Metrics per flush:** `topic, msgs, raw_len, comp_len, ratio, dict_id, flush_ms`.
- **Environment:** CPU model, Python version, OS.
- **Artifacts:** CSV of results and any charts (PNG/SVG). Prefer code‑generated plots.

Example smoke:
```bash
# Train small dicts from samples
python3 train_dict.py --samples_root samples --dict_dir dicts --size 4096
# Start collector
python3 collector_server.py
# Run agent (will synthesize or use your feed)
python3 edge_agent.py
# Inspect metrics
column -t -s, metrics.csv | tail -n 5
```

---

## Security
- **Do not** include secrets/tokens in code, issues, or logs.
- For vulnerabilities, open a private advisory (GitHub Security Advisories) or email the maintainer.
- Always **compress → then encrypt** when sending frames over untrusted links.

---

## PR checklist
- [ ] Commits are **signed off** (`git commit -s`).
- [ ] Code under Apache‑2.0; docs/images under CC‑BY‑4.0.
- [ ] README/docs updated for any behavior/interface changes.
- [ ] Basic smoke test included or described; metrics captured if performance‑related.
- [ ] No credentials or private data in diffs.

---

## Governance (lightweight)
- Maintainers review and merge PRs.
- Significant design changes should be proposed in an issue first.
- We aim for consensus; maintainers decide when needed.

---

## Contact
Open an issue for technical questions; use private advisory/email for security concerns.

