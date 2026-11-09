# ledgerline

Shared code and assets for the Poridhi **MLOps with Cloud (Season 2)** labs. Every lab runs on a fresh VM, so each lab clones this repository at a pinned tag in its first step.

| Path | What it is |
|---|---|
| `ledgerline/` | Python package: the `PaymentAuthorized` v1 event contract and a deterministic payment generator with fault knobs (duplicates, late events, bad values, hot keys) |
| `compose/epNN/lab-XX/` | Docker Compose files for a lab's services |
| `requirements/epNN/lab-XX.txt` | Pinned Python packages for a lab |
| `provided/epNN/lab-XX/` | Helper scripts a lab copies into its working folder |
| `checks/epNN/lab-XX/` | Automated checks a learner runs at the end of a lab (`pytest` from the lab folder) |

```bash
git clone --depth 1 --branch <lab-tag> https://github.com/poridhioss/ledgerline.git ~/ledgerline
python3 -m venv ~/venv && source ~/venv/bin/activate
pip install -r ~/ledgerline/requirements/ep04/lab-00.txt ~/ledgerline
python -m ledgerline -n 5            # five synthetic payment events
```

The generator is deterministic: the same `--seed` produces the same stream on every machine.

This repository contains no lab solutions.
