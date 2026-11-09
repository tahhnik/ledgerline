"""Chapter 1: split a raw stream into contract-valid and rejected events."""
import json

from ledgerline.generator import GeneratorConfig, generate

from contract_check import validate_stream


def write_stream(tmp_path, **cfg):
    path = tmp_path / "stream.jsonl"
    with path.open("w") as f:
        for r in generate(GeneratorConfig(**cfg)):
            f.write(json.dumps({"ingested_at": r.ingested_at.isoformat(), "event": r.event}) + "\n")
    return path


def test_clean_stream_has_no_rejects(tmp_path):
    report = validate_stream(write_stream(tmp_path, n_events=300, seed=1))
    assert report.valid == 300
    assert report.rejected == 0


def test_bad_values_are_rejected_with_a_reason(tmp_path):
    report = validate_stream(write_stream(tmp_path, n_events=1000, bad_rate=0.03, seed=9))
    assert report.rejected > 0
    assert report.valid + report.rejected == 1000
    assert "currency" in report.reasons            # the field that failed
    assert sum(report.reasons.values()) == report.rejected
