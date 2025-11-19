from pathlib import Path


def test_readme_mentions_project_name():
    """Sanity check so pytest always discovers at least one test."""
    root = Path(__file__).resolve().parents[1]
    readme = root / "README.md"
    assert readme.exists(), "README.md is missing"
    contents = readme.read_text(encoding="utf-8")
    assert "tfl-realtime-lakehouse" in contents
