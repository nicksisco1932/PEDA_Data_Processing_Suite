# PURPOSE: Unit tests for path sanitization helpers.
# INPUTS: None.
# OUTPUTS: Assertions on sanitize_path_str behavior.
# NOTES: Covers quoted and trimmed Windows paths.
from src.path_utils import sanitize_path_str


def test_sanitize_path_str_double_quotes() -> None:
    assert sanitize_path_str('"B:\\x\\y.zip"') == "B:\\x\\y.zip"


def test_sanitize_path_str_single_quotes() -> None:
    assert sanitize_path_str("'B:\\x\\y.zip'") == "B:\\x\\y.zip"


def test_sanitize_path_str_trimmed() -> None:
    assert sanitize_path_str('  "B:\\x\\y.zip"  ') == "B:\\x\\y.zip"


def test_sanitize_path_str_plain() -> None:
    assert sanitize_path_str("B:\\x\\y.zip") == "B:\\x\\y.zip"
