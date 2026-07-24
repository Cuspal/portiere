"""Tests for the `portiere profile-report` CLI command."""

from click.testing import CliRunner

from portiere.cli import cli


def test_cli_profile_report(tmp_path):
    import polars as pl

    src = tmp_path / "HIS_person.csv"
    pl.DataFrame({"code": ["A", "", "BB"], "sys": ["x", "y", "z"]}).write_csv(src)
    out = tmp_path / "out"

    res = CliRunner().invoke(cli, ["profile-report", str(src), "-o", str(out), "--system", "HIS"])
    assert res.exit_code == 0, res.output
    assert (out / "data_profile.html").exists()
    assert (out / "data_profile.csv").exists()
    assert (out / "data_profile_summary.csv").exists()

    html = (out / "data_profile.html").read_text()
    assert "HIS_person" in html
    assert "HIS" in html


def test_cli_profile_report_missing_source(tmp_path):
    out = tmp_path / "out"
    res = CliRunner().invoke(cli, ["profile-report", str(tmp_path / "nope.csv"), "-o", str(out)])
    assert res.exit_code != 0
    assert "not found" in res.output.lower()


def test_cli_profile_report_bad_format(tmp_path):
    res = CliRunner().invoke(
        cli, ["profile-report", "x.csv", "-o", str(tmp_path), "--format", "xml"]
    )
    assert res.exit_code != 0
