"""Tests for the profile-report exporter (models, builder, renderers)."""

from portiere.quality.profile_report import (
    build_source_profile,
    export_profile_report,
)


def test_build_source_profile():
    profile = {
        "row_count": 10,
        "column_count": 2,
        "columns": [
            {
                "name": "code",
                "n_unique": 3,
                "present_count": 8,
                "min_len": 1,
                "max_len": 2,
                "example": "A",
                "top_values": [{"code": "A", "len": 5}],
            },
            {
                "name": "val",
                "n_unique": 10,
                "present_count": 10,
                "min_len": 0,
                "max_len": 0,
                "example": "42",
                "top_values": [{"val": "42", "count": 1}],
            },
        ],
    }
    sp = build_source_profile(profile, source="HIS_person", system="HIS")
    assert sp.source == "HIS_person"
    assert sp.system == "HIS"
    c = {col.name: col for col in sp.columns}
    assert c["code"].present_pct == 80.0
    assert c["code"].n_missing == 2
    assert c["code"].top_value == "A"
    assert c["code"].top_pct == 62.5  # 5 / 8
    assert c["val"].top_value == "42"
    # avg completeness = (8 + 10) / (10 * 2) * 100 = 90.0
    assert sp.avg_completeness_pct == 90.0


def test_build_source_profile_zero_present():
    profile = {
        "row_count": 4,
        "column_count": 1,
        "columns": [
            {
                "name": "empty",
                "n_unique": 0,
                "present_count": 0,
                "min_len": 0,
                "max_len": 0,
                "example": "",
                "top_values": [],
            }
        ],
    }
    sp = build_source_profile(profile, source="s")
    assert sp.system == ""
    assert sp.columns[0].present_pct == 0.0
    assert sp.columns[0].top_pct == 0.0
    assert sp.columns[0].n_missing == 4


def test_build_source_profile_truncates():
    long_val = "x" * 100
    profile = {
        "row_count": 1,
        "column_count": 1,
        "columns": [
            {
                "name": "c",
                "n_unique": 1,
                "present_count": 1,
                "min_len": 100,
                "max_len": 100,
                "example": long_val,
                "top_values": [{"c": long_val, "len": 1}],
            }
        ],
    }
    sp = build_source_profile(profile, source="s", top_value_maxlen=40, example_maxlen=40)
    assert len(sp.columns[0].top_value) == 40
    assert len(sp.columns[0].example) == 40


def test_export_profile_report(tmp_path):
    profile = {
        "row_count": 2,
        "column_count": 1,
        "columns": [
            {
                "name": "c",
                "n_unique": 2,
                "present_count": 2,
                "min_len": 1,
                "max_len": 3,
                "example": "<x>",
                "top_values": [{"c": "<x>", "len": 1}],
            }
        ],
    }
    sp = build_source_profile(profile, source="src1", system="SYS")
    paths = export_profile_report([sp], tmp_path)
    assert paths["html"].exists()
    assert paths["csv"].exists()
    assert paths["summary"].exists()

    html = paths["html"].read_text()
    assert "src1" in html
    assert "SYS" in html
    assert "&lt;x&gt;" in html  # example is HTML-escaped
    assert "<x>" not in html  # raw value must not leak into markup

    long_rows = paths["csv"].read_text().strip().splitlines()
    assert long_rows[0].startswith("system,source,column")
    assert len(long_rows) == 2  # header + 1 column

    summary = paths["summary"].read_text().strip().splitlines()
    assert len(summary) == 2  # header + 1 source


def test_export_formats_subset(tmp_path):
    profile = {
        "row_count": 1,
        "column_count": 1,
        "columns": [
            {
                "name": "c",
                "n_unique": 1,
                "present_count": 1,
                "min_len": 1,
                "max_len": 1,
                "example": "a",
                "top_values": [{"c": "a", "len": 1}],
            }
        ],
    }
    sp = build_source_profile(profile, source="s")
    paths = export_profile_report([sp], tmp_path, formats=("html",))
    assert set(paths) == {"html"}
    assert not (tmp_path / "data_profile.csv").exists()


def test_export_empty_profiles(tmp_path):
    paths = export_profile_report([], tmp_path)
    assert paths["html"].exists()
    # summary table renders with a header row but no source rows
    summary = paths["summary"].read_text().strip().splitlines()
    assert len(summary) == 1  # header only


def test_public_api_exports():
    from portiere.quality import (
        ColumnProfile,
        SourceProfile,
        build_source_profile,
        export_profile_report,
    )

    assert all(
        x is not None
        for x in (ColumnProfile, SourceProfile, build_source_profile, export_profile_report)
    )


def test_build_source_profile_carries_numeric_stats():
    profile = {
        "row_count": 4,
        "column_count": 1,
        "columns": [
            {
                "name": "age",
                "n_unique": 4,
                "present_count": 4,
                "min_len": 0,
                "max_len": 0,
                "example": "10",
                "top_values": [],
                "num_min": 10.0,
                "num_max": 40.0,
                "num_mean": 25.0,
                "num_std": 12.9,
            }
        ],
    }
    sp = build_source_profile(profile, source="s")
    c = sp.columns[0]
    assert c.num_min == 10.0 and c.num_max == 40.0
    assert c.num_mean == 25.0 and c.num_std == 12.9
