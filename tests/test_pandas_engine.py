"""
Tests for PandasEngine.

This module tests the Pandas-based compute engine including:
- Reading data from various formats (CSV, Parquet, JSON)
- Data profiling (schema, statistics, null analysis)
- Data transformation (renames, casts, projections)
- Writing data to various formats

Run with: python3 -m pytest tests/test_pandas_engine.py -v -s -o "addopts="
The -s flag enables output display for each stage.
"""

import json
from pathlib import Path

import pandas as pd
import pytest
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from portiere.engines.pandas_engine import PandasEngine

# Initialize rich console for pretty output
console = Console()

# Check if pyarrow is available for parquet tests
try:
    import pyarrow  # noqa: F401

    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False

requires_pyarrow = pytest.mark.skipif(
    not HAS_PYARROW, reason="pyarrow is required for parquet support"
)


def print_stage(stage_name: str, description: str = ""):
    """Print a stage header with optional description."""
    console.print()
    console.print(
        Panel(
            f"[bold cyan]{stage_name}[/bold cyan]\n{description}",
            title="🔬 Stage",
            border_style="blue",
        )
    )


def print_dataframe(df: pd.DataFrame, title: str = "DataFrame"):
    """Print a DataFrame as a rich table."""
    table = Table(title=title, show_header=True, header_style="bold magenta")

    for col in df.columns:
        table.add_column(str(col))

    for _, row in df.head(5).iterrows():
        table.add_row(*[str(v) for v in row.values])

    if len(df) > 5:
        table.add_row(*["..." for _ in df.columns])

    console.print(table)
    console.print(f"[dim]Total rows: {len(df)}[/dim]")


def print_json(data: dict, title: str = "Output"):
    """Print JSON data with syntax highlighting."""
    console.print(f"\n[bold green]{title}:[/bold green]")
    console.print_json(json.dumps(data, indent=2, default=str))


class TestPandasEngineInit:
    """Tests for PandasEngine initialization."""

    def test_init_success(self):
        """Test engine initializes successfully."""
        print_stage("Engine Initialization", "Creating PandasEngine instance")

        engine = PandasEngine()

        console.print(f"[green]✓[/green] Engine created: [bold]{engine.engine_name}[/bold]")
        assert engine is not None
        assert engine.engine_name == "pandas"

    def test_engine_name(self):
        """Test engine_name property returns 'pandas'."""
        engine = PandasEngine()
        assert engine.engine_name == "pandas"


class TestReadSource:
    """Tests for reading source data."""

    @pytest.fixture
    def sample_csv_path(self, tmp_path: Path) -> Path:
        """Create a sample CSV file for testing."""
        csv_path = tmp_path / "sample.csv"
        df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3, 4, 5],
                "drug_code": ["A001", "B002", "A001", "C003", "B002"],
                "drug_name": ["Paracetamol", "Ibuprofen", "Paracetamol", "Aspirin", "Ibuprofen"],
                "dosage": [500, 400, 500, 300, 400],
                "date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"],
            }
        )
        df.to_csv(csv_path, index=False)
        return csv_path

    @pytest.fixture
    def sample_parquet_path(self, tmp_path: Path) -> Path:
        """Create a sample Parquet file for testing."""
        parquet_path = tmp_path / "sample.parquet"
        df = pd.DataFrame(
            {
                "concept_id": [100, 200, 300],
                "concept_name": ["Drug A", "Drug B", "Drug C"],
                "vocabulary_id": ["RxNorm", "RxNorm", "SNOMED"],
            }
        )
        df.to_parquet(parquet_path, index=False)
        return parquet_path

    @pytest.fixture
    def sample_json_path(self, tmp_path: Path) -> Path:
        """Create a sample JSON file for testing."""
        json_path = tmp_path / "sample.json"
        df = pd.DataFrame(
            {
                "id": [1, 2],
                "name": ["Test A", "Test B"],
            }
        )
        df.to_json(json_path, orient="records")
        return json_path

    def test_read_csv(self, sample_csv_path: Path):
        """Test reading CSV files."""
        print_stage("Read Source (CSV)", f"Loading data from: {sample_csv_path.name}")

        engine = PandasEngine()
        df = engine.read_source(str(sample_csv_path), format="csv")

        print_dataframe(df, "📄 Loaded CSV Data")

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 5
        assert list(df.columns) == ["patient_id", "drug_code", "drug_name", "dosage", "date"]

    @requires_pyarrow
    def test_read_parquet(self, sample_parquet_path: Path):
        """Test reading Parquet files."""
        print_stage("Read Source (Parquet)", f"Loading data from: {sample_parquet_path.name}")

        engine = PandasEngine()
        df = engine.read_source(str(sample_parquet_path), format="parquet")

        print_dataframe(df, "📦 Loaded Parquet Data")

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert "concept_id" in df.columns

    def test_read_json(self, sample_json_path: Path):
        """Test reading JSON files."""
        print_stage("Read Source (JSON)", f"Loading data from: {sample_json_path.name}")

        engine = PandasEngine()
        df = engine.read_source(str(sample_json_path), format="json")

        print_dataframe(df, "📋 Loaded JSON Data")

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2

    def test_read_unsupported_format(self, tmp_path: Path):
        """Test that unsupported format raises ValueError."""
        engine = PandasEngine()
        with pytest.raises(ValueError, match="Unsupported format"):
            engine.read_source(str(tmp_path / "dummy.xyz"), format="xyz")

    def test_read_with_options(self, tmp_path: Path):
        """Test reading with custom options."""
        csv_path = tmp_path / "custom.csv"
        with open(csv_path, "w") as f:
            f.write("col1;col2;col3\n1;a;x\n2;b;y\n")

        engine = PandasEngine()
        df = engine.read_source(str(csv_path), format="csv", options={"sep": ";"})

        assert len(df) == 2
        assert list(df.columns) == ["col1", "col2", "col3"]


class TestProfile:
    """Tests for data profiling functionality."""

    @pytest.fixture
    def sample_df(self) -> pd.DataFrame:
        """Create a sample DataFrame for profiling tests."""
        return pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "category": ["A", "B", "A", "C", "A"],
                "value": [10.5, 20.0, None, 40.5, 50.0],
                "flag": [True, False, True, True, False],
            }
        )

    def test_profile_basic(self, sample_df: pd.DataFrame):
        """Test basic profiling returns expected structure."""
        print_stage("Data Profiling", "Analyzing DataFrame structure and statistics")

        engine = PandasEngine()

        console.print("\n[bold]Input DataFrame:[/bold]")
        print_dataframe(sample_df, "📊 Source Data")

        profile = engine.profile(sample_df)

        print_json(profile, "📈 Profile Result")

        assert "row_count" in profile
        assert "column_count" in profile
        assert "columns" in profile
        assert profile["row_count"] == 5
        assert profile["column_count"] == 4

    def test_profile_column_stats(self, sample_df: pd.DataFrame):
        """Test that column statistics are correctly calculated."""
        engine = PandasEngine()
        profile = engine.profile(sample_df)

        columns = {col["name"]: col for col in profile["columns"]}

        # Check category column
        assert columns["category"]["n_unique"] == 3
        assert "top_values" in columns["category"]  # Low cardinality should have top values

        # Check value column (has null)
        assert columns["value"]["null_count"] == 1
        assert columns["value"]["null_pct"] == 20.0

    def test_profile_nullable_detection(self, sample_df: pd.DataFrame):
        """Test that nullable columns are correctly identified."""
        engine = PandasEngine()
        profile = engine.profile(sample_df)

        columns = {col["name"]: col for col in profile["columns"]}

        assert not columns["id"]["nullable"]
        assert columns["value"]["nullable"]


class TestGetDistinctValues:
    """Tests for getting distinct values with counts."""

    @pytest.fixture
    def sample_df(self) -> pd.DataFrame:
        """Create a sample DataFrame."""
        return pd.DataFrame(
            {
                "drug_code": ["A001", "B002", "A001", "A001", "C003", "B002"],
                "status": ["active", "inactive", "active", "active", "active", "inactive"],
            }
        )

    def test_get_distinct_values_basic(self, sample_df: pd.DataFrame):
        """Test getting distinct values with counts."""
        print_stage("Get Distinct Values", "Extracting unique values with frequency counts")

        engine = PandasEngine()

        console.print("\n[bold]Input DataFrame:[/bold]")
        print_dataframe(sample_df, "📊 Source Data")

        result = engine.get_distinct_values(sample_df, "drug_code")

        # Display as table
        table = Table(title="🔢 Distinct Values for 'drug_code'", show_header=True)
        table.add_column("Value", style="cyan")
        table.add_column("Count", style="green")
        for item in result:
            table.add_row(str(item["value"]), str(item["count"]))
        console.print(table)

        assert isinstance(result, list)
        assert len(result) == 3  # A001, B002, C003

        # Results should be sorted by count descending
        assert result[0]["value"] == "A001"
        assert result[0]["count"] == 3

    def test_get_distinct_values_with_limit(self, sample_df: pd.DataFrame):
        """Test that limit parameter works correctly."""
        engine = PandasEngine()
        result = engine.get_distinct_values(sample_df, "drug_code", limit=2)

        assert len(result) == 2

    def test_get_distinct_values_returns_dict_structure(self, sample_df: pd.DataFrame):
        """Test that returned items have correct structure."""
        engine = PandasEngine()
        result = engine.get_distinct_values(sample_df, "status")

        for item in result:
            assert "value" in item
            assert "count" in item
            assert isinstance(item["count"], int)


class TestTransform:
    """Tests for data transformation functionality."""

    @pytest.fixture
    def sample_df(self) -> pd.DataFrame:
        """Create a sample DataFrame."""
        return pd.DataFrame(
            {
                "old_id": [1, 2, 3],
                "old_name": ["Alice", "Bob", "Charlie"],
                "old_value": ["10", "20", "30"],
                "extra_col": ["x", "y", "z"],
            }
        )

    def test_transform_rename(self, sample_df: pd.DataFrame):
        """Test column renaming."""
        print_stage("Transform - Rename", "Renaming columns based on mapping specification")

        engine = PandasEngine()
        mapping_spec = {
            "renames": {
                "old_id": "new_id",
                "old_name": "new_name",
            }
        }

        console.print("\n[bold]Before Transform:[/bold]")
        print_dataframe(sample_df, "📊 Original Data")

        console.print("\n[bold]Mapping Spec:[/bold]")
        print_json(mapping_spec, "🔧 Configuration")

        result = engine.transform(sample_df, mapping_spec)

        console.print("\n[bold]After Transform:[/bold]")
        print_dataframe(result, "✨ Transformed Data")

        assert "new_id" in result.columns
        assert "new_name" in result.columns
        assert "old_id" not in result.columns
        assert "old_name" not in result.columns

    def test_transform_cast(self, sample_df: pd.DataFrame):
        """Test type casting."""
        print_stage("Transform - Type Cast", "Converting column types")

        engine = PandasEngine()
        mapping_spec = {
            "casts": {
                "old_value": "int64",
            }
        }

        console.print(f"\n[bold]Before:[/bold] old_value dtype = {sample_df['old_value'].dtype}")

        result = engine.transform(sample_df, mapping_spec)

        console.print(f"[bold]After:[/bold] old_value dtype = {result['old_value'].dtype}")
        console.print(f"[bold]Values:[/bold] {result['old_value'].tolist()}")

        assert result["old_value"].dtype == "int64"
        assert result["old_value"].tolist() == [10, 20, 30]

    def test_transform_select(self, sample_df: pd.DataFrame):
        """Test column selection/projection."""
        print_stage("Transform - Select", "Projecting specific columns")

        engine = PandasEngine()
        mapping_spec = {
            "select": ["old_id", "old_name"],
        }

        console.print(f"\n[bold]Original columns:[/bold] {list(sample_df.columns)}")

        result = engine.transform(sample_df, mapping_spec)

        console.print(f"[bold]Selected columns:[/bold] {list(result.columns)}")
        print_dataframe(result, "✨ Projected Data")

        assert list(result.columns) == ["old_id", "old_name"]
        assert len(result.columns) == 2

    def test_transform_combined(self, sample_df: pd.DataFrame):
        """Test combined transformations (rename + cast + select)."""
        print_stage("Transform - Combined", "Applying multiple transformations in sequence")

        engine = PandasEngine()
        mapping_spec = {
            "renames": {"old_id": "id", "old_name": "name", "old_value": "value"},
            "casts": {"value": "int64"},
            "select": ["id", "name", "value"],
        }

        console.print("\n[bold]Pipeline:[/bold]")
        console.print("  1️⃣ Rename: old_id → id, old_name → name, old_value → value")
        console.print("  2️⃣ Cast: value → int64")
        console.print("  3️⃣ Select: id, name, value")

        result = engine.transform(sample_df, mapping_spec)

        print_dataframe(result, "✨ Final Transformed Data")

        assert list(result.columns) == ["id", "name", "value"]
        assert result["value"].dtype == "int64"

    def test_transform_preserves_original(self, sample_df: pd.DataFrame):
        """Test that transform does not modify original DataFrame."""
        engine = PandasEngine()
        original_columns = list(sample_df.columns)

        mapping_spec = {
            "renames": {"old_id": "new_id"},
        }

        _ = engine.transform(sample_df, mapping_spec)

        # Original should be unchanged
        assert list(sample_df.columns) == original_columns


class TestWrite:
    """Tests for writing data to files."""

    @pytest.fixture
    def sample_df(self) -> pd.DataFrame:
        """Create a sample DataFrame."""
        return pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["A", "B", "C"],
            }
        )

    @requires_pyarrow
    def test_write_parquet(self, sample_df: pd.DataFrame, tmp_path: Path):
        """Test writing to Parquet format."""
        print_stage("Write Output (Parquet)", "Saving DataFrame to Parquet format")

        engine = PandasEngine()
        output_path = tmp_path / "output.parquet"

        engine.write(sample_df, str(output_path), format="parquet")

        console.print(f"[green]✓[/green] Written to: {output_path}")
        console.print(f"[dim]File size: {output_path.stat().st_size} bytes[/dim]")

        assert output_path.exists()

        # Verify content
        loaded = pd.read_parquet(output_path)
        assert len(loaded) == 3
        assert list(loaded.columns) == ["id", "name"]

    def test_write_csv(self, sample_df: pd.DataFrame, tmp_path: Path):
        """Test writing to CSV format."""
        print_stage("Write Output (CSV)", "Saving DataFrame to CSV format")

        engine = PandasEngine()
        output_path = tmp_path / "output.csv"

        engine.write(sample_df, str(output_path), format="csv")

        console.print(f"[green]✓[/green] Written to: {output_path}")

        # Show file contents
        with open(output_path) as f:
            content = f.read()
        console.print(f"\n[bold]File Contents:[/bold]\n{content}")

        assert output_path.exists()

        # Verify content
        loaded = pd.read_csv(output_path)
        assert len(loaded) == 3

    def test_write_json(self, sample_df: pd.DataFrame, tmp_path: Path):
        """Test writing to JSON format."""
        print_stage("Write Output (JSON)", "Saving DataFrame to JSON format")

        engine = PandasEngine()
        output_path = tmp_path / "output.json"

        engine.write(sample_df, str(output_path), format="json")

        console.print(f"[green]✓[/green] Written to: {output_path}")

        # Show file contents
        with open(output_path) as f:
            content = f.read()
        console.print(f"\n[bold]File Contents:[/bold]\n{content}")

        assert output_path.exists()

    def test_write_creates_parent_dirs(self, sample_df: pd.DataFrame, tmp_path: Path):
        """Test that write creates parent directories if they don't exist."""
        engine = PandasEngine()
        output_path = tmp_path / "nested" / "dir" / "output.csv"

        engine.write(sample_df, str(output_path), format="csv")

        assert output_path.exists()

    def test_write_unsupported_format(self, sample_df: pd.DataFrame, tmp_path: Path):
        """Test that unsupported format raises ValueError."""
        engine = PandasEngine()
        with pytest.raises(ValueError, match="Unsupported format"):
            engine.write(sample_df, str(tmp_path / "output.xyz"), format="xyz")


class TestUtilityMethods:
    """Tests for utility methods like count, schema, to_pandas."""

    @pytest.fixture
    def sample_df(self) -> pd.DataFrame:
        """Create a sample DataFrame."""
        return pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "name": ["A", "B", "C", "D", "E"],
                "value": [1.1, None, 3.3, 4.4, 5.5],
            }
        )

    def test_count(self, sample_df: pd.DataFrame):
        """Test row counting."""
        engine = PandasEngine()
        count = engine.count(sample_df)

        assert count == 5

    def test_schema(self, sample_df: pd.DataFrame):
        """Test schema extraction."""
        print_stage("Schema Extraction", "Getting DataFrame schema information")

        engine = PandasEngine()
        schema = engine.schema(sample_df)

        # Display as table
        table = Table(title="📋 Schema", show_header=True)
        table.add_column("Column", style="cyan")
        table.add_column("Type", style="green")
        table.add_column("Nullable", style="yellow")
        for col in schema:
            table.add_row(col["name"], col["type"], str(col["nullable"]))
        console.print(table)

        assert isinstance(schema, list)
        assert len(schema) == 3

        schema_dict = {col["name"]: col for col in schema}

        assert "id" in schema_dict
        assert "name" in schema_dict
        assert "value" in schema_dict

        # Check schema structure
        for col in schema:
            assert "name" in col
            assert "type" in col
            assert "nullable" in col

    def test_to_pandas(self, sample_df: pd.DataFrame):
        """Test to_pandas returns the same DataFrame."""
        engine = PandasEngine()
        result = engine.to_pandas(sample_df)

        assert result is sample_df  # Should return same object for PandasEngine

    def test_to_dict_records(self, sample_df: pd.DataFrame):
        """Test conversion to list of dicts."""
        print_stage("To Dict Records", "Converting DataFrame to list of dictionaries")

        engine = PandasEngine()
        records = engine.to_dict_records(sample_df, limit=3)

        print_json({"records": records}, "📤 Output Records")

        assert isinstance(records, list)
        assert len(records) == 3
        assert all(isinstance(r, dict) for r in records)
        assert records[0]["id"] == 1
        assert records[0]["name"] == "A"

    def test_sql_not_supported(self, sample_df: pd.DataFrame):
        """Test that SQL raises NotImplementedError for PandasEngine."""
        engine = PandasEngine()
        with pytest.raises(NotImplementedError):
            engine.sql("SELECT * FROM table")


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_empty_dataframe(self):
        """Test operations on empty DataFrame."""
        engine = PandasEngine()
        empty_df = pd.DataFrame(columns=["id", "name"])

        # Profile should work
        profile = engine.profile(empty_df)
        assert profile["row_count"] == 0
        assert profile["column_count"] == 2

        # Count should return 0
        assert engine.count(empty_df) == 0

        # Schema should still return columns
        schema = engine.schema(empty_df)
        assert len(schema) == 2

    def test_single_row_dataframe(self):
        """Test operations on single-row DataFrame."""
        engine = PandasEngine()
        single_df = pd.DataFrame({"id": [1], "name": ["Test"]})

        profile = engine.profile(single_df)
        assert profile["row_count"] == 1

        distinct = engine.get_distinct_values(single_df, "id")
        assert len(distinct) == 1
        assert distinct[0]["count"] == 1

    def test_dataframe_with_all_nulls(self):
        """Test profiling column with all null values."""
        engine = PandasEngine()
        null_df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "all_null": [None, None, None],
            }
        )

        profile = engine.profile(null_df)
        columns = {col["name"]: col for col in profile["columns"]}

        assert columns["all_null"]["null_count"] == 3
        assert columns["all_null"]["null_pct"] == 100.0
