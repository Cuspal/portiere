# Database Connections

Connect Portiere directly to your database to map clinical data without exporting to files. This guide covers connection setup, supported databases, and full pipeline examples.

---

## Table of Contents

- [Quick Start](#quick-start)
- [Supported Databases](#supported-databases)
- [Connection String Format](#connection-string-format)
- [Reading Tables](#reading-tables)
- [Custom SQL Queries](#custom-sql-queries)
- [Multiple Sources from One Database](#multiple-sources-from-one-database)
- [Full Pipeline with Database Source](#full-pipeline-with-database-source)
- [Security Best Practices](#security-best-practices)

---

## Quick Start

```python
import portiere
from portiere.engines import PolarsEngine

project = portiere.init(name="Hospital Migration", engine=PolarsEngine())

# Connect to a PostgreSQL database
source = project.add_source(
    connection_string="postgresql://user:pass@localhost:5432/ehr_db",
    table="patients"
)

# Map schema and concepts as usual
schema_map = project.map_schema(source)
concept_map = project.map_concepts(source=source)
```

---

## Supported Databases

| Database | URI Scheme | Example |
|----------|-----------|---------|
| PostgreSQL | `postgresql://` | `postgresql://user:pass@host:5432/dbname` |
| MySQL | `mysql://` | `mysql://user:pass@host:3306/dbname` |
| SQLite | `sqlite:///` | `sqlite:///path/to/database.db` |
| SQL Server | `mssql://` | `mssql://user:pass@host:1433/dbname` |
| Oracle | `oracle://` | `oracle://user:pass@host:1521/service` |

Any database supported by your engine's connector can be used. The connection string follows standard SQLAlchemy URI format.

---

## Connection String Format

Connection strings use the format:

```
dialect://username:password@host:port/database
```

### Examples

**PostgreSQL:**

```python
# Standard connection
connection_string = "postgresql://admin:secret@db.hospital.internal:5432/ehr_production"

# With SSL
connection_string = "postgresql://admin:secret@db.hospital.internal:5432/ehr_production?sslmode=require"
```

**MySQL:**

```python
connection_string = "mysql://reader:password@mysql-host:3306/clinical_data"
```

**SQLite (local file):**

```python
connection_string = "sqlite:///./data/local_ehr.db"
```

**SQL Server:**

```python
connection_string = "mssql://sa:password@sqlserver-host:1433/EHR_DB?driver=ODBC+Driver+17+for+SQL+Server"
```

---

## Reading Tables

Use the `table` parameter to read an entire database table:

```python
source = project.add_source(
    connection_string="postgresql://user:pass@localhost:5432/ehr_db",
    table="patients"
)
```

The table is read by the compute engine (Polars, Pandas, or Spark) and produces the same `Source` object as file-based sources. Column names, types, and row counts are detected automatically.

---

## Custom SQL Queries

Use the `query` parameter for filtered or joined data:

```python
# Filter to recent admissions
source = project.add_source(
    connection_string="postgresql://user:pass@localhost:5432/ehr_db",
    query="SELECT * FROM patients WHERE admission_date >= '2024-01-01'",
    name="recent_patients"
)

# Join tables before mapping
source = project.add_source(
    connection_string="postgresql://user:pass@localhost:5432/ehr_db",
    query="""
        SELECT p.patient_id, p.gender, p.birth_date,
               d.diagnosis_code, d.diagnosis_description,
               m.medication_code, m.medication_name
        FROM patients p
        JOIN diagnoses d ON p.patient_id = d.patient_id
        JOIN medications m ON p.patient_id = m.patient_id
    """,
    name="patient_clinical_data"
)
```

When using `query`, the `name` parameter is required to identify the source in the project.

---

## Multiple Sources from One Database

You can add multiple sources from the same database to map different clinical domains:

```python
conn = "postgresql://user:pass@localhost:5432/ehr_db"

# Source 1: Patient demographics
patients = project.add_source(connection_string=conn, table="patients")

# Source 2: Lab results
labs = project.add_source(
    connection_string=conn,
    query="SELECT * FROM lab_results WHERE result_date >= '2024-01-01'",
    name="lab_results"
)

# Source 3: Medications
meds = project.add_source(connection_string=conn, table="prescriptions")

# Map each source independently
for source in [patients, labs, meds]:
    schema_map = project.map_schema(source)
    concept_map = project.map_concepts(source=source)
```

---

## Full Pipeline with Database Source

A complete end-to-end example connecting to a hospital EHR database:

```python
import portiere
from portiere.config import PortiereConfig, LLMConfig
from portiere.engines import PolarsEngine

# Configure with LLM for concept verification
config = PortiereConfig(
    llm=LLMConfig(
        provider="openai",
        api_key="sk-...",
        model="gpt-4o",
    )
)

project = portiere.init(
    name="Hospital OMOP Migration",
    engine=PolarsEngine(),
    vocabularies=["SNOMED", "LOINC", "RxNorm", "ICD10CM"],
    config=config,
)

# Ingest from database
source = project.add_source(
    connection_string="postgresql://etl_user:password@ehr-db:5432/hospital_ehr",
    query="""
        SELECT patient_id, gender, date_of_birth,
               diagnosis_code, diagnosis_description,
               lab_code, lab_value, lab_unit
        FROM clinical_encounters
        WHERE encounter_date >= '2024-01-01'
    """,
    name="clinical_encounters"
)

# Profile
profile = project.profile(source)
print(f"Columns: {len(profile.get('columns', []))}")

# Schema mapping
schema_map = project.map_schema(source)
for item in schema_map.items:
    if item.status.value == "needs_review":
        item.approve()

# Concept mapping
concept_map = project.map_concepts(source=source)
summary = concept_map.summary()
print(f"Auto-mapped: {summary['auto_mapped']}/{summary['total']}")

# ETL
etl = project.run_etl(
    source,
    output_dir="./omop_output",
    schema_mapping=schema_map,
    concept_mapping=concept_map,
)

# Validate
result = project.validate(etl_result=etl)
print(f"Validation: {'PASSED' if result['all_passed'] else 'FAILED'}")
```

---

## Security Best Practices

### Use Environment Variables for Credentials

Never hardcode database credentials. Use environment variables:

```python
import os

connection_string = (
    f"postgresql://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}"
    f"@{os.environ['DB_HOST']}:{os.environ.get('DB_PORT', '5432')}"
    f"/{os.environ['DB_NAME']}"
)

source = project.add_source(connection_string=connection_string, table="patients")
```

Or use a `.env` file with `python-dotenv`:

```python
from dotenv import load_dotenv
load_dotenv()

connection_string = os.environ["DATABASE_URL"]
```

### Use Read-Only Database Users

Create a dedicated read-only user for ETL extraction:

```sql
-- PostgreSQL
CREATE USER etl_reader WITH PASSWORD 'secure_password';
GRANT CONNECT ON DATABASE ehr_db TO etl_reader;
GRANT USAGE ON SCHEMA public TO etl_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO etl_reader;
```

### Filter Data at Query Time

Use SQL queries to extract only the data you need, minimizing data exposure:

```python
# Only extract the columns and rows needed for mapping
source = project.add_source(
    connection_string=connection_string,
    query="SELECT patient_id, diagnosis_code FROM encounters WHERE year = 2024",
    name="encounters_2024"
)
```

---

## See Also

- [01-quickstart.md](./01-quickstart.md) -- Getting started with file and database sources
- [08-pipeline-architecture.md](./08-pipeline-architecture.md) -- Full pipeline architecture with database ingest
- [03-configuration.md](./03-configuration.md) -- Engine and knowledge layer configuration
