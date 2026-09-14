#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Structural validation for the PostgreSQL Beam YAML example pipelines in
``sdks/python/apache_beam/yaml/examples/transforms/postgres``.

Parses each specification and asserts that it declares a well-formed
pipeline, reporting the transforms in each stage. This is a static check:
it does not execute the pipelines and needs no PostgreSQL instance.

Covers pure replication, filtering, transformation with PII masking,
a streaming materialized view, dimension-table enrichment, and local
model inference.

Run directly:

    python3 sdks/python/apache_beam/yaml/examples/testing/validate_postgres_examples.py
"""

import sys
import yaml
from pathlib import Path

def validate_yaml_pipeline(file_path: Path):
    print(f"--- Validating Beam YAML: {file_path.name} ---")
    with open(file_path, "r") as f:
        spec = yaml.safe_load(f)

    assert "pipeline" in spec, "Missing 'pipeline' root key"
    pipeline = spec["pipeline"]
    transforms = pipeline.get("transforms", [])
    assert len(transforms) >= 2, f"Expected at least 2 transforms, got {len(transforms)}"

    print(f"Pipeline structure: type={pipeline.get('type', 'composite')}, stages={len(transforms)}")
    for idx, t in enumerate(transforms):
        t_type = t.get("type")
        t_name = t.get("name", f"Transform_{idx}")
        print(f"  Stage {idx + 1}: [{t_type}] '{t_name}'")
        if t_type in ("ReadFromPostgres", "WriteToPostgres"):
            config = t.get("config", {})
            assert "url" in config, f"{t_name} missing 'url'"
            assert "table" in config, f"{t_name} missing 'table'"
            print(f"    -> Table: {config['table']}, URL: {config['url']}")
        elif t_type in ("ReadFromPostgreSQLCDC", "ReadFromPostgresCDC"):
            config = t.get("config", {})
            assert "slot_name" in config, f"{t_name} missing 'slot_name'"
            assert "publication" in config, f"{t_name} missing 'publication'"
            print(f"    -> Slot: {config['slot_name']}, Pub: {config['publication']}")
        elif t_type == "WriteToPostgreSQL":
            config = t.get("config", {})
            assert "table" in config, f"{t_name} missing 'table'"
            print(f"    -> Table: {config['table']}")
        elif t_type == "Sql":
            config = t.get("config", {})
            assert "query" in config, f"{t_name} missing 'query'"
            print(f"    -> Query: {config['query'].strip()[:60]}...")
        elif t_type == "Filter":
            config = t.get("config", {})
            assert "keep" in config, f"{t_name} missing 'keep' predicate"
            print(f"    -> Predicate: {config['keep']}")
        elif t_type == "MapToFields":
            config = t.get("config", {})
            assert "fields" in config, f"{t_name} missing 'fields' mapping"
            print(f"    -> Mapped fields: {list(config['fields'].keys())}")

    print(f"SUCCESS: {file_path.name} is a valid Beam YAML specification!\n")

if __name__ == "__main__":
    # This script lives in yaml/examples/testing/; the specifications it
    # validates are in the sibling directory yaml/examples/transforms/postgres.
    base_dir = Path(__file__).resolve().parent.parent / "transforms" / "postgres"
    yaml_files = [
        base_dir / "postgres_replication.yaml",
        base_dir / "postgres_filtering.yaml",
        base_dir / "postgres_transformation.yaml",
        base_dir / "postgres_streaming_materialized_view.yaml",
        base_dir / "postgres_enrichment.yaml",
        base_dir / "postgres_local_inference.yaml",
    ]
    for yf in yaml_files:
        if not yf.exists():
            print(f"ERROR: File not found: {yf}", file=sys.stderr)
            sys.exit(1)
        validate_yaml_pipeline(yf)
    print("ALL 6 PostgreSQL Beam YAML pipelines validated successfully!")
