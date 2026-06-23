import pytest


@pytest.mark.pre_pipeline
def test_dimension_registry_is_non_empty_and_consumable(metadata):
    registry = metadata.get("dimension_registry")
    assert isinstance(registry, list)
    assert registry

    defaults = metadata.get("dimension_defaults", {})
    required_keys = {
        "business_key",
        "input_pattern",
        "target_table",
        "natural_key_columns",
    }
    for label in registry:
        dim_config = {**defaults, **metadata.get(label, {})}
        missing = required_keys - set(dim_config)
        assert not missing, f"{label} missing required keys: {missing}"
        assert dim_config["natural_key_columns"]


@pytest.mark.pre_pipeline
def test_fact_measure_resolves_to_real_source_column(metadata):
    source_fields = {
        field["name"]
        for field in metadata["ingestion"]["source"]["schema"]["fields"]
    }
    fact_config = metadata["star_schema"]["facts"][0]
    measure_source = fact_config["measures"]["annual_premium_amount"]["source"]
    assert measure_source in source_fields


@pytest.mark.pre_pipeline
def test_validations_reference_real_input_columns(metadata):
    source_fields = {
        field["name"]
        for field in metadata["ingestion"]["source"]["schema"]["fields"]
    }
    validation_fields = {
        validation["field"]
        for validation in metadata["ingestion"].get("validations", [])
    }
    assert validation_fields
    assert validation_fields <= source_fields
