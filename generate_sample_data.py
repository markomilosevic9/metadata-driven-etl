import json
import logging
import os
import random
import string
import tempfile
from typing import Dict, Tuple, List, Set

from faker import Faker

from pipeline.core.config_loader import (
    create_minio_client,
    create_spark_session,
    load_config,
    load_metadata,
)
from pipeline.stages.field_validator import split_valid_invalid
from pipeline.stages.schema_enforcer import build_spark_schema


logger = logging.getLogger(__name__)


# script generates deterministic sample input and uploads it to MinIO
# the current demo size is 100k records split across two batches
# faker provides names and birth years, repository helpers generate source-system keys, vehicle attributes, and intentionally invalid values

fake = Faker(["en_GB", "fr_FR", "de_DE", "es_ES", "it_IT", "nl_NL"])
Faker.seed(42)
USED_LICENSE_NUMBERS: Set[str] = set()


DATA_GENERATION_CONFIG = {
    "total_records": 100000,
    "input_bucket": "input-data",
    "seed": 42,
    "num_batches": 2,
    "batch_dates": ["2025-01-15", "2025-02-15"],
    "batch_prefix": "batch-",
    "date_format": "%Y-%m-%d",
    "deterministic_scd2_overlap_count": 6000,
    "deterministic_repeat_overlap_count": 3000,
    "batch2_duplicate_count": 3000,
    "validation_failure_rate": 0.05,
    "vehicle_types": ["sedan", "suv", "truck"],
    "coverage_types": ["basic", "standard", "premium"],
    "status_types": ["active", "suspended", "cancelled"],
    "cities": ["London", "Paris", "Berlin", "Madrid", "Rome", "Amsterdam"],
    "city_country_map": {
        "London": "GB",
        "Paris": "FR",
        "Berlin": "DE",
        "Madrid": "ES",
        "Rome": "IT",
        "Amsterdam": "NL",
    },
    "premium_ranges": {
        "basic": [50, 150],
        "standard": [150, 300],
        "premium": [300, 500],
    },
    "vehicle_makes": {
        "sedan": [
            {"make": "Toyota", "models": ["Corolla", "Camry"]},
            {"make": "Honda", "models": ["Civic", "Accord"]},
            {"make": "Volkswagen", "models": ["Golf", "Passat"]},
        ],
        "suv": [
            {"make": "BMW", "models": ["X3", "X5"]},
            {"make": "Ford", "models": ["Explorer", "Edge"]},
            {"make": "Audi", "models": ["Q3", "Q5"]},
        ],
        "truck": [
            {"make": "Ford", "models": ["F-150", "Ranger"]},
            {"make": "RAM", "models": ["1500", "2500"]},
            {"make": "Chevrolet", "models": ["Silverado", "Colorado"]},
        ],
    },
    "coverage_deductibles": {
        "basic": 1000,
        "standard": 500,
        "premium": 250,
    },
    "email_domains": ["example.com", "mail.net", "post.org"],
}


def generate_license_number() -> str:
    # driver licence generation
    while True:
        license_number = f"LIC-{random.randint(100000, 999999)}"
        if license_number not in USED_LICENSE_NUMBERS:
            USED_LICENSE_NUMBERS.add(license_number)
            return license_number


def generate_city(data_gen_config: dict, intentional_failure: bool = False) -> str:
    if intentional_failure:
        return "" if random.random() < 0.5 else None
    cities = data_gen_config.get("cities", ["London", "Paris", "Berlin", "Madrid", "Rome", "Amsterdam"])
    return random.choice(cities)


def country_from_city(city: str, data_gen_config: dict) -> str:
    # deterministic city-to-country mapping
    city_country_map = data_gen_config.get("city_country_map", {
        "London": "GB", "Paris": "FR", "Berlin": "DE",
        "Madrid": "ES", "Rome":  "IT", "Amsterdam": "NL",
    })
    return city_country_map.get(city, "GB")


def generate_plate_number() -> str:
    # keep AAA-123 format, which is in line with the ^[A-Z0-9-]+$ validation regex
    letters = "".join(random.choices(string.ascii_uppercase, k=3))
    digits  = "".join(random.choices(string.digits, k=3))
    return f"{letters}-{digits}"


def generate_vehicle_type(data_gen_config: dict, intentional_failure: bool = False) -> str:
    if intentional_failure:
        return random.choice(["van", "motorcycle", "bicycle", "boat"])
    vehicle_types = data_gen_config.get("vehicle_types", ["sedan", "suv", "truck"])
    return random.choice(vehicle_types)


def generate_vehicle_year(intentional_failure: bool = False) -> int:
    if intentional_failure:
        return random.choice([random.randint(1950, 1989), random.randint(2027, 2030)])
    return random.randint(1990, 2026)


def generate_vehicle_make_model(
    vehicle_type: str, data_gen_config: dict
) -> Tuple[str, str]:
    # return a (make, model) pair correlated with vehicle_type
    # reads from DATA_GENERATION_CONFIG["vehicle_makes"]
    vehicle_makes = data_gen_config.get("vehicle_makes", {})
    options = vehicle_makes.get(
        vehicle_type, [{"make": "Toyota", "models": ["Corolla"]}]
    )
    chosen = random.choice(options)
    make   = chosen["make"]
    model  = random.choice(chosen["models"])
    return make, model


def generate_policy_number(index: int) -> str:
    return str(10000 + index).zfill(5)


def generate_phone_number(intentional_failure: bool = False) -> str:
    if intentional_failure:
        # keep malformed examples for the invalid path.
        invalid_formats = [
            f"+44{random.randint(1000000, 9999999)}",
            f"44{random.randint(1000000000, 9999999999)}",
            f"+{random.randint(100, 999)}",
        ]
        return random.choice(invalid_formats)
    # faker for valid path: +{2-digit country code}{10 digits}
    country_code = random.choice(["44", "33", "49", "34", "39", "31"])
    return f"+{country_code}{fake.numerify('##########')}"


def generate_email(
    name: str,
    surname: str,
    data_gen_config: dict,
    intentional_failure: bool = False,
) -> str:
    if intentional_failure:
        return random.choice([
            "noatsign",
            f"{name.lower()}@",
            "@nodomain.com",
            "spaces in@email.com",
        ])
    domains = data_gen_config.get("email_domains", ["example.com", "mail.net", "post.org"])
    domain  = random.choice(domains)
    return f"{name.lower()}.{surname.lower()}@{domain}"


def generate_coverage_type(data_gen_config: dict, intentional_failure: bool = False) -> str:
    if intentional_failure:
        return random.choice(["premum", "standrd", "bsic", "gold", "platinum"])
    coverage_types = data_gen_config.get("coverage_types", ["basic", "standard", "premium"])
    return random.choice(coverage_types)


def generate_premium_amount(
    coverage_type: str, data_gen_config: dict, intentional_failure: bool = False
) -> float:
    if intentional_failure:
        return random.choice([None, -100.0, 5500.0, 0.0])
    premium_ranges = data_gen_config.get("premium_ranges", {
        "basic": [50, 150], "standard": [150, 300], "premium": [300, 500],
    })
    lo, hi = premium_ranges.get(coverage_type, [50, 500])
    return round(random.uniform(lo, hi), 2)


def generate_status(data_gen_config: dict, intentional_failure: bool = False) -> str:
    if intentional_failure:
        return random.choice(["pending", "expired", "void", "draft"])
    status_types = data_gen_config.get("status_types", ["active", "suspended", "cancelled"])
    return random.choice(status_types)


def build_valid_new_policy_record(
    policy_num: str, identity: Dict, batch_date: str, data_gen_config: dict
) -> Dict:
    # build a complete new-policy record
    # stable attributes come from identity; SCD1/SCD2 attributes are generated fresh
    coverage_type = generate_coverage_type(data_gen_config)
    name          = identity["name"]
    surname       = identity["surname"]
    city          = identity["city"]
    return {
        "policy_number":  policy_num,
        "license_number": identity["license_number"],
        "birth_year":     identity["birth_year"],
        "name":           name,
        "surname":        surname,
        "city":           city,
        "country":        country_from_city(city, data_gen_config),
        "phone_number":   generate_phone_number(),
        "email":          generate_email(name, surname, data_gen_config),
        "plate_number":   identity["plate_number"],
        "vehicle_type":   identity["vehicle_type"],
        "vehicle_year":   identity["vehicle_year"],
        "vehicle_make":   identity["vehicle_make"],
        "vehicle_model":  identity["vehicle_model"],
        "coverage_type":  coverage_type,
        "premium_amount": generate_premium_amount(coverage_type, data_gen_config),
        "status":         generate_status(data_gen_config),
    }


def build_valid_update_record(
    base_record: Dict, batch_date: str, data_gen_config: dict, force_scd2_change: bool = False
) -> Dict:
    # build an update record for a policy that reappears in a later batch
    # stable attributes (vehicle identity, driver identity) are preserved
    record = base_record.copy()

    record["phone_number"] = generate_phone_number()
    record["email"]        = generate_email(record["name"], record["surname"], data_gen_config)

    if random.random() < 0.20:
        new_city          = generate_city(data_gen_config)
        record["city"]    = new_city
        record["country"] = country_from_city(new_city, data_gen_config)

    if force_scd2_change:
        status_options = [
            s for s in data_gen_config.get("status_types", ["active", "suspended", "cancelled"])
            if s != record["status"]
        ]
        record["status"] = random.choice(status_options) if status_options else record["status"]

        coverage_options = [
            c for c in data_gen_config.get("coverage_types", ["basic", "standard", "premium"])
            if c != record["coverage_type"]
        ]
        record["coverage_type"] = (
            random.choice(coverage_options) if coverage_options else record["coverage_type"]
        )
        record["premium_amount"] = generate_premium_amount(record["coverage_type"], data_gen_config)

    return record


def create_policy_registry(
    total_policies: int, start_index: int, data_gen_config: dict
) -> Dict[str, Dict]:
    # stores stable identity and vehicle attributes so later batches can reuse them without regeneration, preserving cross-batch consistency
    # faker generates name, surname, birth_year
    registry = {}
    for i in range(total_policies):
        policy_num   = generate_policy_number(start_index + i)
        city         = generate_city(data_gen_config)
        vehicle_type = generate_vehicle_type(data_gen_config)
        vehicle_make, vehicle_model = generate_vehicle_make_model(vehicle_type, data_gen_config)
        registry[policy_num] = {
            "name":           fake.first_name(),
            "surname":        fake.last_name(),
            "city":           city,
            "license_number": generate_license_number(),
            "birth_year":     fake.date_of_birth(minimum_age=18, maximum_age=85).year,
            "plate_number":   generate_plate_number(),
            "vehicle_type":   vehicle_type,
            "vehicle_year":   generate_vehicle_year(),
            "vehicle_make":   vehicle_make,
            "vehicle_model":  vehicle_model,
        }
    return registry


def generate_batch_1_new_policies(
    total_records: int,
    start_index: int,
    batch_date: str,
    data_gen_config: dict,
    config: dict,
    metadata: dict,
) -> Tuple[str, Dict, List[Dict], Dict[str, Dict]]:
    # generate batch 1: new policies
    # records are streamed to a NamedTemporaryFile line by line, caller is responsible for deleting the temp file after upload
    failure_rate = data_gen_config.get("validation_failure_rate", 0.05)
    failure_injectors = [
        {
            "stat_key": "invalid_license",
            "apply": lambda record, identity: {
                "license_number": random.choice(["INVALID", "123", "drv-abc"])
            },
        },
        {
            "stat_key": "invalid_birth_year",
            "apply": lambda record, identity: {
                "birth_year": random.choice([1920, 1930, 2010, 2015])
            },
        },
        {
            "stat_key": "invalid_name_surname",
            "apply": lambda record, identity: {
                "name": "" if random.random() < 0.5 else None,
                "surname": "" if random.random() < 0.5 else None,
            },
        },
        {
            "stat_key": "invalid_city",
            "apply": lambda record, identity: {
                "city": generate_city(data_gen_config, intentional_failure=True),
                "country": "XX",
            },
        },
        {
            "stat_key": "invalid_phone",
            "apply": lambda record, identity: {
                "phone_number": generate_phone_number(intentional_failure=True)
            },
        },
        {
            "stat_key": "invalid_email",
            "apply": lambda record, identity: {
                "email": generate_email(
                    record.get("name") or identity["name"],
                    record.get("surname") or identity["surname"],
                    data_gen_config,
                    intentional_failure=True,
                )
            },
        },
        {
            "stat_key": "empty_plate",
            "apply": lambda record, identity: {
                "plate_number": ""
            },
        },
        {
            "stat_key": "invalid_vehicle_type",
            "apply": lambda record, identity: {
                "vehicle_type": generate_vehicle_type(
                    data_gen_config,
                    intentional_failure=True,
                )
            },
        },
        {
            "stat_key": "invalid_vehicle_year",
            "apply": lambda record, identity: {
                "vehicle_year": generate_vehicle_year(intentional_failure=True)
            },
        },
        {
            "stat_key": "invalid_vehicle_make",
            "apply": lambda record, identity: {
                "vehicle_make": "",
                "vehicle_model": "",
            },
        },
        {
            "stat_key": "invalid_coverage",
            "apply": lambda record, identity: {
                "coverage_type": generate_coverage_type(
                    data_gen_config,
                    intentional_failure=True,
                )
            },
        },
        {
            "stat_key": "invalid_premium",
            "apply": lambda record, identity: {
                "premium_amount": generate_premium_amount(
                    record.get("coverage_type", "basic"),
                    data_gen_config,
                    intentional_failure=True,
                )
            },
        },
        {
            "stat_key": "invalid_status",
            "apply": lambda record, identity: {
                "status": generate_status(
                    data_gen_config,
                    intentional_failure=True,
                )
            },
        },
    ]

    stats = {
        "invalid_license":      0,
        "invalid_birth_year":   0,
        "invalid_name_surname": 0,
        "invalid_city":         0,
        "invalid_country":      0,
        "invalid_phone":        0,
        "invalid_email":        0,
        "empty_plate":          0,
        "invalid_vehicle_type": 0,
        "invalid_vehicle_year": 0,
        "invalid_vehicle_make": 0,
        "invalid_coverage":     0,
        "invalid_premium":      0,
        "invalid_status":       0,
    }
    valid_records = []
    generated_records = []
    registry      = create_policy_registry(total_records, start_index, data_gen_config)

    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False)
    try:
        for i in range(total_records):
            policy_num = generate_policy_number(start_index + i)
            identity   = registry[policy_num]
            record = build_valid_new_policy_record(
                policy_num,
                identity,
                batch_date,
                data_gen_config,
            )

            for injector in failure_injectors:
                if random.random() < failure_rate:
                    record.update(injector["apply"](record, identity))
                    stats[injector["stat_key"]] += 1

            tmp.write(json.dumps(record) + "\n")
            generated_records.append(record.copy())

        tmp.flush()
        valid_records = collect_valid_generated_records(
            generated_records,
            config=config,
            metadata=metadata,
        )
        return tmp.name, stats, valid_records, registry
    except Exception:
        tmp.close()
        os.unlink(tmp.name)
        raise
    finally:
        tmp.close()


def generate_deterministic_overlap_batch(
    batch_num: int,
    batch_date: str,
    batch_size: int,
    start_index: int,
    registry: Dict[str, Dict],
    prior_valid_records: Dict[str, Dict],
    data_gen_config: dict,
) -> Tuple[str, Dict, Dict[str, Dict]]:
    # generate later batches with guaranteed valid overlaps
    # streams to temp file
    # strategy:
    # - deterministic SCD2-changed overlap rows (status/premium_amount changes)
    # - deterministic repeated unchanged rows (tests idempotency)
    # - deterministic duplicates from already-valid rows (tests dedup)
    # - remainder are new valid rows
    deterministic_scd2_count = min(
        int(data_gen_config.get("deterministic_scd2_overlap_count", 5000)),
        len(prior_valid_records),
    )
    deterministic_repeat_count = min(
        int(data_gen_config.get("deterministic_repeat_overlap_count", 2500)),
        len(prior_valid_records),
    )
    duplicate_count = min(
        int(data_gen_config.get(f"batch{batch_num}_duplicate_count", 2500)),
        batch_size,
    )

    scd2_policies = sorted(prior_valid_records)[:deterministic_scd2_count]

    remaining_for_repeat = sorted(p for p in prior_valid_records if p not in set(scd2_policies))
    repeat_policies      = remaining_for_repeat[:deterministic_repeat_count]

    used_overlap_total = len(scd2_policies) + len(repeat_policies)
    new_count          = max(batch_size - used_overlap_total - duplicate_count, 0)

    batch_valid_records: Dict[str, Dict] = {}
    stats = {
        "new_policies":        new_count,
        "updated_policies":    len(scd2_policies),
        "repeated_policies":   len(repeat_policies),
        "duplicate_policies":  duplicate_count,
        "scd1_changes":        0,
        "scd2_changes":        len(scd2_policies),
        "validation_failures": 0,
    }

    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False)
    try:
        for policy_num in scd2_policies:
            record = build_valid_update_record(
                prior_valid_records[policy_num], batch_date, data_gen_config, force_scd2_change=True
            )
            tmp.write(json.dumps(record) + "\n")
            batch_valid_records[policy_num] = record

        for policy_num in repeat_policies:
            record = build_valid_update_record(
                prior_valid_records[policy_num], batch_date, data_gen_config, force_scd2_change=False
            )
            tmp.write(json.dumps(record) + "\n")
            batch_valid_records[policy_num] = record
            if record.get("city") != prior_valid_records[policy_num].get("city"):
                stats["scd1_changes"] += 1

        new_registry_entries = {}
        for i in range(new_count):
            policy_num   = generate_policy_number(start_index + i)
            city_val     = generate_city(data_gen_config)
            vehicle_type = generate_vehicle_type(data_gen_config)
            vehicle_make, vehicle_model = generate_vehicle_make_model(vehicle_type, data_gen_config)
            identity = {
                "name":           fake.first_name(),
                "surname":        fake.last_name(),
                "city":           city_val,
                "license_number": generate_license_number(),
                "birth_year":     fake.date_of_birth(minimum_age=18, maximum_age=85).year,
                "plate_number":   generate_plate_number(),
                "vehicle_type":   vehicle_type,
                "vehicle_year":   generate_vehicle_year(),
                "vehicle_make":   vehicle_make,
                "vehicle_model":  vehicle_model,
            }
            new_registry_entries[policy_num] = identity
            record = build_valid_new_policy_record(policy_num, identity, batch_date, data_gen_config)
            tmp.write(json.dumps(record) + "\n")
            batch_valid_records[policy_num] = record

        registry.update(new_registry_entries)

        duplicate_sources = list(batch_valid_records.values())[:duplicate_count]
        for record in duplicate_sources:
            tmp.write(json.dumps(record) + "\n")

        tmp.flush()
        return tmp.name, stats, batch_valid_records

    except Exception:
        tmp.close()
        os.unlink(tmp.name)
        raise
    finally:
        tmp.close()


def collect_valid_generated_records(
    records: List[Dict],
    config: dict,
    metadata: dict,
) -> List[Dict]:
    if not records:
        return []

    spark = create_spark_session(
        "MotorPolicySampleDataValidation",
        config=config,
        event_log_enabled=False,
    )
    try:
        source = metadata["ingestion"]["source"]
        schema = build_spark_schema(source["schema"])
        df_records = spark.read.schema(schema).json(
            spark.sparkContext.parallelize(json.dumps(record) for record in records)
        )
        df_valid, _ = split_valid_invalid(
            spark,
            df_records,
            metadata["ingestion"].get("validations", []),
        )
        return [row.asDict(recursive=True) for row in df_valid.collect()]
    finally:
        spark.stop()


def upload_jsonl_to_minio(
    file_path: str,
    bucket: str,
    object_name: str,
    storage_config: dict,
) -> None:
    # upload a local jsonl file to minio using fput_object
    client = create_minio_client(storage_config)
    client.fput_object(
        bucket_name=bucket,
        object_name=object_name,
        file_path=file_path,
        content_type="application/json",
    )


# data generation workflow
def generate_sample_data() -> None:
    config = load_config()
    metadata = load_metadata(config)
    data_gen_config = DATA_GENERATION_CONFIG
    storage_config = config.get("storage", {})

    total_records = data_gen_config["total_records"]
    input_bucket  = data_gen_config["input_bucket"]
    seed          = data_gen_config["seed"]
    num_batches   = data_gen_config["num_batches"]
    batch_dates   = data_gen_config["batch_dates"][:num_batches]

    random.seed(seed)
    Faker.seed(seed)
    USED_LICENSE_NUMBERS.clear()

    records_per_batch = total_records // num_batches
    registry: Dict[str, Dict] = {}
    prior_valid_records: Dict[str, Dict] = {}

    for batch_idx, batch_date in enumerate(batch_dates):
        object_name = f"batch-{batch_date}/input.jsonl"
        start_index = batch_idx * records_per_batch
        tmp_path    = None

        try:
            if batch_idx == 0:
                tmp_path, _stats, valid_records, new_registry_entries = (
                    generate_batch_1_new_policies(
                        records_per_batch,
                        start_index,
                        batch_date,
                        data_gen_config,
                        config=config,
                        metadata=metadata,
                    )
                )
                registry.update(new_registry_entries)
                prior_valid_records.update({r["policy_number"]: r for r in valid_records})
            else:
                tmp_path, _stats, batch_valid_records = (
                    generate_deterministic_overlap_batch(
                        batch_num=batch_idx + 1,
                        batch_date=batch_date,
                        batch_size=records_per_batch,
                        start_index=start_index,
                        registry=registry,
                        prior_valid_records=prior_valid_records,
                        data_gen_config=data_gen_config,
                    )
                )
                prior_valid_records.update(batch_valid_records)

            upload_jsonl_to_minio(
                file_path=tmp_path,
                bucket=input_bucket,
                object_name=object_name,
                storage_config=storage_config,
            )
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.unlink(tmp_path)

    logger.info(
        "Generated %s batches and %s records into %s",
        num_batches,
        total_records,
        input_bucket,
    )


if __name__ == "__main__":
    generate_sample_data()
