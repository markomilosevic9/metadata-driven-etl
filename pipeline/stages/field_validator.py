# field validation module

# validates DataFrame records against rules declared in metadata and produces separate valid and invalid DataFrames

# supported rule types:
# a) simple rules (no parameters):
# notNull - field value must not be NULL
# notEmpty - field value must not be an empty string after trimming

# b) parametrised rules:
# regex - field value must match the defined regex pattern
# minValue - field value must be >= the defined numeric minimum
# maxValue - field value must be <= the defined numeric maximum

# multiple rules per field are evaluated independently
# all failures are collected before the record is classified
# if a field is entirely absent from the DataFrame the validator records it as fieldMissing without evaluating any other rules for that field

# output contract:
# a) valid records - no validation_errors column present
# b) invalid records - carry a validation_errors column map<string, array<string>> containing {field_name: [error_codes]}


from typing import Callable, Dict, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def notNull_rule(field: str) -> str:
    # return a sql case expression that flags null values for field
    return f"CASE WHEN {field} IS NULL THEN 'notNull' END"


def notEmpty_rule(field: str) -> str:
    # return a sql case expression that flags empty-string values for field
    return (
        f"CASE WHEN {field} IS NOT NULL AND "
        f"trim(CAST({field} AS STRING)) = '' "
        f"THEN 'notEmpty' END"
    )


def regex_rule(field: str, pattern: str) -> str:
    # return a sql case expression that flags values of field that do not match pattern 
    # backslashes in the pattern are escaped for sparksql parsing
    safe_pattern = str(pattern).replace("\\", "\\\\").replace("'", "''")
    return (
        f"CASE WHEN {field} IS NOT NULL AND "
        f"NOT regexp_like(CAST({field} AS STRING), '{safe_pattern}') "
        f"THEN 'regex: {safe_pattern}' END"
    )


def minValue_rule(field: str, min_val) -> str:
    # return a sql case expression that flags values of field below min_val
    return (
        f"CASE WHEN {field} IS NOT NULL AND "
        f"CAST({field} AS DOUBLE) < {min_val} "
        f"THEN 'minValue: {min_val}' END"
    )


def maxValue_rule(field: str, max_val) -> str:
    # return a sql case expression that flags values of field above max_val
    return (
        f"CASE WHEN {field} IS NOT NULL AND "
        f"CAST({field} AS DOUBLE) > {max_val} "
        f"THEN 'maxValue: {max_val}' END"
    )


# rule dispatch tables keyed by rule name as declared in metadata
SIMPLE_RULES: Dict[str, Callable[[str], str]] = {
    "notNull":  notNull_rule,
    "notEmpty": notEmpty_rule,
}

PARAMETERIZED_RULES: Dict[str, Callable[[str, any], str]] = {
    "regex":    regex_rule,
    "minValue": minValue_rule,
    "maxValue": maxValue_rule,
}


def generate_validation_sql(validations: list, df_columns: list) -> list:
    # translate a list of field validation configs into sparksql expressions
    # for each validation config, produces a <field>_error column expression that collects all rule failures for that field into an array<string>
    # fields absent from the DataFrame produce a constant array('fieldMissing') without evaluating any other rules

    # parameters:
    # validations: list - list of validation config dicts from metadata, each with field and rules keys
    # df_columns: list - column names present in the input DataFrame

    # returns:
    # list - sparksql expressions, one per validated field, ready for use in a select clause

    # raises:
    # ValueError - if an unsupported rule name is encountered
    sql_exprs = []

    for v in validations:
        field = v["field"]
        rules = v["rules"]

        if field not in df_columns:
            # column absent from schema, always flag as fieldMissing
            sql_exprs.append(f"array('fieldMissing') AS {field}_error")
            continue

        conditions = []

        for rule in rules:
            if isinstance(rule, str):
                if rule in SIMPLE_RULES:
                    conditions.append(SIMPLE_RULES[rule](field))
                else:
                    supported_simple = sorted(SIMPLE_RULES.keys())
                    raise ValueError(
                        f"Unsupported validation rule for field '{field}': '{rule}'. "
                        f"Supported rules: {supported_simple}"
                    )

            elif isinstance(rule, dict):
                rule_name   = rule.get("name")
                rule_params = rule.get("params")

                if rule_name in PARAMETERIZED_RULES:
                    conditions.append(PARAMETERIZED_RULES[rule_name](field, rule_params))
                else:
                    supported_parameterized = sorted(PARAMETERIZED_RULES.keys())
                    raise ValueError(
                        f"Unsupported validation rule for field '{field}': '{rule_name}'. "
                        f"Supported rules: {supported_parameterized}"
                    )
            else:
                raise ValueError(
                    f"Invalid validation rule configuration for field '{field}': {rule}."
                )

        if not conditions:
            sql_exprs.append(f"CAST(NULL AS array<string>) AS {field}_error")
            continue

        # collect all rule results into an array, strip nulls (passing rules) and return NULL if every rule passed (empty array -> null via nullif)
        array_expr = f"array({', '.join(conditions)})"
        sql_expr   = f"nullif(array_compact({array_expr}), array()) AS {field}_error"
        sql_exprs.append(sql_expr)

    return sql_exprs


def split_valid_invalid(
    spark,
    df_input: DataFrame,
    validations: list,
) -> Tuple[DataFrame, DataFrame]:
    # apply validation rules to an input dataframe and split records into valid and invalid dataframes

    # parameters:
    # spark: SparkSession
    # df_input: DataFrame - DataFrame containing records to validate
    # validations : list - List of validation config dicts from metadata

    # returns:
    # tuple[DataFrame, DataFrame] - (df_valid, df_invalid)
    # where df_valid contains records with no validation errors 
    # and df_invalid contains records with at least one error, carrying a validation_errors map column
    input_columns = df_input.columns

    validation_exprs = generate_validation_sql(validations, input_columns)
    df_validated = df_input.selectExpr("*", *validation_exprs)

    original_columns = [
        col for col in df_validated.columns
        if not col.endswith("_error")
    ]

    error_columns = [f"{v['field']}_error" for v in validations]
    error_condition = F.lit(False)
    for column_name in error_columns:
        error_condition = error_condition | F.col(column_name).isNotNull()

    error_map_entries = []
    for validation in validations:
        error_map_entries.extend(
            [F.lit(validation["field"]), F.col(f"{validation['field']}_error")]
        )

    df_invalid = df_validated.where(error_condition).select(
        *original_columns,
        F.map_filter(
            F.create_map(*error_map_entries),
            lambda _, value: value.isNotNull(),
        ).alias("validation_errors"),
    )
    df_valid = df_validated.where(~error_condition).select(*original_columns)

    return df_valid, df_invalid
