from functools import reduce

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def apply_rules(df, config):
    """
    Apply configurable data quality rules.

    Returns:
        valid_df: Records passing all rules
        invalid_df: Records with failure reasons
    """
    rules = config["rules"]
    failure_columns = []

    for rule in rules:
        rule_id = rule["id"]
        column = rule["column"]
        rule_type = rule["type"]
        value = F.col(column)

        if column not in df.columns:
            raise ValueError(f"Missing input column: {column}")

        if rule_type == "not_null":
            failed = (
                value.isNull()
                | (F.trim(value.cast("string")) == "")
            )

        elif rule_type == "greater_than":
            numeric = F.expr(
                f"try_cast(`{column}` AS DECIMAL(18,2))"
            )
            failed = (
                numeric.isNull()
                | (numeric <= F.lit(rule["value"]))
            )

        elif rule_type == "allowed_values":
            failed = (
                value.isNull()
                | ~F.upper(F.trim(value)).isin(
                    [v.upper() for v in rule["values"]]
                )
            )

        elif rule_type == "valid_date":
            parsed = F.expr(
                f"try_to_timestamp(`{column}`, "
                f"'{rule['format']}')"
            )
            failed = parsed.isNull()

        elif rule_type == "unique":
            # Flag every occurrence of a duplicated ID.
            counts = Window.partitionBy(column)
            failed = F.count(F.lit(1)).over(counts) > 1

        else:
            raise ValueError(
                f"Unsupported rule type: {rule_type}"
            )

        failure_column = f"_failed_{rule_id}"
        df = df.withColumn(
            failure_column,
            F.when(failed, F.lit(rule_id))
        )
        failure_columns.append(failure_column)

    df = df.withColumn(
    "failure_reasons",
    F.filter(
        F.array(*[F.col(c) for c in failure_columns]),
        lambda x: x.isNotNull()
    )
)

    valid_df = df.filter(
        F.size(F.col("failure_reasons")) == 0
    ).drop(*failure_columns)

    invalid_df = df.filter(
        F.size(F.col("failure_reasons")) > 0
    ).drop(*failure_columns)

    return valid_df, invalid_df
