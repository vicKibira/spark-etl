
"""
Pizza Sales ETL using PySpark

This script reads pizza sales data from a CSV file, cleans and validates it,
removes duplicate records, creates useful date columns, generates sales
reports, and saves the results as Parquet files.

It also demonstrates:
- Creating a SparkSession
- Reading CSV files with Spark
- DataFrame transformations and actions
- Handling missing and invalid data
- Type casting and date conversion
- Removing duplicate records
- Grouping and aggregating sales data
- Window functions and ranking
- Spark SQL queries
- Saving partitioned Parquet files
- Understanding Spark execution plans
- Adaptive Query Execution (AQE)
- Optional publishing to Google BigQuery

The script is designed to run on a single-node or small Spark cluster
without unnecessarily caching large DataFrames.
"""

import logging
import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window


# These are the columns expected in the original pizza sales CSV file.
COLS = [
    "pizza_id",
    "order_id",
    "pizza_name_id",
    "quantity",
    "order_date",
    "order_time",
    "unit_price",
    "total_price",
    "pizza_size",
    "pizza_category",
    "pizza_ingredients",
    "pizza_name"
]


# Main configuration settings.
# You can change the paths and settings here without modifying the ETL logic.

SOURCE_PATH = (
    sys.argv[1]
    if len(sys.argv) > 1
    else "/opt/data/pizza_sales.csv"
)

OUTPUT_PATH = (
    sys.argv[2]
    if len(sys.argv) > 2
    else "/opt/data/output/pizza_sales"
)

DATE_FORMAT = "M/d/yyyy"

# overwrite replaces existing output, while append adds new records.
WRITE_MODE = "overwrite"

# Controls the number of partitions used during Spark shuffle operations.
SHUFFLE_PARTITIONS = (
    int(sys.argv[3])
    if len(sys.argv) > 3
    else 8
)

# 1.0 processes the full dataset. A value such as 0.1 processes about 10%.
SAMPLE_FRACTION = 1.0


# BigQuery settings.
# Leave BQ_PROJECT_ID as None if you do not want to publish to BigQuery.

BQ_PROJECT_ID = None
BQ_DATASET = "pizza_sales"
BQ_TEMP_BUCKET = None


def start_spark() -> SparkSession:
    """
    Creates and configures the SparkSession.

    SparkSession is the main entry point for working with Spark DataFrames
    and Spark SQL.

    Adaptive Query Execution allows Spark to adjust parts of the execution
    plan at runtime. This can improve performance when Spark has better
    information about the actual data being processed.

    Snappy compression is enabled for Parquet output to reduce storage usage.
    """

    if SHUFFLE_PARTITIONS <= 0:
        raise ValueError("SHUFFLE_PARTITIONS must be greater than zero.")

    spark = (
        SparkSession.builder
        .appName("PizzaSales_ETL_A_to_Z")
        .config(
            "spark.sql.shuffle.partitions",
            SHUFFLE_PARTITIONS
        )
        .config(
            "spark.sql.adaptive.enabled",
            "true"
        )
        .config(
            "spark.sql.adaptive.coalescePartitions.enabled",
            "true"
        )
        .config(
            "spark.sql.adaptive.skewJoin.enabled",
            "true"
        )
        .config(
            "spark.sql.parquet.compression.codec",
            "snappy"
        )
        .getOrCreate()
    )

    return spark


def read_pizza_sales(
    spark: SparkSession,
    source: str,
    fraction: float
) -> DataFrame:
    """
    Reads the original pizza sales CSV file into a Spark DataFrame.

    The CSV is initially read without automatically converting the data types.
    This allows the cleaning function to handle type conversion consistently.

    The function also checks that all expected columns exist before
    continuing with the ETL process.

    Sampling is optional and can be useful when testing the pipeline
    with a smaller portion of the dataset.
    """

    if fraction <= 0 or fraction > 1:
        raise ValueError(
            "SAMPLE_FRACTION must be greater than 0 and at most 1."
        )

    df = (
        spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .option("quote", '"')
        .option("escape", '"')
        .option("encoding", "UTF-8")
        .csv(source)
    )

    # Remove accidental spaces from column names.
    df = df.toDF(*[column.strip() for column in df.columns])

    # Check whether the source file contains every required column.
    missing = [
        column
        for column in COLS
        if column not in df.columns
    ]

    if missing:
        raise ValueError(
            f"Missing required pizza dataset columns: {missing}. "
            f"Columns found: {df.columns}"
        )

    # Select only the columns needed by this ETL pipeline.
    df = df.select(*COLS)

    # Take a random sample only when the configured fraction is below 1.
    if fraction < 1:
        df = df.sample(
            withReplacement=False,
            fraction=fraction,
            seed=17
        )

    return df


def save_pizza_data(
    df: DataFrame,
    path: str,
    mode: str = "overwrite",
    partitions: list = None
) -> None:
    """
    Saves a DataFrame as a Parquet dataset.

    Parquet is a column-oriented file format. It is generally more efficient
    than CSV for analytical workloads because it supports compression
    and allows Spark to read only the columns needed by a query.

    Optional partitioning creates separate directory structures based
    on the selected columns.

    For example, partitioning by order_year and order_month creates
    directories that make it easier to query particular periods.
    """

    allowed_modes = {
        "overwrite",
        "append",
        "ignore",
        "error",
        "errorifexists"
    }

    if mode not in allowed_modes:
        raise ValueError(
            f"Unsupported write mode: {mode}"
        )

    writer = (
        df.write
        .mode(mode)
        .format("parquet")
        .option("compression", "snappy")
    )

    # Partition only when partition columns have been supplied.
    if partitions:
        writer = writer.partitionBy(*partitions)

    writer.save(path)

    logging.info("Successfully saved Parquet data to %s", path)


def clean_pizza_sales(
    raw_sales: DataFrame,
    date_format: str
):
    """
    Cleans and standardizes the raw pizza sales data.

    This function is responsible for the main Silver-layer transformation.

    It performs the following operations:

    1. Converts numeric columns into appropriate data types.
    2. Converts order dates and times into timestamp values.
    3. Removes unnecessary spaces from text fields.
    4. Standardizes pizza sizes and category names.
    5. Identifies records containing invalid or missing values.
    6. Separates invalid records into bad_sales.
    7. Removes duplicate pizza line items.
    8. Creates additional date and time columns for analysis.

    Invalid conversions are handled using try_cast and try_to_timestamp.
    These functions return null for values that cannot be converted,
    allowing the records to be identified and quarantined.
    """

    # Prepare the date formats that may appear in the source CSV.
    # The configured format is tried first, followed by common alternatives.
    date_formats = list(dict.fromkeys([
        date_format,
        "M/d/yyyy",
        "d/M/yyyy",
        "M-d-yyyy",
        "d-M-yyyy"
    ]))

    # Prepare the possible time formats.
    time_formats = [
        "H:mm:ss",
        "H:mm",
        "h:mm:ss a",
        "h:mm a"
    ]

    # Escape quotes in format strings before placing them inside SQL expressions.
    def sql_string(value):
        return "'" + value.replace("'", "''") + "'"

    # Build safe timestamp parsing expressions for Spark SQL.
    def try_timestamp(column_expression, fmt):
        return F.expr(
            f"try_to_timestamp("
            f"{column_expression}, {sql_string(fmt)})"
        )

    # Parse the date independently so it can be used for date-based analysis.
    parsed_date = F.coalesce(
        *[
            try_timestamp("trim(order_date)", fmt)
            for fmt in date_formats
        ]
    ).cast("date")

    # Combine the date and time columns to create a complete timestamp.
    combined_datetime = "concat_ws(' ', trim(order_date), trim(order_time))"

    timestamp_formats = [
        f"{date_fmt} {time_fmt}"
        for date_fmt in date_formats
        for time_fmt in time_formats
    ]

    parsed_timestamp = F.coalesce(
        *[
            try_timestamp(combined_datetime, fmt)
            for fmt in timestamp_formats
        ]
    )

    # Convert and standardize the raw fields.
    parsed = (
        raw_sales.select(

            F.expr(
                "try_cast(trim(pizza_id) AS BIGINT)"
            ).alias("pizza_id"),

            F.expr(
                "try_cast(trim(order_id) AS BIGINT)"
            ).alias("order_id"),

            F.when(
                F.trim(F.col("pizza_name_id")) != "",
                F.trim(F.col("pizza_name_id"))
            ).otherwise(None).alias("pizza_name_id"),

            F.expr(
                "try_cast(trim(quantity) AS INT)"
            ).alias("quantity"),

            parsed_date.alias("order_date"),

            parsed_timestamp.alias("order_timestamp"),

            F.expr(
                "try_cast(trim(unit_price) AS DECIMAL(10,2))"
            ).alias("unit_price"),

            F.expr(
                "try_cast(trim(total_price) AS DECIMAL(12,2))"
            ).alias("total_price"),

            F.upper(
                F.trim(F.col("pizza_size"))
            ).alias("pizza_size"),

            F.initcap(
                F.trim(F.col("pizza_category"))
            ).alias("pizza_category"),

            F.trim(
                F.col("pizza_ingredients")
            ).alias("pizza_ingredients"),

            F.trim(
                F.col("pizza_name")
            ).alias("pizza_name")

        )
        .withColumn(
            "order_time",
            F.date_format("order_timestamp", "HH:mm:ss")
        )
    )

    # Define the rules that determine whether a record is invalid.
    #
    # A record is considered invalid when important fields are missing,
    # conversion fails, or numerical values fall outside the expected range.

    bad = (
        F.col("pizza_id").isNull()
        | F.col("order_id").isNull()
        | F.col("pizza_name_id").isNull()
        | (F.col("pizza_name_id") == "")
        | F.col("quantity").isNull()
        | (F.col("quantity") <= 0)
        | F.col("order_date").isNull()
        | F.col("order_timestamp").isNull()
        | F.col("unit_price").isNull()
        | (F.col("unit_price") <= 0)
        | F.col("total_price").isNull()
        | (F.col("total_price") <= 0)
        | F.col("pizza_size").isNull()
        | (F.col("pizza_size") == "")
        | F.col("pizza_category").isNull()
        | (F.col("pizza_category") == "")
        | F.col("pizza_name").isNull()
        | (F.col("pizza_name") == "")
    )

    # Add a reason column so that invalid records are easier to investigate.
    rejection_reasons = [
        F.when(
            F.col("pizza_id").isNull(),
            F.lit("Missing or invalid pizza_id")
        ),
        F.when(
            F.col("order_id").isNull(),
            F.lit("Missing or invalid order_id")
        ),
        F.when(
            F.col("pizza_name_id").isNull()
            | (F.col("pizza_name_id") == ""),
            F.lit("Missing pizza_name_id")
        ),
        F.when(
            F.col("quantity").isNull(),
            F.lit("Missing or invalid quantity")
        ),
        F.when(
            F.col("quantity") <= 0,
            F.lit("Quantity must be greater than zero")
        ),
        F.when(
            F.col("order_date").isNull(),
            F.lit("Missing or invalid order_date")
        ),
        F.when(
            F.col("order_timestamp").isNull(),
            F.lit("Missing or invalid order_time")
        ),
        F.when(
            F.col("unit_price").isNull(),
            F.lit("Missing or invalid unit_price")
        ),
        F.when(
            F.col("unit_price") <= 0,
            F.lit("Unit price must be greater than zero")
        ),
        F.when(
            F.col("total_price").isNull(),
            F.lit("Missing or invalid total_price")
        ),
        F.when(
            F.col("total_price") <= 0,
            F.lit("Total price must be greater than zero")
        ),
        F.when(
            F.col("pizza_size").isNull()
            | (F.col("pizza_size") == ""),
            F.lit("Missing pizza_size")
        ),
        F.when(
            F.col("pizza_category").isNull()
            | (F.col("pizza_category") == ""),
            F.lit("Missing pizza_category")
        ),
        F.when(
            F.col("pizza_name").isNull()
            | (F.col("pizza_name") == ""),
            F.lit("Missing pizza_name")
        )
    ]

    # Keep invalid records separate instead of silently discarding them.
    bad_sales = (
        parsed
        .filter(bad)
        .withColumn(
            "rejection_reason",
            F.concat_ws("; ", *rejection_reasons)
        )
    )

    # Only valid records continue to the cleaned dataset.
    valid = parsed.filter(~bad)

    # Remove duplicate pizza line items.
    #
    # This assumes pizza_id uniquely identifies a line item.
    # Confirm that assumption against the source dataset before production use.
    window = (
        Window
        .partitionBy("pizza_id")
        .orderBy(
            F.col("order_timestamp").asc(),
            F.col("order_id").asc()
        )
    )

    clean_sales = (
        valid
        .withColumn(
            "_row_number",
            F.row_number().over(window)
        )
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")

        # Create additional columns that make reporting easier.
        .withColumn(
            "order_year",
            F.year("order_date")
        )
        .withColumn(
            "order_month",
            F.month("order_date")
        )
        .withColumn(
            "order_day",
            F.dayofmonth("order_date")
        )
        .withColumn(
            "order_weekday",
            F.date_format("order_date", "EEEE")
        )
        .withColumn(
            "order_hour",
            F.hour("order_timestamp")
        )
        .withColumn(
            "order_month_key",
            F.date_format("order_date", "yyyy-MM")
        )
    )

    return clean_sales, bad_sales


def make_sales_reports(s: DataFrame) -> dict:
    """
    Creates the Gold-layer sales reports.

    These reports summarize the cleaned pizza sales data and provide
    information that can be used for business analysis and dashboards.

    The function uses groupBy, agg, sum, countDistinct, avg, max, min,
    and window functions to calculate different sales metrics.

    The returned dictionary contains one DataFrame for each report.
    """

    # Overall business performance.
    overall = (
        s.agg(
            F.round(
                F.sum("total_price"), 2
            ).alias("total_revenue"),

            F.countDistinct(
                "order_id"
            ).alias("total_orders"),

            F.sum(
                "quantity"
            ).alias("total_pizzas_sold")
        )
        .withColumn(
            "average_order_value",
            F.round(
                F.col("total_revenue")
                / F.col("total_orders"),
                2
            )
        )
    )

    # Revenue and sales volume for each pizza category.
    category = (
        s.groupBy("pizza_category")
        .agg(
            F.round(
                F.sum("total_price"), 2
            ).alias("total_revenue"),

            F.sum(
                "quantity"
            ).alias("pizzas_sold"),

            F.countDistinct(
                "order_id"
            ).alias("orders")
        )
        .orderBy(F.desc("total_revenue"))
    )

    # Ten pizzas with the highest quantities sold.
    top_quantity = (
        s.groupBy("pizza_name")
        .agg(
            F.sum(
                "quantity"
            ).alias("total_quantity_sold"),

            F.round(
                F.sum("total_price"), 2
            ).alias("total_revenue")
        )
        .orderBy(F.desc("total_quantity_sold"))
        .limit(10)
    )

    # Ten pizzas generating the highest revenue.
    top_revenue = (
        s.groupBy("pizza_name")
        .agg(
            F.round(
                F.sum("total_price"), 2
            ).alias("total_revenue"),

            F.sum(
                "quantity"
            ).alias("total_quantity_sold")
        )
        .orderBy(F.desc("total_revenue"))
        .limit(10)
    )

    # Revenue and number of pizzas sold for each pizza size.
    size = (
        s.groupBy("pizza_size")
        .agg(
            F.round(
                F.sum("total_price"), 2
            ).alias("revenue"),

            F.sum(
                "quantity"
            ).alias("pizzas_sold")
        )
        .orderBy(F.desc("revenue"))
    )

    # Sales performance grouped by the hour of the day.
    hourly = (
        s.groupBy("order_hour")
        .agg(
            F.countDistinct(
                "order_id"
            ).alias("orders"),

            F.sum(
                "quantity"
            ).alias("pizzas_sold"),

            F.round(
                F.sum("total_price"), 2
            ).alias("revenue")
        )
        .orderBy(F.desc("orders"))
    )

    # Daily sales performance.
    daily = (
        s.groupBy("order_date")
        .agg(
            F.round(
                F.sum("total_price"), 2
            ).alias("daily_revenue"),

            F.countDistinct(
                "order_id"
            ).alias("daily_orders"),

            F.sum(
                "quantity"
            ).alias("daily_pizzas_sold")
        )
        .orderBy("order_date")
    )

    # Monthly sales performance.
    monthly = (
        s.groupBy("order_month_key")
        .agg(
            F.round(
                F.sum("total_price"), 2
            ).alias("monthly_revenue"),

            F.countDistinct(
                "order_id"
            ).alias("monthly_orders"),

            F.sum(
                "quantity"
            ).alias("monthly_pizzas_sold")
        )
        .orderBy("order_month_key")
    )

    # Calculate order-level statistics.
    #
    # First, calculate the total value of each order.
    # Then calculate the average, largest, and smallest order values.
    #
    # This is more accurate than averaging individual pizza line items.

    per_order = (
        s.groupBy("order_id")
        .agg(
            F.sum("total_price").alias("order_total")
        )
    )

    order_stats = (
        per_order.agg(
            F.round(
                F.avg("order_total"), 2
            ).alias("average_order_value"),

            F.round(
                F.max("order_total"), 2
            ).alias("largest_order"),

            F.round(
                F.min("order_total"), 2
            ).alias("smallest_order")
        )
    )

    # Analyze sales performance by day of the week.
    weekday = (
        s.groupBy(
            F.dayofweek("order_date").alias("weekday_number"),
            F.date_format(
                "order_date", "EEEE"
            ).alias("day_of_week")
        )
        .agg(
            F.countDistinct(
                "order_id"
            ).alias("orders"),

            F.sum(
                "quantity"
            ).alias("pizzas_sold"),

            F.round(
                F.sum("total_price"), 2
            ).alias("revenue")
        )
        .orderBy("weekday_number")
    )

    # Identify the ten pizzas with the lowest sales quantities.
    bottom = (
        s.groupBy("pizza_name")
        .agg(
            F.sum(
                "quantity"
            ).alias("quantity_sold"),

            F.round(
                F.sum("total_price"), 2
            ).alias("revenue")
        )
        .orderBy(F.asc("quantity_sold"))
        .limit(10)
    )

    # Use a window function to rank pizzas within each category.
    #
    # dense_rank assigns a ranking based on revenue.
    # The ranking restarts for each pizza category.
    # Only the top three pizzas in each category are retained.

    window = (
        Window
        .partitionBy("pizza_category")
        .orderBy(F.desc("revenue"))
    )

    category_rank = (
        s.groupBy(
            "pizza_category",
            "pizza_name"
        )
        .agg(
            F.round(
                F.sum("total_price"), 2
            ).alias("revenue")
        )
        .withColumn(
            "rank_in_category",
            F.dense_rank().over(window)
        )
        .filter(
            F.col("rank_in_category") <= 3
        )
    )

    # Return all report DataFrames in one dictionary.
    return {
        "overall_metrics": overall,
        "revenue_by_category": category,
        "top_10_by_quantity": top_quantity,
        "top_10_by_revenue": top_revenue,
        "revenue_by_size": size,
        "sales_by_hour": hourly,
        "daily_revenue": daily,
        "monthly_revenue": monthly,
        "order_value_stats": order_stats,
        "sales_by_weekday": weekday,
        "bottom_10_by_quantity": bottom,
        "top_3_pizzas_per_category": category_rank
    }


def show_sql_examples(s: DataFrame) -> None:
    """
    Demonstrates how to use Spark SQL with an existing DataFrame.

    A temporary view allows the cleaned DataFrame to be queried using
    standard SQL statements instead of only the DataFrame API.

    The temporary view exists only during the current SparkSession.
    """

    s.createOrReplaceTempView("pizza_sales_silver")

    sql_examples = [

        (
            "Overall metrics",
            """
            SELECT
                ROUND(SUM(total_price), 2) AS revenue,
                COUNT(DISTINCT order_id) AS orders,
                SUM(quantity) AS pizzas_sold
            FROM pizza_sales_silver
            """
        ),

        (
            "Revenue by category",
            """
            SELECT
                pizza_category,
                ROUND(SUM(total_price), 2) AS revenue
            FROM pizza_sales_silver
            GROUP BY pizza_category
            ORDER BY revenue DESC
            """
        ),

        (
            "Top pizzas",
            """
            SELECT
                pizza_name,
                SUM(quantity) AS quantity_sold
            FROM pizza_sales_silver
            GROUP BY pizza_name
            ORDER BY quantity_sold DESC
            LIMIT 10
            """
        ),

        (
            "Hourly sales",
            """
            SELECT
                order_hour,
                COUNT(DISTINCT order_id) AS orders,
                ROUND(SUM(total_price), 2) AS revenue
            FROM pizza_sales_silver
            GROUP BY order_hour
            ORDER BY order_hour
            """
        ),

        (
            "Daily sales",
            """
            SELECT
                order_date,
                ROUND(SUM(total_price), 2) AS revenue
            FROM pizza_sales_silver
            GROUP BY order_date
            ORDER BY order_date
            """
        )

    ]

    # Run each SQL query and display its result.
    for label, query in sql_examples:

        logging.info("Running Spark SQL example: %s", label)

        s.sparkSession.sql(query).show(
            20,
            truncate=False
        )


def save_to_bigquery(
    df: DataFrame,
    table: str,
    temp_bucket: str = None
) -> None:
    """
    Publishes a DataFrame to Google BigQuery.

    This requires the Spark BigQuery connector to be installed
    and configured in the Spark environment.

    The destination table must use this format:

        project_id.dataset_name.table_name

    When a temporary GCS bucket is supplied, the connector uses
    the indirect write method.

    Without a temporary bucket, the direct write method is selected.
    """

    writer = (
        df.write
        .format("bigquery")
        .option("table", table)
        .mode("overwrite")
    )

    if temp_bucket:
        writer = (
            writer
            .option("temporaryGcsBucket", temp_bucket)
            .option("writeMethod", "indirect")
        )
    else:
        writer = writer.option("writeMethod", "direct")

    writer.save()

    logging.info(
        "Successfully published data to BigQuery table: %s",
        table
    )


def main() -> None:
    """
    Controls the complete pizza sales ETL process.

    The main function coordinates the different stages:

    1. Start Spark.
    2. Read the raw CSV data.
    3. Save the original representation in the Bronze layer.
    4. Clean and validate the data for the Silver layer.
    5. Save invalid records separately.
    6. Generate Gold-layer business reports.
    7. Demonstrate Spark SQL.
    8. Display the execution plan.
    9. Optionally publish the reports to BigQuery.
    10. Stop Spark when processing is complete.

    The try/finally structure makes sure the SparkSession is stopped
    even if an error occurs during processing.
    """

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )

    if SAMPLE_FRACTION <= 0 or SAMPLE_FRACTION > 1:
        raise ValueError(
            "SAMPLE_FRACTION must be greater than 0 and at most 1."
        )

    if BQ_PROJECT_ID and not BQ_DATASET:
        raise ValueError(
            "BQ_DATASET must be configured when BigQuery publishing is enabled."
        )

    spark = start_spark()

    try:

        logging.info(
            "Starting Pizza Sales ETL using Spark version %s",
            spark.version
        )

        # Step 1: Read the source CSV file.
        #
        # Spark transformations are lazy. Reading the file creates a
        # DataFrame plan; Spark executes the work when an action is called.

        logging.info("Reading source data from %s", SOURCE_PATH)

        raw_sales = read_pizza_sales(
            spark,
            SOURCE_PATH,
            SAMPLE_FRACTION
        )

        raw_sales.printSchema()

        raw_sales.show(
            5,
            truncate=False
        )

        # Step 2: Save the raw data in the Bronze layer.
        #
        # This preserves the original field representation as read from
        # the CSV, before the cleaning and type conversion take place.

        logging.info("Saving Bronze data")

        save_pizza_data(
            raw_sales,
            f"{OUTPUT_PATH}/bronze/raw_sales",
            WRITE_MODE
        )

        # Step 3: Clean the source data.
        #
        # The function returns two DataFrames:
        # clean_sales contains valid and deduplicated records.
        # bad_sales contains records that failed validation.

        logging.info("Cleaning and validating pizza sales data")

        clean_sales, bad_sales = clean_pizza_sales(
            raw_sales,
            DATE_FORMAT
        )

        clean_sales.printSchema()

        # Save the cleaned Silver data.
        # Partitioning by year and month makes date-based queries easier.

        save_pizza_data(
            clean_sales,
            f"{OUTPUT_PATH}/clean_sales/cleaned",
            WRITE_MODE,
            ["order_year", "order_month"]
        )

        # Save invalid records separately so they can be reviewed later.

        save_pizza_data(
            bad_sales,
            f"{OUTPUT_PATH}/bad_sales/invalid",
            WRITE_MODE
        )

        # The writes above execute the DataFrame transformations.
        # Avoiding extra count() calls reduces unnecessary repeated work.

        logging.info(
            "Bronze, Silver, and invalid-record datasets were saved."
        )

        # Step 4: Generate the Gold-layer business reports.
        #
        # Each report is a DataFrame containing a particular view
        # of the cleaned pizza sales data.

        logging.info("Generating Gold-layer sales reports")

        sales_reports = make_sales_reports(clean_sales)

        for name, df in sales_reports.items():

            logging.info(
                "Displaying and saving report: %s",
                name
            )

            df.show(
                20,
                truncate=False
            )

            save_pizza_data(
                df,
                f"{OUTPUT_PATH}/sales_reports/{name}",
                WRITE_MODE
            )

        # Step 5: Demonstrate how the Silver DataFrame can be queried
        # using Spark SQL in addition to the DataFrame API.

        logging.info("Running Spark SQL examples")

        show_sql_examples(clean_sales)

        # Step 6: Display the physical execution plan.
        #
        # This helps explain how Spark intends to execute the query.
        # The Spark UI can also be used to inspect stages, tasks,
        # shuffle activity, memory usage, and execution time.

        logging.info("Displaying Spark execution plan")

        (
            clean_sales
            .groupBy("pizza_category")
            .agg(
                F.sum("total_price").alias("revenue")
            )
            .explain(mode="formatted")
        )

        # Step 7: Optionally publish the Gold reports to BigQuery.
        #
        # This section is skipped when BQ_PROJECT_ID is None.

        if BQ_PROJECT_ID:

            logging.info("Publishing reports to Google BigQuery")

            for name, df in sales_reports.items():

                table_name = (
                    f"{BQ_PROJECT_ID}."
                    f"{BQ_DATASET}."
                    f"{name}"
                )

                save_to_bigquery(
                    df,
                    table_name,
                    BQ_TEMP_BUCKET
                )

        logging.info("Pizza Sales ETL completed successfully.")

    except Exception:

        logging.exception(
            "An error occurred while running the Pizza Sales ETL."
        )

        raise

    finally:

        # Always stop Spark to release resources after the ETL process.
        spark.stop()

        logging.info("SparkSession stopped.")


# This makes sure main() runs only when this file is executed directly.
# It will not automatically run if the file is imported as a Python module.

if __name__ == "__main__":
    main()
