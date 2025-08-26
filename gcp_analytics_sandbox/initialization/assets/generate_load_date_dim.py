import polars as pl
from datetime import date
from dagster_gcp import GCSResource
from pathlib import Path
import holidays # For fetching statutory holidays
import dagster as dg
from gcp_analytics_sandbox.shared_resources.default_partition_definition import default_partition_definition

def generate_date_dimension_polars(start_date_str: str, end_date_str: str, country_code: str = 'CA', province_code: str = None, subdivision_code: str = None):
    """
    Generates a comprehensive Date Dimension DataFrame using Polars with statutory holidays.

    Args:
        start_date_str (str): The start date in 'YYYY-MM-DD' format.
        end_date_str (str): The end date in 'YYYY-MM-DD' format.
        country_code (str): ISO 3166-1 alpha-2 country code (e.g., 'CA' for Canada, 'US' for USA).
        province_code (str): ISO 3166-2 province/state code (e.g., 'BC' for British Columbia).
                             Required for some countries like Canada for accurate provincial holidays.
        subdivision_code (str): Further subdivision if applicable (less common for general holidays).

    Returns:
        polars.DataFrame: The Date Dimension table.
    """
    start_date_dt = date.fromisoformat(start_date_str)
    end_date_dt = date.fromisoformat(end_date_str)

    # Determine the years for holiday fetching
    years_for_holidays = list(range(start_date_dt.year, end_date_dt.year + 1))

    # Fetch holidays using the 'holidays' library
    if country_code == 'CA' and not province_code:
        print("Warning: For Canadian holidays, specifying a province_code (e.g., 'BC') is highly recommended for accuracy.")
        country_holidays_obj = holidays.CountryHoliday(country_code, years=years_for_holidays)
    elif country_code == 'CA' and province_code:
        country_holidays_obj = holidays.CountryHoliday(country_code, prov=province_code, years=years_for_holidays)
        print(f"Fetching holidays for {country_code}, Province: {province_code}")
    elif country_code:
        country_holidays_obj = holidays.CountryHoliday(country_code, years=years_for_holidays)
        print(f"Fetching holidays for {country_code}")
    else:
        country_holidays_obj = {}
        print("No country code specified, no holidays will be automatically populated.")

    # Create a Polars DataFrame from the holidays dictionary for easier joining
    if country_holidays_obj:
        holidays_list = [{"holiday_date": d, "holiday_name_temp": name} for d, name in sorted(country_holidays_obj.items())]
        holidays_df = pl.DataFrame(holidays_list).with_columns(pl.col("holiday_date").cast(pl.Date))
    else:
        holidays_df = pl.DataFrame({"holiday_date": [], "holiday_name_temp": []}).with_columns(
            pl.col("holiday_date").cast(pl.Date),
            pl.col("holiday_name_temp").cast(pl.String)
        )

    # Generate a date range
    df = pl.DataFrame({
        "full_date": pl.date_range(start_date_dt, end_date_dt, interval="1d", eager=True)
    })

    df = df.with_columns([
        # --- Core Date Information ---
        pl.col("full_date").dt.strftime('%Y%m%d').cast(pl.Int32).alias("date_key"),
        (pl.col("full_date").dt.weekday() - 1).alias("day_of_week_number"), # Polars: Monday=1..Sunday=7 -> Monday=0..Sunday=6
        pl.col("full_date").dt.strftime('%A').alias("day_of_week_name"),
        pl.col("full_date").dt.strftime('%a').alias("day_of_week_short_name"),
        # polars temporal expressions can be found at: https://docs.pola.rs/api/python/dev/reference/expressions/temporal.html
        pl.col("full_date").dt.day().alias("day_of_month"),
        pl.col("full_date").dt.ordinal_day().alias("day_of_year"), # Day of year
        pl.col("full_date").dt.week().alias("iso_week_number"),
        pl.col("full_date").dt.iso_year().alias("iso_year"),
        ((pl.col("full_date").dt.ordinal_day() -1 ) // 7 + 1).alias("us_week_of_year"), # Approx US week

        # --- Month-Related Information ---
        pl.col("full_date").dt.month().alias("month_number"),
        pl.col("full_date").dt.strftime('%B').alias("month_name"),
        pl.col("full_date").dt.strftime('%b').alias("month_short_name"),
        pl.col("full_date").dt.strftime('%Y%m').cast(pl.Int32).alias("year_month_number"),
        (pl.col("full_date").dt.year().cast(pl.String) + pl.lit("-") + pl.col("full_date").dt.strftime('%B')).alias("year_month_name"),
        pl.col("full_date").dt.month_start().alias("first_day_of_month"),
        pl.col("full_date").dt.month_end().alias("last_day_of_month"),
        pl.col("full_date").dt.month_end().dt.day().alias("days_in_month"),


        # --- Quarter-Related Information ---
        pl.col("full_date").dt.quarter().alias("quarter_number"),
        (pl.lit("Q") + pl.col("full_date").dt.quarter().cast(pl.String)).alias("quarter_name"),
        (pl.col("full_date").dt.year().cast(pl.String) + pl.col("full_date").dt.quarter().cast(pl.String)).cast(pl.Int32).alias("year_quarter_number"),
        (pl.col("full_date").dt.year().cast(pl.String) + pl.lit("-Q") + pl.col("full_date").dt.quarter().cast(pl.String)).alias("year_quarter_name"),
        pl.col("full_date").dt.truncate("1q").alias("first_day_of_quarter"),
        pl.col("full_date").dt.truncate("1q").dt.offset_by("1q").dt.offset_by("-1d").alias("last_day_of_quarter"),

        # --- Year-Related Information ---
        pl.col("full_date").dt.year().alias("year_number"),
        pl.col("full_date").dt.truncate("1y").alias("first_day_of_year"),
        pl.col("full_date").dt.truncate("1y").dt.offset_by("1y").dt.offset_by("-1d").alias("last_day_of_year"),
        pl.col("full_date").dt.is_leap_year().alias("is_leap_year"),
    ])

    # --- Flags and Indicators (Holidays) ---
    # Join with holidays_df
    df = df.join(holidays_df, left_on="full_date", right_on="holiday_date", how="left")
    df = df.with_columns([
        pl.col("holiday_name_temp").alias("holiday_name"), # rename
        pl.col("holiday_name_temp").is_not_null().alias("is_holiday")
    ]).drop("holiday_name_temp")


    df = df.with_columns([
        (pl.col("day_of_week_number") < 5).alias("is_weekday"), # Monday to Friday (0-4)
    ])
    df = df.with_columns([
        pl.col("is_weekday").not_().alias("is_weekend"),
        (pl.col("is_weekday") & pl.col("is_holiday").not_()).alias("is_workday")
    ])
    
    # --- Other Useful Attributes ---
    # Day Suffix - Polars doesn't have a direct .apply like pandas for complex scalar functions easily,
    # so we might need a helper or do it more explicitly if performance is critical.
    # For simplicity here, we'll use .map_elements, which is less performant than pure expressions.
    def get_day_suffix_polars(day_series: pl.Series) -> pl.Series:
        def suffix(day):
            if 11 <= day <= 13:
                return 'th'
            suffixes = {1: 'st', 2: 'nd', 3: 'rd'}
            return suffixes.get(day % 10, 'th')
        return day_series.map_elements(suffix, return_dtype=pl.String)

    df = df.with_columns(
        get_day_suffix_polars(pl.col("day_of_month")).alias("day_suffix")
    )
    
    # Week ending dates
    # Polars weekday: Monday=1, ..., Sunday=7. We adjusted day_of_week_number to Mon=0..Sun=6
    # For Saturday end: days to add = 5 - day_of_week_number
    # For Sunday end: days to add = 6 - day_of_week_number
    df = df.with_columns([
        (pl.col("full_date") + pl.duration(days=(5 - pl.col("day_of_week_number")))).alias("week_ending_date_saturday"),
        (pl.col("full_date") + pl.duration(days=(6 - pl.col("day_of_week_number")))).alias("week_ending_date_sunday")
    ])


    # --- Fiscal Period Information (Example: Fiscal year starts October 1st) ---
    fiscal_start_month = 10
    df = df.with_columns([
        pl.when(pl.col("month_number") < fiscal_start_month)
          .then(pl.col("year_number"))
          .otherwise(pl.col("year_number") + 1)
          .alias("fiscal_year_number"),
        
        (((pl.col("month_number") - fiscal_start_month + 12) % 12)).alias("fiscal_month_offset") # 0-indexed fiscal month
    ])
    df = df.with_columns(
        (pl.col("fiscal_month_offset") // 3 + 1).alias("fiscal_quarter_number")
    )

    # Reorder columns for better readability
    column_order = [
        'date_key', 'full_date', 'year_number', 'month_number', 'day_of_month',
        'day_of_week_number', 'day_of_week_name', 'day_of_week_short_name', 'day_of_year',
        'iso_week_number', 'iso_year', 'us_week_of_year', 'month_name', 'month_short_name',
        'year_month_number', 'year_month_name', 'first_day_of_month', 'last_day_of_month', 'days_in_month',
        'quarter_number', 'quarter_name', 'year_quarter_number', 'year_quarter_name',
        'first_day_of_quarter', 'last_day_of_quarter', 'first_day_of_year', 'last_day_of_year',
        'is_leap_year', 'is_weekday', 'is_weekend', 'is_holiday', 'holiday_name', 'is_workday',
        'day_suffix', 'week_ending_date_saturday', 'week_ending_date_sunday',
        'fiscal_year_number', 'fiscal_quarter_number', 'fiscal_month_offset'
    ]
    
    # Select columns in the desired order, including any not explicitly listed
    final_columns = [col for col in column_order if col in df.columns]
    # Add any remaining columns that might have been created but not in column_order
    remaining_columns = [col for col in df.columns if col not in final_columns]
    df_ordered = df.select(final_columns + remaining_columns)

    return df_ordered

#dagster
@dg.asset(
    name="generate_date_dim_load_to_gcs",
    kinds={"python","duckdb", "gcs"},
    group_name="date_dimension_generation_and_load",
    deps=["create_bq_datasets"],
    partitions_def=default_partition_definition,
)
def generate_date_dim(context: dg.AssetExecutionContext, gcs: GCSResource) -> dg.MaterializeResult:
    """
        Generate data for populating a Date dimension table, including holidays for a respective
        country/province, as a Polars dataframe and then write the dataframe to GCS as a Parquet file.

        Configuration (date range, locality for holidays, GCS destination, etc.) is done in the asset's code itself atm.
    """

    # configuration, can be made to take an input file (e.g. YAML) 
    # or make it take ENV VARS, or you can just edit this
    START_YEAR = 2023
    END_YEAR = 2026
    TARGET_COUNTRY = 'CA'
    TARGET_PROVINCE = 'BC'
    TARGET_FOLDER = "datalake_raw/date_dimension"
    
    start_date_input = f"{START_YEAR}-01-01"
    end_date_input = f"{END_YEAR}-12-31"
    
    context.log.info(f"Generating Date Dimension using Polars from {start_date_input} to {end_date_input} for {TARGET_COUNTRY} (Province: {TARGET_PROVINCE or 'N/A'})...")
    try:
        date_dimension_df = generate_date_dimension_polars(
            start_date_input, 
            end_date_input, 
            country_code=TARGET_COUNTRY, 
            province_code=TARGET_PROVINCE
        )
        context.log.info(f"Generated {len(date_dimension_df)} date entries.")

        output_parquet_file = f"dim_date_polars_{TARGET_COUNTRY}_{TARGET_PROVINCE if TARGET_PROVINCE else 'ALL'}_{START_YEAR}_{END_YEAR}.parquet"

        GCS_BUCKET_NAME = dg.EnvVar("GCS_INGESTION_BUCKET").get_value()
        destination = f'gs://{GCS_BUCKET_NAME}/{TARGET_FOLDER}/{output_parquet_file}'

        # requires gcsfs installed in the env, polars will try to use it but it's not part of its dependencies
        try:
            date_dimension_df.write_parquet(destination, compression='zstd')
            context.log.info(f"Date Dimension written to {destination}")
        except Exception as e:
            context.log.info(f"An error occurred while writing to GCS: {e}")
            raise dg.Failure(description=f"An error occurred while writing to GCS: {e}")

        #more parameters: https://docs.pola.rs/api/python/dev/reference/api/polars.DataFrame.write_parquet.html

        # optional to also save locally as parquet
        local_output = Path(__file__).parent.joinpath("data", output_parquet_file)
        date_dimension_df.write_parquet(local_output, compression='zstd')
        context.log.info(f"Date Dimension saved to {local_output}")

        # pre-polars, pandas code for Dagster preview:
        # holidays_preview = date_dimension_df.filter(pl.col('is_holiday')).select(["full_date", "holiday_name"])
        # "date_dim_row_count": dg.MetadataValue.int(date_dimension_df.shape[0]),
        # "date_dim_schema": dg.MetadataValue.md(date_dimension_df.schema.to_markdown(index=False)),
        # "date_dim_head_preview": dg.MetadataValue.md(date_dimension_df.head(5).to_markdown(index=False)),
        # "date_dim_tail_preview": dg.MetadataValue.md(date_dimension_df.tail(5).to_markdown(index=False)),
        # "holidays_preview": dg.MetadataValue.md(holidays_preview.to_markdown(index=False)),

    except Exception as e:
        print(f"An error occurred: {e}")

    return dg.MaterializeResult(
        metadata={
            "gcs_uri_list": [destination]
        }
    )