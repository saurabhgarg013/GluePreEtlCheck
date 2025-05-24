import re
import boto3
import json
from pyspark.sql.functions import col, lit, concat_ws, when, count, to_date
from pyspark.sql.types import StringType

def load_config(s3_client, config_path):
    """Load JSON configuration from S3."""
    try:
        bucket, key = config_path.replace("s3://", "").split("/", 1)
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return json.loads(response['Body'].read().decode('utf-8'))
    except Exception as e:
        raise Exception(f"Failed to load config from {config_path}: {str(e)}")

def check_file_exists(s3_client, file_path):
    """Check if file exists in S3."""
    try:
        bucket, key = file_path.replace("s3://", "").split("/", 1)
        s3_client.head_object(Bucket=bucket, Key=key)
        return True, None
    except s3_client.exceptions.ClientError:
        return False, f"File does not exist: {file_path}"

def check_file_name(file_path, vendor_config):
    """Validate file name against expected pattern."""
    file_name = file_path.split("/")[-1]
    pattern = vendor_config['file_name_pattern']
    if not re.match(pattern, file_name):
        return False, f"Invalid file name. Expected pattern: {pattern}"
    return True, None

def check_file_size(s3_client, file_path):
    """Check if file size is greater than 0."""
    bucket, key = file_path.replace("s3://", "").split("/", 1)
    response = s3_client.head_object(Bucket=bucket, Key=key)
    if response['ContentLength'] == 0:
        return False, "File size is 0 bytes"
    return True, None

def check_file_format_and_headers(spark, file_path, vendor_config):
    """Check file format and headers."""
    try:
        df = spark.read.option("header", "true").csv(file_path)
        actual_headers = df.columns
        expected_headers = vendor_config['expected_headers']
        
        # Check headers
        if not all(h in actual_headers for h in expected_headers):
            missing = [h for h in expected_headers if h not in actual_headers]
            return False, df, f"Missing headers: {missing}"
        
        # Check for unexpected columns
        unexpected = [h for h in actual_headers if h not in expected_headers]
        if unexpected:
            return False, df, f"Unexpected columns: {unexpected}"
        
        return True, df, None
    except Exception as e:
        return False, None, f"Failed to read file: {str(e)}"

def check_compression(file_path, vendor_config):
    """Check if file compression matches expectation."""
    file_name = file_path.split("/")[-1]
    expected_compression = vendor_config.get('compression', 'none')
    is_gzip = file_name.endswith('.gz')
    if (expected_compression == 'gzip' and not is_gzip) or (expected_compression == 'none' and is_gzip):
        return False, f"Compression mismatch. Expected: {expected_compression}, Found: {'gzip' if is_gzip else 'none'}"
    return True, None

def find_vendor_config(file_path, config):
    """Find matching vendor config for the file."""
    file_name = file_path.split("/")[-1]
    for vendor_config in config:
        if re.match(vendor_config['file_name_pattern'], file_name):
            return vendor_config, None
    return None, f"No matching vendor config for file: {file_name}"

def validate_data(df, vendor_config):
    """Perform data-level validations."""
    errors_col = lit("")  # Initialize exception column
    primary_key = vendor_config['primary_key']
    mandatory_fields = vendor_config['mandatory_fields']
    date_columns = vendor_config.get('date_columns', [])

    # 1. Validate date/time formats
    for date_col in date_columns:
        column = date_col['column']
        format = date_col['format']
        try:
            df = df.withColumn(
                f"date_validation_{column}",
                when(
                    col(column).isNotNull(),
                    when(
                        to_date(col(column), format).isNotNull(),
                        lit(None)
                    ).otherwise(lit(f"{column}: Invalid date format. Expected {format}"))
                ).otherwise(lit(None))
            )
            errors_col = concat_ws("; ", errors_col, col(f"date_validation_{column}"))
        except:
            errors_col = concat_ws("; ", errors_col, lit(f"{column}: Failed to parse date"))

    # 2. Check for duplicate rows
    row_count = df.groupBy(df.columns).count().filter(col("count") > 1)
    if row_count.count() > 0:
        errors_col = concat_ws("; ", errors_col, lit("Duplicate row detected"))

    # 3. Check for duplicate primary key
    pk_counts = df.groupBy(primary_key).count().filter(col("count") > 1)
    if pk_counts.count() > 0:
        errors_col = concat_ws("; ", errors_col, lit(f"Duplicate {primary_key} detected"))

    # 4. Check mandatory fields
    for field in mandatory_fields:
        errors_col = concat_ws(
            "; ",
            errors_col,
            when(col(field).isNull(), lit(f"{field}: Mandatory field is null")).otherwise(lit(None))
        )

    # Add exception column
    df = df.withColumn("exception", errors_col)

    # Drop temporary validation columns
    validation_cols = [c for c in df.columns if c.startswith("date_validation_")]
    df = df.drop(*validation_cols)

    return df

def log_errors(spark, errors, output_path):
    """Log validation errors to S3."""
    if errors:
        error_df = spark.createDataFrame(
            [(e["file_path"], ", ".join(e["errors"])) for e in errors],
            ["file_path", "errors"]
        )
        error_df.write.mode("append").csv(output_path)