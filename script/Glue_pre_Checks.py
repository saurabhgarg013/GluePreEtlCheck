import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
import boto3
from etl_utils import (
    load_config,
    check_file_exists,
    check_file_name,
    check_file_size,
    check_file_format_and_headers,
    check_compression,
    find_vendor_config,
    validate_data,
    log_errors
)

# Initialize Glue context
spark = SparkSession.builder.appName("GlueETLValidation").getOrCreate()
glueContext = GlueContext(spark)
spark.sparkContext.setLogLevel("ERROR")

def main():
    # Get job arguments
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'input_path', 'config_path', 'output_path', 'error_log_path'])
    input_path = args['input_path']
    config_path = args['config_path']
    output_path = args['output_path']
    error_log_path = args['error_log_path']

    # Initialize S3 client
    s3_client = boto3.client('s3')

    # Load configuration
    config = load_config(s3_client, config_path)

    # File-level validations
    errors = [{"file_path": input_path, "errors": []}]

    # Find vendor config
    vendor_config, error = find_vendor_config(input_path, config)
    if error:
        errors[0]["errors"].append(error)
        log_errors(spark, errors, error_log_path)
        raise Exception(f"File validation failed: {error}")

    # Check file existence
    is_valid, error = check_file_exists(s3_client, input_path)
    if not is_valid:
        errors[0]["errors"].append(error)
        log_errors(spark, errors, error_log_path)
        raise Exception(f"File validation failed: {error}")

    # Check file name
    is_valid, error = check_file_name(input_path, vendor_config)
    if not is_valid:
        errors[0]["errors"].append(error)
        log_errors(spark, errors, error_log_path)
        raise Exception(f"File validation failed: {error}")

    # Check file size
    is_valid, error = check_file_size(s3_client, input_path)
    if not is_valid:
        errors[0]["errors"].append(error)
        log_errors(spark, errors, error_log_path)
        raise Exception(f"File validation failed: {error}")

    # Check file format and headers
    is_valid, df, error = check_file_format_and_headers(spark, input_path, vendor_config)
    if not is_valid:
        errors[0]["errors"].append(error)
        log_errors(spark, errors, error_log_path)
        raise Exception(f"File validation failed: {error}")

    # Check compression
    is_valid, error = check_compression(input_path, vendor_config)
    if not is_valid:
        errors[0]["errors"].append(error)
        log_errors(spark, errors, error_log_path)
        raise Exception(f"File validation failed: {error}")

    # Data-level validation
    validated_df = validate_data(df, vendor_config)

    # Write output
    output_df = DynamicFrame.fromDF(validated_df, glueContext, "output_df")
    glueContext.write_dynamic_frame.from_options(
        frame=output_df,
        connection_type="s3",
        connection_options={"path": output_path},
        format="csv",
        format_options={"writeHeader": True}
    )

    # Log any remaining errors
    log_errors(spark, errors, error_log_path)

if __name__ == "__main__":
    main()
