import csv
import json
import os
from config_loader import get_config


# Define a mapping for common data types inference
def infer_data_type(column_name):
    # Default to STRING for most fields
    column_name = column_name.lower()

    # Exact date fields (Django DateField)
    if column_name == "date":
        return "DATE"

    # Date-related fields that should be DATE type
    elif any(
        date_term == column_name or column_name.endswith(date_term)
        for date_term in ["birth_date", "birth_day", "hidden_at_dashboard", "hidden_at"]
    ):
        return "DATE"

    # Time fields (for meal times)
    elif column_name in ["breakfast", "lunch", "dinner"]:
        return "TIME"

    # Timestamp/datetime related fields (Django DateTimeField)
    elif any(
        date_term in column_name
        for date_term in [
            "created_at",
            "updated_at",
            "timestamp",
            "last_login",
            "migrated_at",
            "onboarded_at",
            "deleted_at",
            "beta_phase_start",
            "start_phase_one",
            "token_last_updated",
            "date_joined",
        ]
    ):
        return "TIMESTAMP"

    # Boolean fields - only use for very explicit boolean naming patterns
    elif any(
        bool_term == column_name or column_name.startswith(bool_term)
        for bool_term in ["is_", "has_", "shake_"]
    ):
        return "BOOLEAN"

    # Special case for 'active' and 'enabled' only if they're exact matches
    elif column_name in ["active", "enabled"]:
        return "BOOLEAN"

    # Integer fields (Django IntegerField, PositiveSmallIntegerField, etc.)
    elif any(int_term == column_name for int_term in ["gender", "phase", "position"]):
        return "INTEGER"

    # Email fields (Django EmailField)
    elif column_name == "email" or "email" in column_name:
        return "STRING"

    # Relationship fields (Django ForeignKey, ManyToManyField, OneToOneField)
    elif any(
        rel_term in column_name
        for rel_term in ["groups", "permissions", "user_set", "related_"]
    ):
        return "STRING"

    # IDs are always strings in BigQuery for flexibility
    elif column_name == "id" or column_name.endswith("_id"):
        return "STRING"

    # Known problematic fields that should be strings
    elif any(
        term in column_name
        for term in [
            "image",
            "url",
            "path",
            "file",
            "link",
            "flag",
            "token",
            "password",
            "name",
            "address",
        ]
    ):
        return "STRING"

    # Decimal fields (Django DecimalField)
    elif column_name in [
        "count",
        "amount",
        "total",
        "price",
        "quantity",
        "weight",
        "age",
        "height",
        "calories",
        "hip",
        "waist",
        "chest",
        "belly",
        "start_hip",
        "start_waist",
        "start_chest",
        "start_weight",
        "target_weight",
        "quantity",
        "quantity_one",
        "quantity_two",
        "unit_one",
        "unit_two",
        "calorie",
        "carbohydrates",
        "protein",
        "fat",
        "sugar",
        "preparation_quantity",
        "preparation_time",
        "difficulty",
        "baking_time",
        "cooling_time",
        "rest_time",
    ]:
        return "NUMERIC"

    # Integer fields for specific metrics
    elif column_name in [
        "sleep",
        "freetime",
        "work",
        "sports",
    ]:
        return "INTEGER"

    # Default type
    return "STRING"


def get_bigquery_schema(csv_path: str, delimiter=";"):
    with open(csv_path, "r") as f:
        # Try to detect if the file uses semicolons instead of commas
        first_line = f.readline().strip()
        if ";" in first_line and "," not in first_line:
            delimiter = ";"

        # Reset file pointer to beginning
        f.seek(0)

        reader = csv.reader(f, delimiter=delimiter)
        headers = next(reader)

        # Create schema in BigQuery JSON format
        schema = []
        for header in headers:
            header = header.strip()
            if header:  # Skip empty headers
                data_type = infer_data_type(header)
                # Default to NULLABLE for most fields, but make ID fields REQUIRED
                mode = (
                    "REQUIRED"
                    if header.lower() == "id" or header.lower().endswith("_id")
                    else "NULLABLE"
                )

                # Create field schema object
                field_schema = {"name": header, "type": data_type, "mode": mode}

                schema.append(field_schema)

        return schema


def generate_schemas(bucket_path=None, schema_dir=None):
    """Generate BigQuery schemas from CSV files in the specified bucket path."""
    # Load config and set defaults
    config = get_config()
    if not bucket_path:
        bucket_path = config.get_local_bucket_dir()
    if not schema_dir:
        schema_dir = config.get_schemas_dir()
    
    print(f"\nBucket path: {bucket_path}")

    # Check if bucket path exists
    if not os.path.isdir(bucket_path):
        print(f"Error: Directory '{bucket_path}' does not exist!")
        return False

    # Get list of CSV files
    csv_files = [f for f in os.listdir(bucket_path) if f.endswith(".csv")]
    print(f"Processing {len(csv_files)} files")

    if not csv_files:
        print("No CSV files found in the specified directory.")
        return False

    # Create schemas directory if it doesn't exist
    os.makedirs(schema_dir, exist_ok=True)

    # Process each CSV file
    for filename in csv_files:
        csv_path = os.path.join(bucket_path, filename)
        schema = get_bigquery_schema(csv_path)
        print(f"\n{filename}:")

        # Save the schema to a file
        schema_filename = os.path.splitext(filename)[0] + ".json"
        schema_path = os.path.join(schema_dir, schema_filename)

        with open(schema_path, "w") as schema_file:
            json.dump(schema, schema_file, indent=2)

        print(f"Schema saved to {schema_path}")

    return True


if __name__ == "__main__":
    # If run directly, use default paths
    generate_schemas()
