#!/usr/bin/env python3
import os
import json
import csv


def check_csv_and_schema():
    bucket_dir = "./bucket"
    schema_dir = "./schemas"
    issues = []

    # Get all CSV files
    csv_files = [f for f in os.listdir(bucket_dir) if f.endswith(".csv")]

    for csv_file in csv_files:
        csv_path = os.path.join(bucket_dir, csv_file)
        schema_file = os.path.splitext(csv_file)[0] + ".json"
        schema_path = os.path.join(schema_dir, schema_file)

        print(f"\nChecking {csv_file} against {schema_file}...")

        # Check if schema file exists
        if not os.path.exists(schema_path):
            issues.append(f"{csv_file}: Missing schema file")
            continue

        # Read schema
        try:
            with open(schema_path, "r") as f:
                schema = json.load(f)
        except json.JSONDecodeError:
            issues.append(f"{schema_file}: Invalid JSON format")
            continue

        # Read CSV headers
        try:
            with open(csv_path, "r") as f:
                first_line = f.readline().strip()
                # Detect delimiter
                delimiter = "," if "," in first_line and ";" not in first_line else ";"

                # Reset file pointer and read headers
                f.seek(0)
                reader = csv.reader(f, delimiter=delimiter)
                headers = next(reader)

                # Check if there's any data
                try:
                    data_row = next(reader, None)
                    has_data = data_row is not None
                except Exception as e:
                    print(f"Error reading data row: {str(e)}")
                    has_data = False

                # Compare headers with schema
                schema_fields = [field["name"] for field in schema]

                # Check for missing fields in schema
                for header in headers:
                    if header not in schema_fields:
                        issues.append(
                            f"{csv_file}: Header '{header}' not found in schema"
                        )

                # Check for extra fields in schema
                for field in schema_fields:
                    if field not in headers:
                        issues.append(
                            f"{csv_file}: Schema field '{field}' not found in CSV headers"
                        )

                # Check data types if data exists
                if has_data:
                    for i, field in enumerate(schema):
                        if i < len(data_row):
                            field_name = field["name"]
                            field_type = field["type"]
                            value = data_row[i]

                            # Basic type checking
                            if (
                                field_type == "NUMERIC"
                                and value
                                and not value.replace(".", "", 1).isdigit()
                            ):
                                issues.append(
                                    f"{csv_file}: Field '{field_name}' is defined as {field_type} but contains '{value}'"
                                )
                            elif (
                                field_type == "BOOLEAN"
                                and value
                                and value.lower()
                                not in ["true", "false", "t", "f", "0", "1"]
                            ):
                                issues.append(
                                    f"{csv_file}: Field '{field_name}' is defined as {field_type} but contains '{value}'"
                                )

                if not has_data:
                    issues.append(f"{csv_file}: No data rows found")

        except Exception as e:
            issues.append(f"{csv_file}: Error reading file - {str(e)}")

    # Print summary
    print("\n--- SUMMARY OF ISSUES ---")
    if issues:
        for issue in issues:
            print(f"- {issue}")
    else:
        print("No issues found!")


def main():
    """Main entry point for the schema checking functionality."""
    return check_csv_and_schema()


if __name__ == "__main__":
    main()
