#!/usr/bin/env python3
import os
import csv
import argparse

def analyze_csv_file(file_path, delimiter=";", check_all_rows=False, problem_line=None):
    """
    Analyze a CSV file to determine its structure.
    Returns the number of columns and sample data.
    """
    try:
        # First, check for multi-line fields by reading the raw file
        with open(file_path, 'r', encoding='utf-8') as f:
            raw_lines = f.readlines()
            for i, line in enumerate(raw_lines, 1):
                if '"' in line and '\n' in line and not line.endswith('\n'):
                    print(f"\n⚠️ CRITICAL: Found multi-line field at line {i}:")
                    print(f"  {line.strip()}")
                    print("  This can cause BigQuery to misinterpret the CSV structure.")
                    print("  Consider replacing newlines within quoted fields with spaces.")
        
        # Now analyze the CSV structure
        with open(file_path, 'r', encoding='utf-8') as f:
            # Try to detect the dialect
            sample = f.read(4096)
            f.seek(0)
            
            # Try to detect the dialect
            try:
                dialect = csv.Sniffer().sniff(sample)
                print(f"Detected delimiter: '{dialect.delimiter}'")
                reader = csv.reader(f, dialect)
            except (csv.Error, UnicodeDecodeError):
                print(f"Using provided delimiter: '{delimiter}'")
                reader = csv.reader(f, delimiter=delimiter)
            
            # Read header and first few rows
            header = next(reader, None)
            rows = []
            problem_rows = []
            expected_columns = len(header) if header else 0
            
            if check_all_rows:
                print("\nChecking all rows for consistency...")
                for i, row in enumerate(reader, 1):
                    # Check for fields with newlines
                    for j, field in enumerate(row):
                        if '\n' in field:
                            print(f"\n⚠️ Row {i}, Column {j}: Contains newline character")
                            print(f"  Value: {field}")
                    
                    if len(row) != expected_columns:
                        problem_rows.append((i, row))
                        if len(problem_rows) <= 10:  # Only show first 10 problem rows
                            print(f"  ⚠️ Row {i}: Expected {expected_columns} columns, found {len(row)}")
                    if problem_line and i == problem_line:
                        print(f"\n🔍 Examining problem line {problem_line}:")
                        print(f"  Content: {row}")
                        print(f"  Column count: {len(row)}")
                
                if problem_rows:
                    print(f"\n⚠️ Found {len(problem_rows)} rows with inconsistent column counts")
                else:
                    print(f"\n✅ All rows have consistent column count ({expected_columns})")
            else:
                # Just read a few sample rows
                for i, row in enumerate(reader):
                    # Check for fields with newlines
                    for j, field in enumerate(row):
                        if '\n' in field:
                            print(f"\n⚠️ Row {i+1}, Column {j}: Contains newline character")
                            print(f"  Value: {field}")
                    
                    rows.append(row)
                    if i >= 4:  # Only read first 5 rows
                        break
                
                if header:
                    print(f"\nFound {len(header)} columns in header:")
                    for i, col in enumerate(header):
                        print(f"  {i}: '{col}'")
                
                # Check if sample rows have the same number of columns
                row_lengths = [len(row) for row in rows]
                if len(set(row_lengths)) > 1:
                    print("\n⚠️ WARNING: Inconsistent number of columns in sample rows!")
                    for i, row in enumerate(rows):
                        print(f"  Row {i+1}: {len(row)} columns")
                
                # Print sample data
                print("\nSample data (first 5 rows):")
                for i, row in enumerate(rows):
                    print(f"  Row {i+1}: {row}")
            
            return True
    except Exception as e:
        print(f"Error analyzing CSV file: {str(e)}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Analyze CSV file structure and compare with schema")
    parser.add_argument("csv_file", help="Path to the CSV file to analyze")
    parser.add_argument("-d", "--delimiter", default=";", help="CSV delimiter (default: ;)")
    parser.add_argument("-a", "--all-rows", action="store_true", help="Check all rows for consistency")
    parser.add_argument("-l", "--line", type=int, help="Specific line number to examine")
    
    args = parser.parse_args()
    
    if not os.path.isfile(args.csv_file):
        print(f"Error: CSV file '{args.csv_file}' does not exist!")
        return False
    
    print(f"Analyzing CSV file: {args.csv_file}")
    analyze_csv_file(args.csv_file, args.delimiter, args.all_rows, args.line)
    
    return True

if __name__ == "__main__":
    main()
