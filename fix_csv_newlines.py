#!/usr/bin/env python3
import os
import csv
import argparse

def fix_csv_newlines(input_file, output_file=None, delimiter=';'):
    """
    Fix CSV files by replacing newlines within quoted fields with spaces.
    """
    if output_file is None:
        base, ext = os.path.splitext(input_file)
        output_file = f"{base}_fixed{ext}"
    
    print(f"Processing {input_file} -> {output_file}")
    
    # Read the raw file to detect and fix multi-line fields
    with open(input_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Fix quoted fields with newlines
    in_quotes = False
    fixed_content = ""
    
    for char in content:
        if char == '"':
            in_quotes = not in_quotes
            fixed_content += char
        elif char == '\n' and in_quotes:
            fixed_content += ' '  # Replace newline with space
            print("  Replaced a newline within quotes with space")
        else:
            fixed_content += char
    
    # Write the fixed content
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(fixed_content)
    
    # Verify the fix worked by reading with csv module
    with open(output_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=delimiter)
        header = next(reader, None)
        expected_columns = len(header) if header else 0
        
        row_count = 0
        problem_count = 0
        
        for i, row in enumerate(reader, 1):
            row_count += 1
            if len(row) != expected_columns:
                problem_count += 1
                if problem_count <= 5:  # Only show first 5 problems
                    print(f"  ⚠️ Row {i} still has wrong column count: expected {expected_columns}, got {len(row)}")
    
    if problem_count == 0:
        print(f"✅ Success! Fixed CSV has {row_count} rows, all with {expected_columns} columns")
    else:
        print(f"⚠️ Fixed some issues, but {problem_count} rows still have problems")
    
    return output_file

def main():
    parser = argparse.ArgumentParser(description="Fix CSV files with newlines in quoted fields")
    parser.add_argument("input_file", help="Path to the CSV file to fix")
    parser.add_argument("-o", "--output", help="Output file path (default: input_file_fixed.csv)")
    parser.add_argument("-d", "--delimiter", default=";", help="CSV delimiter (default: ;)")
    
    args = parser.parse_args()
    
    if not os.path.isfile(args.input_file):
        print(f"Error: Input file '{args.input_file}' does not exist!")
        return False
    
    fix_csv_newlines(args.input_file, args.output, args.delimiter)
    return True

if __name__ == "__main__":
    main()
