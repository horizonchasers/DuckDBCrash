# DuckDB.NET Crash Reproduction

This repository contains a minimal example to reproduce a crash in DuckDB.NET when handling large batch sizes during data insertion.

## Issue Description

When attempting to insert a large number of records into a DuckDB database using DuckDB.NET, the application crashes if the batch size is too large. The crash occurs during the processing of the first batch.

## Environment

- .NET 8.0
- DuckDB.NET.Data (latest version as of September 2024)
- Windows 10 (or your specific OS version)

## Steps to Reproduce

1. Clone this repository:
   ```
   git clone https://github.com/your-username/duckdb-crash-reproduction.git
   cd duckdb-crash-reproduction
   ```

2. Ensure you have .NET 8.0 SDK installed.

3. Build the project:
   ```
   dotnet build -c Release
   ```

4. Run the application with different batch sizes:
   ```
   .\DuckDBCrash.exe --batch-size 1000 --input path\to\scanned_files_dump.json
   .\DuckDBCrash.exe --batch-size 1500 --input path\to\scanned_files_dump.json
   ```

## Command-Line Interface

The application accepts the following command-line arguments:

- `--input`: Path to the input JSON file (default: "scanned_files_dump.json")
- `--batch-size`: Number of items to process in each batch (default: 1000)
- `--db-path`: Path to the DuckDB database file (default: "database_[timestamp].db")

## Code Overview

The main logic is in the `SimplifiedFileAnalyzer` class. It performs the following steps:

1. Initializes a DuckDB database
2. Loads sample data from a JSON file
3. Inserts the data into the database using an appender

The crash occurs during the data insertion phase when the batch size is too large.

## Observed Behavior

- With a batch size of 1000:
  - The insertion completes successfully.
  - 9 batches are processed for 8841 files.

- With a batch size of 1500:
  - The application crashes during the processing of the first batch.

## Error Message

When the crash occurs with a batch size of 1500, the application exits abruptly without a specific error message. The last output is:

```
Processing batch 1/6...
```

## Possible Cause

The issue appears to be related to memory management in either the DuckDB.NET wrapper or the underlying DuckDB library when handling large batch sizes.

## Workaround

For now, limiting the batch size to 1000 or less allows the application to run without crashing. However, this is not a solution to the underlying problem.

## Additional Notes

- The issue is reproducible and occurs consistently with batch sizes of 1500 or more.
- The application uses a command-line interface for easy testing with different parameters.

## Next Steps

If you're experiencing this issue:

1. Try reducing your batch size to 1000 or less as a temporary workaround.
2. Check the [DuckDB.NET issues page](https://github.com/Giorgi/DuckDB.NET/issues) to see if this has been reported or if there are any updates.
3. If the issue hasn't been reported, please create a new issue with the details of your reproduction case.

## Contributing

If you have any insights into this issue or have found additional ways to reproduce or mitigate it, please feel free to open an issue or submit a pull request.
