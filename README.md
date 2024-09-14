# DuckDB.NET Crash Reproduction

This repository contains a minimal example to reproduce a crash in DuckDB.NET when handling large batch sizes during data insertion.

## Issue Description

When attempting to insert a large number of records into a DuckDB database using DuckDB.NET, the application crashes with a heap corruption error (exit code 0xc0000374) if the batch size is too large. The crash occurs during the `appender.Close()` operation.

## Environment

- .NET 8.0
- DuckDB.NET.Data 1.1.0.1 and 1.0.2
- Windows 11 Pro x64

## Steps to Reproduce

1. Clone this repository:
   ```
   git clone https://github.com/your-username/duckdb-crash-reproduction.git
   cd duckdb-crash-reproduction
   ```

2. Ensure you have .NET 8.0 SDK installed.

3. Build the project:
   ```
   dotnet build
   ```

4. Run the application with different batch sizes:
   ```
   dotnet run -- 1000  # Works fine
   dotnet run -- 1500  # Crashes
   ```

## Code Overview

The main logic is in the `SimplifiedFileAnalyzer` class. It performs the following steps:

1. Initializes a DuckDB database
2. Loads sample data from a JSON file
3. Inserts the data into the database using an appender

The crash occurs during the data insertion phase when the batch size is too large.

## Observed Behavior

- With a batch size of 1000 or less, the insertion completes successfully.
- With a batch size of 1500 or more, the application crashes with a heap corruption error.

## Error Message

When the crash occurs, you may see an error message similar to this:

```
F:\src\DuckDBCrash\DuckDBCrash\bin\Debug\net8.0\DuckDBCrash.exe (process 28128) exited with code -1073740940 (0xc0000374).
```

## Possible Cause

The issue appears to be related to memory management in either the DuckDB.NET wrapper or the underlying DuckDB library when handling large batch sizes.

## Workaround

For now, limiting the batch size to 1000 or less allows the application to run without crashing. However, this is not a solution to the underlying problem.

## Additional Notes

- We've also observed a `DivideByZeroException` occurring in the `appender.Close()` method in some cases.
- The issue is reproducible and occurs consistently with larger batch sizes.

## Next Steps

If you're experiencing this issue:

1. Try reducing your batch size as a temporary workaround.
2. Check the [DuckDB.NET issues page](https://github.com/Giorgi/DuckDB.NET/issues) to see if this has been reported or if there are any updates.
3. If the issue hasn't been reported, please create a new issue with the details of your reproduction case.

## Contributing

If you have any insights into this issue or have found additional ways to reproduce or mitigate it, please feel free to open an issue or submit a pull request.