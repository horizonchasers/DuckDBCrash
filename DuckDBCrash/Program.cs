using System.Text.Json;
using DuckDB.NET.Data;

public class Program
{
    public static async Task Main(string[] args)
    {
        string timestamp = DateTime.Now.ToString("yyyyMMddHHmmss");
        string dbPath = $@"F:\src\DuckDBCrash\DuckDBCrash\database_{timestamp}.db";
        string jsonFilePath = @"F:\src\DuckDBCrash\DuckDBCrash\scanned_files_dump.json";

        Console.WriteLine($"Using database: {dbPath}");

        var analyzer = new SimplifiedFileAnalyzer(dbPath);

        try
        {
            analyzer.InitializeDuckDB();
            await analyzer.LoadAndInsertDataAsync(jsonFilePath);
            Console.WriteLine("Data insertion completed successfully.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
            Console.WriteLine($"Stack Trace: {ex.StackTrace}");
        }
        finally
        {
            Console.WriteLine("Application Execution Complete.");
        }
    }
}

public class SimplifiedFileAnalyzer
{
    private readonly string _connectionString;
    private const int BatchSize = 1000;

    public SimplifiedFileAnalyzer(string dbPath)
    {
        _connectionString = $"Data Source={dbPath}";
    }

    public void InitializeDuckDB()
    {
        Console.WriteLine("Initializing DuckDB...");
        using var connection = new DuckDBConnection(_connectionString);
        connection.Open();
        using var command = connection.CreateCommand();

        command.CommandText = @"
            CREATE TABLE IF NOT EXISTS file_info (
                file_path VARCHAR,
                file_name VARCHAR,
                extension_id USMALLINT,
                file_size BIGINT,
                creation_date USMALLINT,
                last_access_date USMALLINT,
                last_write_date USMALLINT,
                attributes INTEGER,
                read_failure BOOLEAN
            );

            CREATE SEQUENCE IF NOT EXISTS file_extensions_id_seq;

            CREATE TABLE IF NOT EXISTS file_extensions (
                extension_id INTEGER PRIMARY KEY DEFAULT nextval('file_extensions_id_seq'),
                extension VARCHAR UNIQUE
            );";
        command.ExecuteNonQuery();
        Console.WriteLine("DuckDB initialized successfully.");
    }

    public async Task LoadAndInsertDataAsync(string jsonFilePath)
    {
        Console.WriteLine($"Loading data from JSON file: {jsonFilePath}");
        var scannedFiles = await LoadScannedFilesFromJsonAsync(jsonFilePath);
        Console.WriteLine($"Loaded {scannedFiles.Count} files from JSON.");
        await BulkInsertScannedFilesAsync(scannedFiles);
    }

    private async Task<List<ScannedFileInfo>> LoadScannedFilesFromJsonAsync(string jsonFilePath)
    {
        using var jsonFile = File.OpenRead(jsonFilePath);
        var options = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        };
        return await JsonSerializer.DeserializeAsync<List<ScannedFileInfo>>(jsonFile, options);
    }

    private async Task BulkInsertScannedFilesAsync(List<ScannedFileInfo> scannedFiles)
    {
        await Task.Run(() =>
        {
            Console.WriteLine("Starting bulk insert...");
            using var connection = new DuckDBConnection(_connectionString);
            connection.Open();
            using var transaction = connection.BeginTransaction();

            try
            {
                // Insert file extensions
                var uniqueExtensions = scannedFiles
                    .Select(f => f.FileExtension)
                    .Where(ext => !string.IsNullOrEmpty(ext))
                    .Distinct()
                    .ToList();

                Console.WriteLine($"Inserting {uniqueExtensions.Count} unique file extensions...");
                var extensionDict = new Dictionary<string, int>();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "INSERT OR IGNORE INTO file_extensions (extension) VALUES (?) RETURNING extension_id";
                    var parameter = command.CreateParameter();
                    command.Parameters.Add(parameter);

                    foreach (var extension in uniqueExtensions)
                    {
                        parameter.Value = extension;
                        var result = command.ExecuteScalar();
                        if (result != null)
                        {
                            extensionDict[extension] = Convert.ToInt32(result);
                        }
                    }
                }
                Console.WriteLine("File extensions inserted successfully.");

                // Insert file info
                var batches = Enumerable.Range(0, (scannedFiles.Count + BatchSize - 1) / BatchSize)
                    .Select(i => scannedFiles.Skip(i * BatchSize).Take(BatchSize).ToList())
                    .ToList();

                Console.WriteLine($"Inserting file info in {batches.Count} batches...");
                for (int i = 0; i < batches.Count; i++)
                {
                    var batch = batches[i];
                    Console.WriteLine($"Processing batch {i + 1}/{batches.Count}...");
                    using var appender = connection.CreateAppender("file_info");
                    foreach (var file in batch)
                    {
                        int extensionId = 0;
                        if (!string.IsNullOrEmpty(file.FileExtension))
                        {
                            extensionDict.TryGetValue(file.FileExtension, out extensionId);
                        }

                        appender.CreateRow()
                            .AppendValue(file.FilePath)
                            .AppendValue(file.FileName)
                            .AppendValue(extensionId)
                            .AppendValue(file.FileSize)
                            .AppendValue(file.CreationDate)
                            .AppendValue(file.LastAccessDate)
                            .AppendValue(file.LastWriteDate)
                            .AppendValue((int)file.Attributes)
                            .AppendValue(file.ReadFailure)
                            .EndRow();
                    }
                    try
                    {
                        appender.Close();
                        Console.WriteLine($"Batch {i + 1}/{batches.Count} processed successfully.");
                    }
                    catch (DivideByZeroException ex)
                    {
                        Console.WriteLine($"DivideByZeroException occurred while closing appender for batch {i + 1}/{batches.Count}.");
                        Console.WriteLine($"Exception details: {ex.Message}");
                        Console.WriteLine($"Stack trace: {ex.StackTrace}");
                        // You might want to add additional error handling or recovery logic here
                    }
                }

                transaction.Commit();
                Console.WriteLine("Bulk insert completed successfully.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error during bulk insert: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
                transaction.Rollback();
                throw;
            }
        });
    }
}

public struct ScannedFileInfo
{
    public string FilePath { get; set; }
    public string FileName { get; set; }
    public string FileExtension { get; set; }
    public long FileSize { get; set; }
    public ushort CreationDate { get; set; }
    public ushort LastAccessDate { get; set; }
    public ushort LastWriteDate { get; set; }
    public FileAttributes Attributes { get; set; }
    public bool ReadFailure { get; set; }
    public int ErrorCode { get; set; }
}