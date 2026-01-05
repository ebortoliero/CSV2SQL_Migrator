using System.Data;
using Microsoft.Data.SqlClient;
using CSV2SQL_Migrator.Application.Ports;
using CSV2SQL_Migrator.Domain.Models;

namespace CSV2SQL_Migrator.Infrastructure.Persistence;

/// <summary>
/// Implementação de persistência de Jobs usando SQL Server.
/// Schema 7.1: Tabelas Jobs, JobFiles, JobErrors, JobMetrics
/// </summary>
public class JobRepository : IJobRepository
{
    private readonly string _connectionString;

    public JobRepository(string connectionString)
    {
        _connectionString = connectionString;
    }

    public async Task InitializeSchemaAsync()
    {
        var createTablesSql = @"
            -- Tabela Jobs
            IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Jobs]') AND type in (N'U'))
            BEGIN
                CREATE TABLE [dbo].[Jobs] (
                    [Id] INT IDENTITY(1,1) PRIMARY KEY,
                    [CreatedAt] DATETIME NOT NULL,
                    [StartedAt] DATETIME NULL,
                    [FinishedAt] DATETIME NULL,
                    [Status] INT NOT NULL,
                    [RootFolder] NVARCHAR(MAX) NOT NULL,
                    [TotalFiles] INT NOT NULL,
                    [ProcessedFiles] INT NOT NULL
                )
            END

            -- Tabela JobFiles
            IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[JobFiles]') AND type in (N'U'))
            BEGIN
                CREATE TABLE [dbo].[JobFiles] (
                    [Id] INT IDENTITY(1,1) PRIMARY KEY,
                    [JobId] INT NOT NULL,
                    [FilePath] NVARCHAR(MAX) NOT NULL,
                    [Status] INT NOT NULL,
                    [StartedAt] DATETIME NULL,
                    [FinishedAt] DATETIME NULL,
                    [LinesRead] BIGINT NOT NULL DEFAULT 0,
                    [LinesInserted] BIGINT NOT NULL DEFAULT 0,
                    [LinesRejected] BIGINT NOT NULL DEFAULT 0,
                    [TableName] NVARCHAR(255) NOT NULL,
                    FOREIGN KEY ([JobId]) REFERENCES [dbo].[Jobs]([Id])
                )
            END

            -- Tabela JobErrors
            IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[JobErrors]') AND type in (N'U'))
            BEGIN
                CREATE TABLE [dbo].[JobErrors] (
                    [Id] INT IDENTITY(1,1) PRIMARY KEY,
                    [JobId] INT NOT NULL,
                    [JobFileId] INT NULL,
                    [LineNumber] INT NULL,
                    [ColumnName] NVARCHAR(255) NULL,
                    [ErrorType] INT NOT NULL,
                    [Message] NVARCHAR(MAX) NOT NULL,
                    [CreatedAt] DATETIME NOT NULL,
                    FOREIGN KEY ([JobId]) REFERENCES [dbo].[Jobs]([Id]),
                    FOREIGN KEY ([JobFileId]) REFERENCES [dbo].[JobFiles]([Id])
                )
            END

            -- Tabela JobMetrics
            IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[JobMetrics]') AND type in (N'U'))
            BEGIN
                CREATE TABLE [dbo].[JobMetrics] (
                    [Id] INT IDENTITY(1,1) PRIMARY KEY,
                    [JobId] INT NOT NULL,
                    [MetricName] NVARCHAR(255) NOT NULL,
                    [MetricValue] DECIMAL(18,2) NOT NULL,
                    [RecordedAt] DATETIME NOT NULL,
                    FOREIGN KEY ([JobId]) REFERENCES [dbo].[Jobs]([Id])
                )
            END";

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        
        await using var command = new SqlCommand(createTablesSql, connection);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<int> CreateJobAsync(Job job)
    {
        var sql = @"
            INSERT INTO [dbo].[Jobs] ([CreatedAt], [StartedAt], [FinishedAt], [Status], [RootFolder], [TotalFiles], [ProcessedFiles])
            VALUES (@CreatedAt, @StartedAt, @FinishedAt, @Status, @RootFolder, @TotalFiles, @ProcessedFiles);
            SELECT CAST(SCOPE_IDENTITY() AS INT);";

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        
        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@CreatedAt", job.CreatedAt);
        command.Parameters.AddWithValue("@StartedAt", (object?)job.StartedAt ?? DBNull.Value);
        command.Parameters.AddWithValue("@FinishedAt", (object?)job.FinishedAt ?? DBNull.Value);
        command.Parameters.AddWithValue("@Status", (int)job.Status);
        command.Parameters.AddWithValue("@RootFolder", job.RootFolder);
        command.Parameters.AddWithValue("@TotalFiles", job.TotalFiles);
        command.Parameters.AddWithValue("@ProcessedFiles", job.ProcessedFiles);

        var result = await command.ExecuteScalarAsync();
        return Convert.ToInt32(result);
    }

    public async Task<Job?> GetJobByIdAsync(int jobId)
    {
        var sql = @"
            SELECT [Id], [CreatedAt], [StartedAt], [FinishedAt], [Status], [RootFolder], [TotalFiles], [ProcessedFiles]
            FROM [dbo].[Jobs]
            WHERE [Id] = @Id";

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        
        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@Id", jobId);

        await using var reader = await command.ExecuteReaderAsync();
        if (await reader.ReadAsync())
        {
            return new Job
            {
                Id = reader.GetInt32(0),
                CreatedAt = reader.GetDateTime(1),
                StartedAt = reader.IsDBNull(2) ? null : reader.GetDateTime(2),
                FinishedAt = reader.IsDBNull(3) ? null : reader.GetDateTime(3),
                Status = (JobStatus)reader.GetInt32(4),
                RootFolder = reader.GetString(5),
                TotalFiles = reader.GetInt32(6),
                ProcessedFiles = reader.GetInt32(7)
            };
        }

        return null;
    }

    public async Task UpdateJobAsync(Job job)
    {
        var sql = @"
            UPDATE [dbo].[Jobs]
            SET [StartedAt] = @StartedAt,
                [FinishedAt] = @FinishedAt,
                [Status] = @Status,
                [TotalFiles] = @TotalFiles,
                [ProcessedFiles] = @ProcessedFiles
            WHERE [Id] = @Id";

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        
        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@Id", job.Id);
        command.Parameters.AddWithValue("@StartedAt", (object?)job.StartedAt ?? DBNull.Value);
        command.Parameters.AddWithValue("@FinishedAt", (object?)job.FinishedAt ?? DBNull.Value);
        command.Parameters.AddWithValue("@Status", (int)job.Status);
        command.Parameters.AddWithValue("@TotalFiles", job.TotalFiles);
        command.Parameters.AddWithValue("@ProcessedFiles", job.ProcessedFiles);

        await command.ExecuteNonQueryAsync();
    }

    public async Task<List<Job>> GetAllJobsAsync()
    {
        var sql = @"
            SELECT [Id], [CreatedAt], [StartedAt], [FinishedAt], [Status], [RootFolder], [TotalFiles], [ProcessedFiles]
            FROM [dbo].[Jobs]
            ORDER BY [CreatedAt] DESC";

        var jobs = new List<Job>();

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        
        await using var command = new SqlCommand(sql, connection);
        await using var reader = await command.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            jobs.Add(new Job
            {
                Id = reader.GetInt32(0),
                CreatedAt = reader.GetDateTime(1),
                StartedAt = reader.IsDBNull(2) ? null : reader.GetDateTime(2),
                FinishedAt = reader.IsDBNull(3) ? null : reader.GetDateTime(3),
                Status = (JobStatus)reader.GetInt32(4),
                RootFolder = reader.GetString(5),
                TotalFiles = reader.GetInt32(6),
                ProcessedFiles = reader.GetInt32(7)
            });
        }

        return jobs;
    }

    public async Task<int> CreateJobFileAsync(JobFile jobFile)
    {
        var sql = @"
            INSERT INTO [dbo].[JobFiles] ([JobId], [FilePath], [Status], [StartedAt], [FinishedAt], [LinesRead], [LinesInserted], [LinesRejected], [TableName])
            VALUES (@JobId, @FilePath, @Status, @StartedAt, @FinishedAt, @LinesRead, @LinesInserted, @LinesRejected, @TableName);
            SELECT CAST(SCOPE_IDENTITY() AS INT);";

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        
        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@JobId", jobFile.JobId);
        command.Parameters.AddWithValue("@FilePath", jobFile.FilePath);
        command.Parameters.AddWithValue("@Status", (int)jobFile.Status);
        command.Parameters.AddWithValue("@StartedAt", (object?)jobFile.StartedAt ?? DBNull.Value);
        command.Parameters.AddWithValue("@FinishedAt", (object?)jobFile.FinishedAt ?? DBNull.Value);
        command.Parameters.AddWithValue("@LinesRead", jobFile.LinesRead);
        command.Parameters.AddWithValue("@LinesInserted", jobFile.LinesInserted);
        command.Parameters.AddWithValue("@LinesRejected", jobFile.LinesRejected);
        command.Parameters.AddWithValue("@TableName", jobFile.TableName);

        var result = await command.ExecuteScalarAsync();
        return Convert.ToInt32(result);
    }

    public async Task<JobFile?> GetJobFileByIdAsync(int jobFileId)
    {
        var sql = @"
            SELECT [Id], [JobId], [FilePath], [Status], [StartedAt], [FinishedAt], [LinesRead], [LinesInserted], [LinesRejected], [TableName]
            FROM [dbo].[JobFiles]
            WHERE [Id] = @Id";

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        
        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@Id", jobFileId);

        await using var reader = await command.ExecuteReaderAsync();
        if (await reader.ReadAsync())
        {
            return new JobFile
            {
                Id = reader.GetInt32(0),
                JobId = reader.GetInt32(1),
                FilePath = reader.GetString(2),
                Status = (JobFileStatus)reader.GetInt32(3),
                StartedAt = reader.IsDBNull(4) ? null : reader.GetDateTime(4),
                FinishedAt = reader.IsDBNull(5) ? null : reader.GetDateTime(5),
                LinesRead = reader.GetInt64(6),
                LinesInserted = reader.GetInt64(7),
                LinesRejected = reader.GetInt64(8),
                TableName = reader.GetString(9)
            };
        }

        return null;
    }

    public async Task UpdateJobFileAsync(JobFile jobFile)
    {
        var sql = @"
            UPDATE [dbo].[JobFiles]
            SET [Status] = @Status,
                [StartedAt] = @StartedAt,
                [FinishedAt] = @FinishedAt,
                [LinesRead] = @LinesRead,
                [LinesInserted] = @LinesInserted,
                [LinesRejected] = @LinesRejected
            WHERE [Id] = @Id";

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        
        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@Id", jobFile.Id);
        command.Parameters.AddWithValue("@Status", (int)jobFile.Status);
        command.Parameters.AddWithValue("@StartedAt", (object?)jobFile.StartedAt ?? DBNull.Value);
        command.Parameters.AddWithValue("@FinishedAt", (object?)jobFile.FinishedAt ?? DBNull.Value);
        command.Parameters.AddWithValue("@LinesRead", jobFile.LinesRead);
        command.Parameters.AddWithValue("@LinesInserted", jobFile.LinesInserted);
        command.Parameters.AddWithValue("@LinesRejected", jobFile.LinesRejected);

        await command.ExecuteNonQueryAsync();
    }

    public async Task<List<JobFile>> GetJobFilesByJobIdAsync(int jobId)
    {
        var sql = @"
            SELECT [Id], [JobId], [FilePath], [Status], [StartedAt], [FinishedAt], [LinesRead], [LinesInserted], [LinesRejected], [TableName]
            FROM [dbo].[JobFiles]
            WHERE [JobId] = @JobId
            ORDER BY [Id]";

        var jobFiles = new List<JobFile>();

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        
        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@JobId", jobId);

        await using var reader = await command.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            jobFiles.Add(new JobFile
            {
                Id = reader.GetInt32(0),
                JobId = reader.GetInt32(1),
                FilePath = reader.GetString(2),
                Status = (JobFileStatus)reader.GetInt32(3),
                StartedAt = reader.IsDBNull(4) ? null : reader.GetDateTime(4),
                FinishedAt = reader.IsDBNull(5) ? null : reader.GetDateTime(5),
                LinesRead = reader.GetInt64(6),
                LinesInserted = reader.GetInt64(7),
                LinesRejected = reader.GetInt64(8),
                TableName = reader.GetString(9)
            });
        }

        return jobFiles;
    }

    public async Task<int> CreateJobErrorAsync(JobError error)
    {
        var sql = @"
            INSERT INTO [dbo].[JobErrors] ([JobId], [JobFileId], [LineNumber], [ColumnName], [ErrorType], [Message], [CreatedAt])
            VALUES (@JobId, @JobFileId, @LineNumber, @ColumnName, @ErrorType, @Message, @CreatedAt);
            SELECT CAST(SCOPE_IDENTITY() AS INT);";

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        
        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@JobId", error.JobId);
        command.Parameters.AddWithValue("@JobFileId", (object?)error.JobFileId ?? DBNull.Value);
        command.Parameters.AddWithValue("@LineNumber", (object?)error.LineNumber ?? DBNull.Value);
        command.Parameters.AddWithValue("@ColumnName", (object?)error.ColumnName ?? DBNull.Value);
        command.Parameters.AddWithValue("@ErrorType", (int)error.ErrorType);
        command.Parameters.AddWithValue("@Message", error.Message);
        command.Parameters.AddWithValue("@CreatedAt", error.CreatedAt);

        var result = await command.ExecuteScalarAsync();
        return Convert.ToInt32(result);
    }

    public async Task<List<JobError>> GetJobErrorsByJobIdAsync(int jobId)
    {
        var sql = @"
            SELECT [Id], [JobId], [JobFileId], [LineNumber], [ColumnName], [ErrorType], [Message], [CreatedAt]
            FROM [dbo].[JobErrors]
            WHERE [JobId] = @JobId
            ORDER BY [CreatedAt] DESC";

        var errors = new List<JobError>();

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        
        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@JobId", jobId);

        await using var reader = await command.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            errors.Add(new JobError
            {
                Id = reader.GetInt32(0),
                JobId = reader.GetInt32(1),
                JobFileId = reader.IsDBNull(2) ? null : reader.GetInt32(2),
                LineNumber = reader.IsDBNull(3) ? null : reader.GetInt32(3),
                ColumnName = reader.IsDBNull(4) ? null : reader.GetString(4),
                ErrorType = (ErrorType)reader.GetInt32(5),
                Message = reader.GetString(6),
                CreatedAt = reader.GetDateTime(7)
            });
        }

        return errors;
    }

    public async Task<List<JobError>> GetJobErrorsByJobFileIdAsync(int jobFileId)
    {
        var sql = @"
            SELECT [Id], [JobId], [JobFileId], [LineNumber], [ColumnName], [ErrorType], [Message], [CreatedAt]
            FROM [dbo].[JobErrors]
            WHERE [JobFileId] = @JobFileId
            ORDER BY [LineNumber]";

        var errors = new List<JobError>();

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        
        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@JobFileId", jobFileId);

        await using var reader = await command.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            errors.Add(new JobError
            {
                Id = reader.GetInt32(0),
                JobId = reader.GetInt32(1),
                JobFileId = reader.IsDBNull(2) ? null : reader.GetInt32(2),
                LineNumber = reader.IsDBNull(3) ? null : reader.GetInt32(3),
                ColumnName = reader.IsDBNull(4) ? null : reader.GetString(4),
                ErrorType = (ErrorType)reader.GetInt32(5),
                Message = reader.GetString(6),
                CreatedAt = reader.GetDateTime(7)
            });
        }

        return errors;
    }

    public async Task<int> CreateJobMetricAsync(JobMetric metric)
    {
        var sql = @"
            INSERT INTO [dbo].[JobMetrics] ([JobId], [MetricName], [MetricValue], [RecordedAt])
            VALUES (@JobId, @MetricName, @MetricValue, @RecordedAt);
            SELECT CAST(SCOPE_IDENTITY() AS INT);";

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        
        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@JobId", metric.JobId);
        command.Parameters.AddWithValue("@MetricName", metric.MetricName);
        command.Parameters.AddWithValue("@MetricValue", metric.MetricValue);
        command.Parameters.AddWithValue("@RecordedAt", metric.RecordedAt);

        var result = await command.ExecuteScalarAsync();
        return Convert.ToInt32(result);
    }

    public async Task<List<JobMetric>> GetJobMetricsByJobIdAsync(int jobId)
    {
        var sql = @"
            SELECT [Id], [JobId], [MetricName], [MetricValue], [RecordedAt]
            FROM [dbo].[JobMetrics]
            WHERE [JobId] = @JobId
            ORDER BY [RecordedAt]";

        var metrics = new List<JobMetric>();

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        
        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@JobId", jobId);

        await using var reader = await command.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            metrics.Add(new JobMetric
            {
                Id = reader.GetInt32(0),
                JobId = reader.GetInt32(1),
                MetricName = reader.GetString(2),
                MetricValue = reader.GetDecimal(3),
                RecordedAt = reader.GetDateTime(4)
            });
        }

        return metrics;
    }
}

