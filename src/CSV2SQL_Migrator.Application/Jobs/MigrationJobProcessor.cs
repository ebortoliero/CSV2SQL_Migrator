using System.Collections.Concurrent;
using System.Text;
using CSV2SQL_Migrator.Application.Ports;
using CSV2SQL_Migrator.Application.Services;
using CSV2SQL_Migrator.Domain.Models;

namespace CSV2SQL_Migrator.Application.Jobs;

/// <summary>
/// Processador de Jobs de migração.
/// RF06: Acionar manualmente o início da migração
/// RF07: Criar Job de migração
/// RF08: Gerenciar estado do Job
/// RF09: Processar arquivos CSV de forma independente
/// RF20: Disponibilizar progresso do Job durante execução
/// AP 4.1: Processamento em background
/// </summary>
public class MigrationJobProcessor
{
    private readonly IJobRepository _jobRepository;
    private readonly ICsvReader _csvReader;
    private readonly ITypeInferenceService _typeInferenceService;
    private readonly ISqlServerService _sqlServerService;
    private readonly IBulkInsertService _bulkInsertService;
    private readonly CsvFileDiscoveryService _fileDiscoveryService;
    private readonly SemaphoreSlim _semaphore;

    public MigrationJobProcessor(
        IJobRepository jobRepository,
        ICsvReader csvReader,
        ITypeInferenceService typeInferenceService,
        ISqlServerService sqlServerService,
        IBulkInsertService bulkInsertService,
        CsvFileDiscoveryService fileDiscoveryService,
        int maxConcurrentFiles = 4)
    {
        _jobRepository = jobRepository;
        _csvReader = csvReader;
        _typeInferenceService = typeInferenceService;
        _sqlServerService = sqlServerService;
        _bulkInsertService = bulkInsertService;
        _fileDiscoveryService = fileDiscoveryService;
        _semaphore = new SemaphoreSlim(maxConcurrentFiles, maxConcurrentFiles);
    }

    public async Task<int> CreateJobAsync(string rootFolder, string connectionString)
    {
        // RF01, RF02: Descobrir arquivos CSV
        var csvFiles = _fileDiscoveryService.DiscoverCsvFiles(rootFolder);

        // RF07: Criar Job de migração
        var job = new Job
        {
            CreatedAt = DateTime.UtcNow,
            Status = JobStatus.Created,
            RootFolder = rootFolder,
            TotalFiles = csvFiles.Count,
            ProcessedFiles = 0
        };

        var jobId = await _jobRepository.CreateJobAsync(job);
        job.Id = jobId;

        return jobId;
    }

    /// <summary>
    /// RF21: Criar novo Job para reprocessamento de um Job completo.
    /// RN09: Todo reprocessamento gera um novo Job.
    /// </summary>
    public async Task<int> CreateReprocessJobAsync(int originalJobId, string connectionString)
    {
        var originalJob = await _jobRepository.GetJobByIdAsync(originalJobId);
        if (originalJob == null)
        {
            throw new InvalidOperationException($"Job original {originalJobId} não encontrado");
        }

        // RF21: Criar novo Job preservando histórico
        return await CreateJobAsync(originalJob.RootFolder, connectionString);
    }

    /// <summary>
    /// RF21: Criar novo Job para reprocessamento de um arquivo específico.
    /// RF22: Remover e recriar a tabela SQL correspondente.
    /// RN09: Todo reprocessamento gera um novo Job.
    /// RN10: Reprocessamento exige remoção e recriação da estrutura da tabela.
    /// </summary>
    public async Task<int> CreateReprocessFileJobAsync(int originalJobId, int jobFileId, string connectionString)
    {
        var originalJob = await _jobRepository.GetJobByIdAsync(originalJobId);
        if (originalJob == null)
        {
            throw new InvalidOperationException($"Job original {originalJobId} não encontrado");
        }

        var jobFile = await _jobRepository.GetJobFileByIdAsync(jobFileId);
        if (jobFile == null || jobFile.JobId != originalJobId)
        {
            throw new InvalidOperationException($"Arquivo {jobFileId} não encontrado no Job {originalJobId}");
        }

        // RF22: Remover tabela existente antes de reprocessar
        if (!string.IsNullOrEmpty(jobFile.TableName))
        {
            await _sqlServerService.DropTableAsync(connectionString, jobFile.TableName);
        }

        // RF21: Criar novo Job apenas com o arquivo a ser reprocessado
        var job = new Job
        {
            CreatedAt = DateTime.UtcNow,
            Status = JobStatus.Created,
            RootFolder = originalJob.RootFolder,
            TotalFiles = 1, // Apenas um arquivo será reprocessado
            ProcessedFiles = 0
        };

        var jobId = await _jobRepository.CreateJobAsync(job);
        job.Id = jobId;

        // Armazenar o caminho do arquivo a ser reprocessado em um JobFile pendente
        // Isso será usado pelo ProcessJobAsync para processar apenas este arquivo
        await _jobRepository.CreateJobFileAsync(new JobFile
        {
            JobId = jobId,
            FilePath = jobFile.FilePath,
            Status = JobFileStatus.Pending,
            TableName = jobFile.TableName, // Preservar nome da tabela para recriar
            LinesRead = 0,
            LinesInserted = 0,
            LinesRejected = 0
        });

        return jobId;
    }

    /// <summary>
    /// Processa um Job que contém apenas um arquivo específico para reprocessamento.
    /// </summary>
    public async Task ProcessSingleFileJobAsync(int jobId, string filePath, string connectionString, CancellationToken cancellationToken = default)
    {
        var job = await _jobRepository.GetJobByIdAsync(jobId);
        if (job == null)
        {
            throw new InvalidOperationException($"Job {jobId} não encontrado");
        }

        // RF08: Atualizar estado do Job
        job.Status = JobStatus.Running;
        job.StartedAt = DateTime.UtcNow;
        await _jobRepository.UpdateJobAsync(job);

        try
        {
            // RF09: Processar apenas o arquivo específico
            await ProcessFileAsync(jobId, filePath, connectionString, cancellationToken);

            // RF08: Finalizar Job
            job.Status = JobStatus.Completed;
            job.FinishedAt = DateTime.UtcNow;
            await _jobRepository.UpdateJobAsync(job);

            // RF19: Calcular e registrar percentual de aproveitamento
            var jobFiles = await _jobRepository.GetJobFilesByJobIdAsync(jobId);
            if (jobFiles.Count > 0)
            {
                long totalLinesRead = jobFiles.Sum(f => f.LinesRead);
                long totalLinesInserted = jobFiles.Sum(f => f.LinesInserted);

                if (totalLinesRead > 0)
                {
                    decimal utilizationPercentage = (decimal)totalLinesInserted / totalLinesRead * 100;
                    await _jobRepository.CreateJobMetricAsync(new JobMetric
                    {
                        JobId = jobId,
                        MetricName = "UtilizationPercentage",
                        MetricValue = utilizationPercentage,
                        RecordedAt = DateTime.UtcNow
                    });
                }
            }

            // RF23: Registrar tempo total de execução do Job
            if (job.StartedAt.HasValue)
            {
                var totalExecutionTime = (DateTime.UtcNow - job.StartedAt.Value).TotalSeconds;
                await _jobRepository.CreateJobMetricAsync(new JobMetric
                {
                    JobId = jobId,
                    MetricName = "TotalExecutionTime",
                    MetricValue = (decimal)totalExecutionTime,
                    RecordedAt = DateTime.UtcNow
                });
            }
        }
        catch (Exception ex)
        {
            job.Status = JobStatus.Failed;
            job.FinishedAt = DateTime.UtcNow;
            await _jobRepository.UpdateJobAsync(job);

            await _jobRepository.CreateJobErrorAsync(new JobError
            {
                JobId = jobId,
                ErrorType = ErrorType.Other,
                Message = $"Erro ao processar Job: {ex.Message}",
                CreatedAt = DateTime.UtcNow
            });

            throw;
        }
    }

    public async Task ProcessJobAsync(int jobId, string connectionString, CancellationToken cancellationToken = default)
    {
        var job = await _jobRepository.GetJobByIdAsync(jobId);
        if (job == null)
        {
            throw new InvalidOperationException($"Job {jobId} não encontrado");
        }

        // RF08: Atualizar estado do Job
        job.Status = JobStatus.Running;
        job.StartedAt = DateTime.UtcNow;
        await _jobRepository.UpdateJobAsync(job);

        try
        {
            // RF21: Verificar se é reprocessamento de arquivo específico
            var existingJobFiles = await _jobRepository.GetJobFilesByJobIdAsync(jobId);
            var pendingJobFiles = existingJobFiles.Where(f => f.Status == JobFileStatus.Pending).ToList();

            if (pendingJobFiles.Count == 1 && job.TotalFiles == 1)
            {
                // RF21: Reprocessamento de arquivo específico - processar apenas este arquivo
                await ProcessFileAsync(jobId, pendingJobFiles[0].FilePath, connectionString, cancellationToken);
            }
            else
            {
                // RF01, RF02: Descobrir arquivos CSV
                var csvFiles = _fileDiscoveryService.DiscoverCsvFiles(job.RootFolder);

                // RF09: Processar arquivos CSV de forma independente
                var tasks = csvFiles.Select(filePath => ProcessFileAsync(jobId, filePath, connectionString, cancellationToken));
                await Task.WhenAll(tasks);
            }

            // RF08: Finalizar Job
            job.Status = JobStatus.Completed;
            job.FinishedAt = DateTime.UtcNow;
            await _jobRepository.UpdateJobAsync(job);

            // RF19: Calcular e registrar percentual de aproveitamento
            var jobFiles = await _jobRepository.GetJobFilesByJobIdAsync(jobId);
            if (jobFiles.Count > 0)
            {
                long totalLinesRead = jobFiles.Sum(f => f.LinesRead);
                long totalLinesInserted = jobFiles.Sum(f => f.LinesInserted);

                if (totalLinesRead > 0)
                {
                    decimal utilizationPercentage = (decimal)totalLinesInserted / totalLinesRead * 100;
                    await _jobRepository.CreateJobMetricAsync(new JobMetric
                    {
                        JobId = jobId,
                        MetricName = "UtilizationPercentage",
                        MetricValue = utilizationPercentage,
                        RecordedAt = DateTime.UtcNow
                    });
                }
            }

            // RF23: Registrar tempo total de execução do Job
            if (job.StartedAt.HasValue)
            {
                var totalExecutionTime = (DateTime.UtcNow - job.StartedAt.Value).TotalSeconds;
                await _jobRepository.CreateJobMetricAsync(new JobMetric
                {
                    JobId = jobId,
                    MetricName = "TotalExecutionTime",
                    MetricValue = (decimal)totalExecutionTime,
                    RecordedAt = DateTime.UtcNow
                });
            }
        }
        catch (Exception ex)
        {
            job.Status = JobStatus.Failed;
            job.FinishedAt = DateTime.UtcNow;
            await _jobRepository.UpdateJobAsync(job);

            await _jobRepository.CreateJobErrorAsync(new JobError
            {
                JobId = jobId,
                ErrorType = ErrorType.Other,
                Message = $"Erro ao processar Job: {ex.Message}",
                CreatedAt = DateTime.UtcNow
            });

            throw;
        }
    }

    private async Task ProcessFileAsync(int jobId, string filePath, string connectionString, CancellationToken cancellationToken)
    {
        // RF09: Controle de concorrência por arquivo
        await _semaphore.WaitAsync(cancellationToken);

        try
        {
            // Criar JobFile
            var jobFile = new JobFile
            {
                JobId = jobId,
                FilePath = filePath,
                Status = JobFileStatus.Pending,
                LinesRead = 0,
                LinesInserted = 0,
                LinesRejected = 0
            };

            var jobFileId = await _jobRepository.CreateJobFileAsync(jobFile);
            jobFile.Id = jobFileId;

            jobFile.Status = JobFileStatus.Processing;
            jobFile.StartedAt = DateTime.UtcNow;
            await _jobRepository.UpdateJobFileAsync(jobFile);

            // RNF01: Medir tempo de processamento por arquivo
            var fileStartTime = DateTime.UtcNow;

            try
            {
                // RF12, RF13: Inferir tipos
                var header = await _csvReader.ReadHeaderAsync(filePath);
                var columnTypes = await InferColumnTypesAsync(filePath, header);

                // RF15: Gerar nome de tabela determinístico
                var existingTables = await GetExistingTableNamesAsync(connectionString);
                var tableName = _sqlServerService.GenerateTableName(Path.GetFileName(filePath), existingTables);
                jobFile.TableName = tableName;

                // Sanitizar nomes de colunas
                var sanitizedColumns = new Dictionary<string, SqlColumnType>();
                var existingColumnNames = new HashSet<string>();
                var columnMapping = new Dictionary<int, string>();

                for (int i = 0; i < header.Length; i++)
                {
                    var sanitized = _sqlServerService.SanitizeColumnName(header[i], existingColumnNames);
                    existingColumnNames.Add(sanitized);
                    sanitizedColumns[sanitized] = columnTypes[i];
                    columnMapping[i] = sanitized;
                }

                // RF14: Criar tabela SQL automaticamente
                await _sqlServerService.CreateTableAsync(connectionString, tableName, sanitizedColumns);

                // Processar linhas em streaming
                var dataRows = new List<string[]>();
                long linesRead = 0;
                long linesInserted = 0;
                long linesRejected = 0;

                await _csvReader.ReadLinesAsync(
                    filePath,
                    async (fields, lineNumber) =>
                    {
                        linesRead++;
                        // Mapear campos para colunas sanitizadas
                        var mappedRow = new string[columnMapping.Count];
                        for (int i = 0; i < Math.Min(fields.Length, columnMapping.Count); i++)
                        {
                            mappedRow[i] = fields[i];
                        }
                        dataRows.Add(mappedRow);
                    },
                    async (error, lineNumber, file) =>
                    {
                        linesRejected++;
                        await _jobRepository.CreateJobErrorAsync(new JobError
                        {
                            JobId = jobId,
                            JobFileId = jobFileId,
                            LineNumber = lineNumber,
                            ErrorType = ErrorType.LineError,
                            Message = error,
                            CreatedAt = DateTime.UtcNow
                        });
                    },
                    cancellationToken);

                // RF10: Bulk insert
                if (dataRows.Count > 0)
                {
                    var inserted = await _bulkInsertService.BulkInsertAsync(
                        connectionString,
                        tableName,
                        columnMapping.Values.ToArray(),
                        dataRows,
                        async (row, index, error) =>
                        {
                            linesRejected++;
                            await _jobRepository.CreateJobErrorAsync(new JobError
                            {
                                JobId = jobId,
                                JobFileId = jobFileId,
                                LineNumber = index,
                                ErrorType = ErrorType.DatabaseError,
                                Message = error,
                                CreatedAt = DateTime.UtcNow
                            });
                        },
                        batchSize: 1000,
                        cancellationToken);

                    linesInserted = inserted;
                }

                jobFile.LinesRead = linesRead;
                jobFile.LinesInserted = linesInserted;
                jobFile.LinesRejected = linesRejected;
                jobFile.Status = JobFileStatus.Completed;
                jobFile.FinishedAt = DateTime.UtcNow;

                // RNF01: Registrar tempo de processamento por arquivo
                var fileProcessingTime = (DateTime.UtcNow - fileStartTime).TotalSeconds;
                await _jobRepository.CreateJobMetricAsync(new JobMetric
                {
                    JobId = jobId,
                    MetricName = $"FileProcessingTime_{Path.GetFileName(filePath)}",
                    MetricValue = (decimal)fileProcessingTime,
                    RecordedAt = DateTime.UtcNow
                });

                // Atualizar contador do Job
                var job = await _jobRepository.GetJobByIdAsync(jobId);
                if (job != null)
                {
                    job.ProcessedFiles++;
                    await _jobRepository.UpdateJobAsync(job);
                }
            }
            catch (StructuralFailureException ex)
            {
                // RF11: Falha estrutural interrompe apenas o arquivo
                jobFile.Status = JobFileStatus.Failed;
                jobFile.FinishedAt = DateTime.UtcNow;
                await _jobRepository.CreateJobErrorAsync(new JobError
                {
                    JobId = jobId,
                    JobFileId = jobFileId,
                    ErrorType = ErrorType.StructuralFailure,
                    Message = $"Falha estrutural: {ex.Message}",
                    CreatedAt = DateTime.UtcNow
                });
            }
            catch (Exception ex)
            {
                jobFile.Status = JobFileStatus.Failed;
                jobFile.FinishedAt = DateTime.UtcNow;
                await _jobRepository.CreateJobErrorAsync(new JobError
                {
                    JobId = jobId,
                    JobFileId = jobFileId,
                    ErrorType = ErrorType.Other,
                    Message = $"Erro ao processar arquivo: {ex.Message}",
                    CreatedAt = DateTime.UtcNow
                });
            }
            finally
            {
                await _jobRepository.UpdateJobFileAsync(jobFile);
            }
        }
        finally
        {
            _semaphore.Release();
        }
    }

    private async Task<SqlColumnType[]> InferColumnTypesAsync(string filePath, string[] header)
    {
        // Contrato 8.6: Amostragem determinística de até 5.000 linhas
        var samples = new Dictionary<int, List<string>>();
        for (int i = 0; i < header.Length; i++)
        {
            samples[i] = new List<string>();
        }

        int sampleCount = 0;
        await _csvReader.ReadLinesAsync(
            filePath,
            async (fields, lineNumber) =>
            {
                if (sampleCount >= 5000) return;
                for (int i = 0; i < Math.Min(fields.Length, header.Length); i++)
                {
                    if (samples.ContainsKey(i))
                    {
                        samples[i].Add(fields[i]);
                    }
                }
                sampleCount++;
            },
            async (error, lineNumber, file) => { },
            CancellationToken.None);

        var columnTypes = new SqlColumnType[header.Length];
        for (int i = 0; i < header.Length; i++)
        {
            var columnName = header[i];
            var values = samples.ContainsKey(i) ? samples[i] : new List<string>();
            columnTypes[i] = _typeInferenceService.InferType(values, columnName);
        }

        return columnTypes;
    }

    private async Task<HashSet<string>> GetExistingTableNamesAsync(string connectionString)
    {
        // Implementação simplificada - buscar tabelas existentes
        // Em produção, isso deveria consultar o banco
        return new HashSet<string>();
    }
}

