using CSV2SQL_Migrator.Domain.Models;

namespace CSV2SQL_Migrator.Application.Ports;

/// <summary>
/// Interface para persistência de Jobs e entidades relacionadas.
/// RF16, RF17, RF18, RF19, RF23: Persistência de Jobs, métricas e erros
/// </summary>
public interface IJobRepository
{
    // Jobs
    Task<int> CreateJobAsync(Job job);
    Task<Job?> GetJobByIdAsync(int jobId);
    Task UpdateJobAsync(Job job);
    Task<List<Job>> GetAllJobsAsync();

    // JobFiles
    Task<int> CreateJobFileAsync(JobFile jobFile);
    Task<JobFile?> GetJobFileByIdAsync(int jobFileId);
    Task UpdateJobFileAsync(JobFile jobFile);
    Task<List<JobFile>> GetJobFilesByJobIdAsync(int jobId);

    // JobErrors
    Task<int> CreateJobErrorAsync(JobError error);
    Task<List<JobError>> GetJobErrorsByJobIdAsync(int jobId);
    Task<List<JobError>> GetJobErrorsByJobFileIdAsync(int jobFileId);

    // JobMetrics
    Task<int> CreateJobMetricAsync(JobMetric metric);
    Task<List<JobMetric>> GetJobMetricsByJobIdAsync(int jobId);
}

