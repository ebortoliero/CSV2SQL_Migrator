namespace CSV2SQL_Migrator.Domain.Models;

/// <summary>
/// Representa um Job de migração de dados CSV para SQL Server.
/// RF07: Criar Job de migração
/// RF08: Gerenciar estado do Job
/// </summary>
public class Job
{
    public int Id { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime? StartedAt { get; set; }
    public DateTime? FinishedAt { get; set; }
    public JobStatus Status { get; set; }
    public string RootFolder { get; set; } = string.Empty;
    public int TotalFiles { get; set; }
    public int ProcessedFiles { get; set; }
}

/// <summary>
/// Estados possíveis de um Job.
/// </summary>
public enum JobStatus
{
    Created = 0,
    Running = 1,
    Completed = 2,
    Failed = 3,
    Cancelled = 4
}

