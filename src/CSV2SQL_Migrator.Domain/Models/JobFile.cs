namespace CSV2SQL_Migrator.Domain.Models;

/// <summary>
/// Representa um arquivo CSV processado dentro de um Job.
/// RF09: Processar arquivos CSV de forma independente
/// </summary>
public class JobFile
{
    public int Id { get; set; }
    public int JobId { get; set; }
    public string FilePath { get; set; } = string.Empty;
    public JobFileStatus Status { get; set; }
    public DateTime? StartedAt { get; set; }
    public DateTime? FinishedAt { get; set; }
    public long LinesRead { get; set; }
    public long LinesInserted { get; set; }
    public long LinesRejected { get; set; }
    public string TableName { get; set; } = string.Empty;
}

/// <summary>
/// Estados possíveis de um arquivo dentro de um Job.
/// </summary>
public enum JobFileStatus
{
    Pending = 0,
    Processing = 1,
    Completed = 2,
    Failed = 3
}

