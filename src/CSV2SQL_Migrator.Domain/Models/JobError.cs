namespace CSV2SQL_Migrator.Domain.Models;

/// <summary>
/// Representa um erro ocorrido durante o processamento.
/// RF16: Registrar erros de processamento
/// RF17: Persistir erros para auditoria
/// </summary>
public class JobError
{
    public int Id { get; set; }
    public int JobId { get; set; }
    public int? JobFileId { get; set; }
    public int? LineNumber { get; set; }
    public string? ColumnName { get; set; }
    public ErrorType ErrorType { get; set; }
    public string Message { get; set; } = string.Empty;
    public DateTime CreatedAt { get; set; }
}

/// <summary>
/// Tipos de erro possíveis.
/// </summary>
public enum ErrorType
{
    StructuralFailure = 0,
    LineError = 1,
    ColumnError = 2,
    DatabaseError = 3,
    Other = 4
}

