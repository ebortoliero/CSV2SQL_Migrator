namespace CSV2SQL_Migrator.Domain.Models;

/// <summary>
/// Representa uma métrica de execução de um Job.
/// RF18: Medir métricas de execução
/// RF19: Calcular percentual de aproveitamento
/// </summary>
public class JobMetric
{
    public int Id { get; set; }
    public int JobId { get; set; }
    public string MetricName { get; set; } = string.Empty;
    public decimal MetricValue { get; set; }
    public DateTime RecordedAt { get; set; }
}

