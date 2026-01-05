using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using CSV2SQL_Migrator.Application.Jobs;
using CSV2SQL_Migrator.Application.Ports;
using CSV2SQL_Migrator.Domain.Models;

namespace CSV2SQL_Migrator.Web.Pages.Monitoring;

/// <summary>
/// Página de monitoramento de Jobs.
/// RF20: Disponibilizar progresso do Job durante execução
/// RF17: Persistir erros para auditoria (visualização)
/// RF18: Medir métricas de execução (visualização)
/// RF19: Calcular percentual de aproveitamento (visualização)
/// </summary>
public class IndexModel : PageModel
{
    private readonly IJobRepository _jobRepository;
    private readonly MigrationJobProcessor _jobProcessor;
    private readonly JobQueueService _jobQueueService;
    private readonly IConfiguration _configuration;

    public IndexModel(
        IJobRepository jobRepository,
        MigrationJobProcessor jobProcessor,
        JobQueueService jobQueueService,
        IConfiguration configuration)
    {
        _jobRepository = jobRepository;
        _jobProcessor = jobProcessor;
        _jobQueueService = jobQueueService;
        _configuration = configuration;
    }

    public Job? Job { get; set; }
    public List<JobFile> JobFiles { get; set; } = new();
    public List<JobError> JobErrors { get; set; } = new();
    public List<JobMetric> JobMetrics { get; set; } = new();
    public decimal? UtilizationPercentage { get; set; }

    public async Task OnGetAsync(int? jobId)
    {
        if (jobId.HasValue)
        {
            Job = await _jobRepository.GetJobByIdAsync(jobId.Value);
            if (Job != null)
            {
                JobFiles = await _jobRepository.GetJobFilesByJobIdAsync(jobId.Value);
                JobErrors = await _jobRepository.GetJobErrorsByJobIdAsync(jobId.Value);
                JobMetrics = await _jobRepository.GetJobMetricsByJobIdAsync(jobId.Value);

                // RF19: Calcular percentual de aproveitamento
                CalculateUtilizationPercentage();
            }
        }
        else
        {
            // Se não há jobId, redirecionar para lista de jobs
            Response.Redirect("/Jobs");
        }
    }

    private void CalculateUtilizationPercentage()
    {
        // RF19: Calcular percentual de aproveitamento com base nas métricas registradas
        if (JobFiles.Count == 0)
        {
            UtilizationPercentage = null;
            return;
        }

        long totalLinesRead = JobFiles.Sum(f => f.LinesRead);
        long totalLinesInserted = JobFiles.Sum(f => f.LinesInserted);

        if (totalLinesRead == 0)
        {
            UtilizationPercentage = 0;
            return;
        }

        UtilizationPercentage = (decimal)totalLinesInserted / totalLinesRead * 100;
    }

    /// <summary>
    /// RF21: Reprocessar Job completo - cria novo Job preservando histórico.
    /// </summary>
    public async Task<IActionResult> OnPostReprocessJobAsync(int jobId)
    {
        var connectionString = _configuration.GetConnectionString("DefaultConnection")
            ?? throw new InvalidOperationException("Connection string 'DefaultConnection' not found.");

        try
        {
            var newJobId = await _jobProcessor.CreateReprocessJobAsync(jobId, connectionString);
            _jobQueueService.EnqueueJob(newJobId, connectionString);
            return RedirectToPage("/Monitoring/Index", new { jobId = newJobId });
        }
        catch (Exception ex)
        {
            TempData["Error"] = $"Erro ao reprocessar job: {ex.Message}";
            return RedirectToPage("/Monitoring/Index", new { jobId = jobId });
        }
    }

    /// <summary>
    /// RF21-RF22: Reprocessar arquivo específico - cria novo Job e remove/recria tabela.
    /// </summary>
    public async Task<IActionResult> OnPostReprocessFileAsync(int jobId, int jobFileId)
    {
        var connectionString = _configuration.GetConnectionString("DefaultConnection")
            ?? throw new InvalidOperationException("Connection string 'DefaultConnection' not found.");

        try
        {
            var newJobId = await _jobProcessor.CreateReprocessFileJobAsync(jobId, jobFileId, connectionString);
            _jobQueueService.EnqueueJob(newJobId, connectionString);
            return RedirectToPage("/Monitoring/Index", new { jobId = newJobId });
        }
        catch (Exception ex)
        {
            TempData["Error"] = $"Erro ao reprocessar arquivo: {ex.Message}";
            return RedirectToPage("/Monitoring/Index", new { jobId = jobId });
        }
    }
}

