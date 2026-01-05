using Microsoft.AspNetCore.Mvc.RazorPages;
using CSV2SQL_Migrator.Application.Ports;

namespace CSV2SQL_Migrator.Web.Pages.Jobs;

/// <summary>
/// Página de listagem de Jobs.
/// RF20: Disponibilizar progresso do Job durante execução (parcial - lista de jobs)
/// </summary>
public class IndexModel : PageModel
{
    private readonly IJobRepository _jobRepository;

    public IndexModel(IJobRepository jobRepository)
    {
        _jobRepository = jobRepository;
    }

    public List<Domain.Models.Job> Jobs { get; set; } = new();

    public async Task OnGetAsync()
    {
        Jobs = await _jobRepository.GetAllJobsAsync();
    }
}

