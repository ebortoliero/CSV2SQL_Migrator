using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using CSV2SQL_Migrator.Application.Ports;
using CSV2SQL_Migrator.Application.Jobs;

namespace CSV2SQL_Migrator.Web.Pages.Config;

/// <summary>
/// Página de configuração de migração.
/// RF01: Indicar pasta raiz de arquivos CSV
/// RF04: Informar parâmetros de conexão com o banco de dados
/// RF05: Testar conexão com o banco de dados
/// RF06: Acionar manualmente o início da migração
/// </summary>
public class IndexModel : PageModel
{
    private readonly ISqlServerService _sqlServerService;
    private readonly MigrationJobProcessor _jobProcessor;
    private readonly JobQueueService _jobQueueService;
    private readonly IConfiguration _configuration;

    public IndexModel(
        ISqlServerService sqlServerService,
        MigrationJobProcessor jobProcessor,
        JobQueueService jobQueueService,
        IConfiguration configuration)
    {
        _sqlServerService = sqlServerService;
        _jobProcessor = jobProcessor;
        _jobQueueService = jobQueueService;
        _configuration = configuration;
    }

    [BindProperty]
    public string RootFolder { get; set; } = string.Empty;

    [BindProperty]
    public string Server { get; set; } = string.Empty;

    [BindProperty]
    public string Database { get; set; } = string.Empty;

    [BindProperty]
    public string UserId { get; set; } = string.Empty;

    [BindProperty]
    public string Password { get; set; } = string.Empty;

    [BindProperty]
    public bool IntegratedSecurity { get; set; } = false;

    [BindProperty]
    public bool TrustServerCertificate { get; set; } = false;

    public string? ConnectionTestMessage { get; set; }
    public string? ConnectionTestDetails { get; set; }
    public bool? ConnectionTestSuccess { get; set; }
    public int? CreatedJobId { get; set; }

    public void OnGet()
    {
        // Carregar valores padrão do appsettings se existirem
        var defaultConnection = _configuration.GetConnectionString("DefaultConnection");
        if (!string.IsNullOrEmpty(defaultConnection))
        {
            // Tentar parsear connection string básica (simplificado)
            // Em produção, usar SqlConnectionStringBuilder
        }
    }

    public async Task<IActionResult> OnPostTestConnectionAsync()
    {
        // RF05: Testar conexão com o banco de dados
        if (!ModelState.IsValid)
        {
            return Page();
        }

        var connectionString = BuildConnectionString();
        var result = await _sqlServerService.TestConnectionAsync(connectionString);

        ConnectionTestSuccess = result.Success;

        if (result.Success)
        {
            ConnectionTestMessage = "Conexão testada com sucesso!";
            ConnectionTestDetails = null;
        }
        else
        {
            ConnectionTestMessage = result.ErrorMessage ?? "Falha ao conectar ao banco de dados. Verifique os parâmetros.";
            ConnectionTestDetails = result.ErrorDetails;
        }

        return Page();
    }

    public async Task<IActionResult> OnPostStartMigrationAsync()
    {
        // RF06: Acionar manualmente o início da migração
        // RN02: Validação prévia obrigatória da conexão
        if (!ModelState.IsValid)
        {
            return Page();
        }

        if (string.IsNullOrWhiteSpace(RootFolder))
        {
            ModelState.AddModelError(nameof(RootFolder), "A pasta raiz é obrigatória.");
            return Page();
        }

        var connectionString = BuildConnectionString();
        
        // Testar conexão antes de iniciar
        var connectionResult = await _sqlServerService.TestConnectionAsync(connectionString);
        if (!connectionResult.Success)
        {
            ConnectionTestSuccess = false;
            ConnectionTestMessage = $"Não é possível iniciar a migração. {connectionResult.ErrorMessage}";
            ConnectionTestDetails = connectionResult.ErrorDetails;
            return Page();
        }

        try
        {
            // RF07: Criar Job de migração
            var jobId = await _jobProcessor.CreateJobAsync(RootFolder, connectionString);
            
            // Enfileirar job para processamento
            _jobQueueService.EnqueueJob(jobId, connectionString);
            
            CreatedJobId = jobId;
            ConnectionTestMessage = $"Job #{jobId} criado e iniciado com sucesso!";
            ConnectionTestSuccess = true;

            // Redirecionar para página de monitoramento
            return RedirectToPage("/Monitoring/Index", new { jobId = jobId });
        }
        catch (Exception ex)
        {
            ModelState.AddModelError(string.Empty, $"Erro ao criar job: {ex.Message}");
            ConnectionTestSuccess = false;
            ConnectionTestMessage = $"Erro: {ex.Message}";
            return Page();
        }
    }

    private string BuildConnectionString()
    {
        var builder = new Microsoft.Data.SqlClient.SqlConnectionStringBuilder
        {
            DataSource = Server,
            InitialCatalog = Database
        };

        if (IntegratedSecurity)
        {
            builder.IntegratedSecurity = true;
        }
        else
        {
            builder.UserID = UserId;
            builder.Password = Password;
        }

        // RNF06: Segurança mínima obrigatória - conexões seguras
        builder.Encrypt = true;
        builder.TrustServerCertificate = TrustServerCertificate;

        return builder.ConnectionString;
    }
}

