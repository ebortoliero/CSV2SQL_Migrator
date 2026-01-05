using CSV2SQL_Migrator.Application.Jobs;
using CSV2SQL_Migrator.Application.Ports;
using CSV2SQL_Migrator.Application.Services;
using CSV2SQL_Migrator.Infrastructure.Csv;
using CSV2SQL_Migrator.Infrastructure.Persistence;
using CSV2SQL_Migrator.Infrastructure.SqlServer;

var builder = WebApplication.CreateBuilder(args);

// Adicionar Razor Pages
builder.Services.AddRazorPages();

// Registrar serviços
var connectionString = builder.Configuration.GetConnectionString("DefaultConnection") 
    ?? throw new InvalidOperationException("Connection string 'DefaultConnection' not found.");

builder.Services.AddSingleton<IJobRepository>(sp => new JobRepository(connectionString));
builder.Services.AddScoped<ICsvReader, CsvReader>();
builder.Services.AddScoped<ITypeInferenceService, TypeInferenceService>();
builder.Services.AddScoped<ISqlServerService, SqlServerService>();
builder.Services.AddScoped<IBulkInsertService, BulkInsertService>();
builder.Services.AddScoped<CsvFileDiscoveryService>();
builder.Services.AddScoped<MigrationJobProcessor>();
builder.Services.AddSingleton<JobQueueService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<JobQueueService>());

var app = builder.Build();

// Inicializar schema do banco de dados
using (var scope = app.Services.CreateScope())
{
    var jobRepository = scope.ServiceProvider.GetRequiredService<IJobRepository>();
    if (jobRepository is JobRepository repo)
    {
        await repo.InitializeSchemaAsync();
    }
}

// Configurar pipeline
if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error");
    app.UseHsts();
}

app.UseHttpsRedirection();
app.UseStaticFiles();
app.UseRouting();
app.MapRazorPages();

app.Run();
