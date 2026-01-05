using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using CSV2SQL_Migrator.Application.Ports;

namespace CSV2SQL_Migrator.Application.Jobs;

/// <summary>
/// Serviço de fila para processamento de Jobs em background.
/// AP 4.1: Processamento em background no mesmo host da aplicação
/// </summary>
public class JobQueueService : BackgroundService
{
    private readonly ConcurrentQueue<JobQueueItem> _queue = new();
    private readonly SemaphoreSlim _semaphore = new(0);
    private readonly IServiceProvider _serviceProvider;

    public JobQueueService(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider;
    }

    public void EnqueueJob(int jobId, string connectionString)
    {
        _queue.Enqueue(new JobQueueItem { JobId = jobId, ConnectionString = connectionString });
        _semaphore.Release();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            await _semaphore.WaitAsync(stoppingToken);

            if (_queue.TryDequeue(out var item))
            {
                _ = Task.Run(async () =>
                {
                    using var scope = _serviceProvider.CreateScope();
                    var processor = scope.ServiceProvider.GetRequiredService<MigrationJobProcessor>();
                    await processor.ProcessJobAsync(item.JobId, item.ConnectionString, stoppingToken);
                }, stoppingToken);
            }
        }
    }

    private class JobQueueItem
    {
        public int JobId { get; set; }
        public string ConnectionString { get; set; } = string.Empty;
    }
}

