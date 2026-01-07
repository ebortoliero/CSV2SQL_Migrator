namespace CSV2SQL_Migrator.Application.Ports;

/// <summary>
/// Interface para operações de bulk insert no SQL Server.
/// AP 3: Estratégia obrigatória de ingestão de dados = streaming + bulk insert
/// </summary>
public interface IBulkInsertService
{
    /// <summary>
    /// Insere dados em lote na tabela especificada.
    /// </summary>
    /// <param name="connectionString">String de conexão</param>
    /// <param name="tableName">Nome da tabela</param>
    /// <param name="columns">Nomes das colunas na ordem correta</param>
    /// <param name="columnTypes">Tipos das colunas na mesma ordem (opcional, para conversão de valores vazios)</param>
    /// <param name="dataRows">Linhas de dados para inserção</param>
    /// <param name="onRowError">Callback chamado quando uma linha falha</param>
    /// <param name="batchSize">Tamanho do lote (padrão: 1000)</param>
    /// <param name="cancellationToken">Token de cancelamento</param>
    /// <returns>Número de linhas inseridas com sucesso</returns>
    Task<long> BulkInsertAsync(
        string connectionString,
        string tableName,
        string[] columns,
        SqlColumnType[]? columnTypes,
        IEnumerable<string[]> dataRows,
        Func<string[], int, string, Task> onRowError,
        int batchSize = 1000,
        CancellationToken cancellationToken = default);
}

