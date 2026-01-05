using CSV2SQL_Migrator.Domain.Models;

namespace CSV2SQL_Migrator.Application.Ports;

/// <summary>
/// Interface para leitura de arquivos CSV em streaming.
/// RF02: Descobrir automaticamente arquivos CSV
/// RF09: Processar arquivos CSV de forma independente
/// RF10: Processar registros linha a linha
/// </summary>
public interface ICsvReader
{
    /// <summary>
    /// Lê o cabeçalho do arquivo CSV.
    /// </summary>
    /// <param name="filePath">Caminho do arquivo CSV</param>
    /// <returns>Array de nomes de colunas do cabeçalho</returns>
    /// <exception cref="StructuralFailureException">Se houver falha estrutural</exception>
    Task<string[]> ReadHeaderAsync(string filePath);

    /// <summary>
    /// Lê as linhas do arquivo CSV em streaming.
    /// </summary>
    /// <param name="filePath">Caminho do arquivo CSV</param>
    /// <param name="onLineRead">Callback chamado para cada linha válida lida</param>
    /// <param name="onError">Callback chamado para cada erro de linha</param>
    /// <param name="cancellationToken">Token de cancelamento</param>
    /// <returns>Número de linhas lidas</returns>
    Task<long> ReadLinesAsync(
        string filePath,
        Func<string[], int, Task> onLineRead,
        Func<string, int, string, Task> onError,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Exceção lançada quando ocorre uma falha estrutural.
/// RF11: Interromper processamento apenas por falha estrutural
/// </summary>
public class StructuralFailureException : Exception
{
    public StructuralFailureException(string message) : base(message) { }
    public StructuralFailureException(string message, Exception innerException) : base(message, innerException) { }
}

