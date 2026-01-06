namespace CSV2SQL_Migrator.Application.Models;

/// <summary>
/// Resultado do teste de conexão com o banco de dados.
/// </summary>
public class ConnectionTestResult
{
    public bool Success { get; set; }
    public string? ErrorMessage { get; set; }
    public string? ErrorDetails { get; set; }

    public static ConnectionTestResult CreateSuccess()
    {
        return new ConnectionTestResult { Success = true };
    }

    public static ConnectionTestResult CreateFailure(string errorMessage, string? errorDetails = null)
    {
        return new ConnectionTestResult 
        { 
            Success = false, 
            ErrorMessage = errorMessage,
            ErrorDetails = errorDetails 
        };
    }
}

