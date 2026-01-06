using CSV2SQL_Migrator.Application.Models;

namespace CSV2SQL_Migrator.Application.Ports;

/// <summary>
/// Interface para operações com SQL Server.
/// RF04: Informar parâmetros de conexão com o banco de dados
/// RF05: Testar conexão com o banco de dados
/// RF14: Criar tabelas SQL automaticamente
/// RF15: Derivar nomes de tabelas de forma determinística
/// </summary>
public interface ISqlServerService
{
    /// <summary>
    /// Testa a conexão com o banco de dados.
    /// </summary>
    Task<ConnectionTestResult> TestConnectionAsync(string connectionString);

    /// <summary>
    /// Cria uma tabela no banco de dados com base no schema inferido.
    /// </summary>
    Task CreateTableAsync(string connectionString, string tableName, Dictionary<string, SqlColumnType> columns);

    /// <summary>
    /// Remove uma tabela do banco de dados.
    /// </summary>
    Task DropTableAsync(string connectionString, string tableName);

    /// <summary>
    /// Verifica se uma tabela existe.
    /// </summary>
    Task<bool> TableExistsAsync(string connectionString, string tableName);

    /// <summary>
    /// Gera um nome de tabela determinístico a partir do nome do arquivo.
    /// Contrato 8.7: Naming Determinístico de Tabelas
    /// </summary>
    string GenerateTableName(string fileName, HashSet<string> existingTableNames);

    /// <summary>
    /// Sanitiza o nome de uma coluna para ser válido em SQL Server.
    /// Contrato 8.8: Sanitização de Nomes de Coluna
    /// </summary>
    string SanitizeColumnName(string columnName, HashSet<string> existingNames);
}

