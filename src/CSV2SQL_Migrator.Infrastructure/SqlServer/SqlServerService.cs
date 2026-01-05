using System.Data;
using System.Text.RegularExpressions;
using Microsoft.Data.SqlClient;
using CSV2SQL_Migrator.Application.Ports;

namespace CSV2SQL_Migrator.Infrastructure.SqlServer;

/// <summary>
/// Implementação de operações com SQL Server.
/// Contrato 8.7: Naming Determinístico de Tabelas
/// Contrato 8.8: Sanitização de Nomes de Coluna
/// </summary>
public class SqlServerService : ISqlServerService
{
    public async Task<bool> TestConnectionAsync(string connectionString)
    {
        try
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            return true;
        }
        catch
        {
            return false;
        }
    }

    public async Task CreateTableAsync(string connectionString, string tableName, Dictionary<string, SqlColumnType> columns)
    {
        if (columns.Count == 0)
        {
            throw new ArgumentException("A tabela deve ter pelo menos uma coluna", nameof(columns));
        }

        var columnDefinitions = columns
            .Select(kvp => $"[{kvp.Key}] {kvp.Value.ToSqlDefinition()}")
            .ToList();

        var createTableSql = $@"
            IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[{tableName}]') AND type in (N'U'))
            BEGIN
                CREATE TABLE [dbo].[{tableName}] (
                    {string.Join(",\n                    ", columnDefinitions)}
                )
            END";

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        
        await using var command = new SqlCommand(createTableSql, connection);
        await command.ExecuteNonQueryAsync();
    }

    public async Task DropTableAsync(string connectionString, string tableName)
    {
        var dropTableSql = $@"
            IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[{tableName}]') AND type in (N'U'))
            BEGIN
                DROP TABLE [dbo].[{tableName}]
            END";

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        
        await using var command = new SqlCommand(dropTableSql, connection);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<bool> TableExistsAsync(string connectionString, string tableName)
    {
        var checkTableSql = @"
            SELECT COUNT(*) 
            FROM sys.objects 
            WHERE object_id = OBJECT_ID(N'[dbo].[@TableName]') 
            AND type in (N'U')";

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        
        await using var command = new SqlCommand(checkTableSql.Replace("@TableName", tableName), connection);
        var result = await command.ExecuteScalarAsync();
        return Convert.ToInt32(result) > 0;
    }

    public string GenerateTableName(string fileName, HashSet<string> existingTableNames)
    {
        // Contrato 8.7: Formato TB_<NomeArquivoSemExtensao>
        var nameWithoutExtension = Path.GetFileNameWithoutExtension(fileName);
        var baseName = $"TB_{SanitizeForTableName(nameWithoutExtension)}";

        // Contrato 8.7: Se já existe, aplicar prefixo numérico sequencial
        if (!existingTableNames.Contains(baseName))
        {
            return baseName;
        }

        // Tentar com prefixos numéricos
        for (int i = 1; i <= 99; i++)
        {
            var candidateName = $"{i:D2}_{baseName}";
            if (!existingTableNames.Contains(candidateName))
            {
                return candidateName;
            }
        }

        // Se esgotou as opções, usar timestamp como último recurso
        return $"{DateTime.Now:yyyyMMddHHmmss}_{baseName}";
    }

    public string SanitizeColumnName(string columnName, HashSet<string> existingNames)
    {
        // Contrato 8.8: Substituir espaços e pontuação por _
        var sanitized = Regex.Replace(columnName, @"[^\w]", "_");
        
        // Contrato 8.8: Remover caracteres inválidos para SQL Server
        sanitized = Regex.Replace(sanitized, @"[^a-zA-Z0-9_]", "");
        
        // Contrato 8.8: Colunas sem nome devem ser renomeadas para COL###
        if (string.IsNullOrWhiteSpace(sanitized) || sanitized.All(c => c == '_'))
        {
            sanitized = $"COL{existingNames.Count + 1:D3}";
        }

        // Contrato 8.8: Colunas duplicadas devem receber sufixos _2, _3, etc.
        var finalName = sanitized;
        var suffix = 2;
        while (existingNames.Contains(finalName))
        {
            finalName = $"{sanitized}_{suffix}";
            suffix++;
        }

        return finalName;
    }

    private string SanitizeForTableName(string name)
    {
        // Similar à sanitização de coluna, mas para nomes de tabela
        var sanitized = Regex.Replace(name, @"[^\w]", "_");
        sanitized = Regex.Replace(sanitized, @"[^a-zA-Z0-9_]", "");
        
        if (string.IsNullOrWhiteSpace(sanitized))
        {
            sanitized = $"TABLE_{DateTime.Now:yyyyMMddHHmmss}";
        }

        return sanitized;
    }
}

