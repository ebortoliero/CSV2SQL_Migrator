using System.Data;
using System.Text.RegularExpressions;
using Microsoft.Data.SqlClient;
using CSV2SQL_Migrator.Application.Ports;
using CSV2SQL_Migrator.Application.Models;

namespace CSV2SQL_Migrator.Infrastructure.SqlServer;

/// <summary>
/// Implementação de operações com SQL Server.
/// Contrato 8.7: Naming Determinístico de Tabelas
/// Contrato 8.8: Sanitização de Nomes de Coluna
/// </summary>
public class SqlServerService : ISqlServerService
{
    public async Task<ConnectionTestResult> TestConnectionAsync(string connectionString)
    {
        try
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            return ConnectionTestResult.CreateSuccess();
        }
        catch (SqlException sqlEx)
        {
            // Erros específicos do SQL Server
            var errorMessage = GetSqlErrorMessage(sqlEx);
            var errorDetails = GetSqlErrorDetails(sqlEx);
            return ConnectionTestResult.CreateFailure(errorMessage, errorDetails);
        }
        catch (Exception ex)
        {
            // Outros tipos de erro (network, timeout, etc.)
            var errorMessage = GetGenericErrorMessage(ex);
            var errorDetails = ex.ToString();
            return ConnectionTestResult.CreateFailure(errorMessage, errorDetails);
        }
    }

    private string GetSqlErrorMessage(SqlException sqlEx)
    {
        // Verificar se é erro relacionado a SSL/certificado
        if (sqlEx.Message.Contains("nome principal do destino") || 
            sqlEx.Message.Contains("principal name") ||
            sqlEx.Message.Contains("SSL") ||
            sqlEx.Number == -2146893022)
        {
            return "Erro de certificado SSL: O certificado do servidor não corresponde ao nome do servidor. " +
                   "Marque a opção 'Confiar no certificado do servidor' na configuração de conexão.";
        }

        // Mapear códigos de erro comuns do SQL Server para mensagens amigáveis
        return sqlEx.Number switch
        {
            2 => "Não foi possível conectar ao servidor. Verifique se o servidor está acessível e o nome está correto.",
            53 => "Erro de rede ao conectar ao servidor. Verifique se o servidor está rodando e acessível.",
            18456 => "Falha na autenticação. Verifique o usuário e senha informados.",
            4060 => "Não foi possível abrir o banco de dados. Verifique se o nome do banco está correto e se você tem permissão para acessá-lo.",
            233 => "Não foi possível estabelecer conexão com o servidor. Verifique se o SQL Server está configurado para aceitar conexões remotas.",
            10060 => "Timeout ao conectar ao servidor. O servidor pode estar sobrecarregado ou inacessível.",
            10061 => "Não foi possível conectar ao servidor. O servidor pode não estar rodando ou a porta pode estar bloqueada.",
            _ => $"Erro ao conectar ao banco de dados: {sqlEx.Message}"
        };
    }

    private string GetSqlErrorDetails(SqlException sqlEx)
    {
        var details = new System.Text.StringBuilder();
        details.AppendLine($"Código de erro SQL: {sqlEx.Number}");
        details.AppendLine($"Servidor: {sqlEx.Server}");
        details.AppendLine($"Mensagem original: {sqlEx.Message}");
        
        if (sqlEx.InnerException != null)
        {
            details.AppendLine($"Erro interno: {sqlEx.InnerException.Message}");
        }

        // Adicionar informações sobre o erro específico se disponível
        if (sqlEx.Errors != null && sqlEx.Errors.Count > 0)
        {
            details.AppendLine("\nDetalhes adicionais:");
            foreach (SqlError error in sqlEx.Errors)
            {
                details.AppendLine($"  - Classe: {error.Class}, Estado: {error.State}, Linha: {error.LineNumber}");
                details.AppendLine($"    Mensagem: {error.Message}");
            }
        }

        return details.ToString();
    }

    private string GetGenericErrorMessage(Exception ex)
    {
        return ex switch
        {
            System.Net.Sockets.SocketException => "Erro de rede ao conectar ao servidor. Verifique se o servidor está acessível.",
            TimeoutException => "Timeout ao conectar ao servidor. O servidor pode estar sobrecarregado ou inacessível.",
            ArgumentException => "Parâmetros de conexão inválidos. Verifique a string de conexão.",
            _ => $"Erro ao conectar ao banco de dados: {ex.Message}"
        };
    }

    public async Task CreateTableAsync(string connectionString, string tableName, Dictionary<string, SqlColumnType> columns)
    {
        if (columns.Count == 0)
        {
            throw new ArgumentException("A tabela deve ter pelo menos uma coluna", nameof(columns));
        }

        // Validar e sanitizar o nome da tabela
        if (string.IsNullOrWhiteSpace(tableName))
        {
            throw new ArgumentException("O nome da tabela não pode ser vazio", nameof(tableName));
        }

        // Escapar colchetes no nome da tabela para uso seguro no SQL
        var escapedTableName = tableName.Replace("]", "]]");

        var columnDefinitions = columns
            .Select(kvp => 
            {
                // Escapar colchetes no nome da coluna também
                var escapedColumnName = kvp.Key.Replace("]", "]]");
                return $"[{escapedColumnName}] {kvp.Value.ToSqlDefinition()}";
            })
            .ToList();

        var createTableSql = $@"
            IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[{escapedTableName}]') AND type in (N'U'))
            BEGIN
                CREATE TABLE [dbo].[{escapedTableName}] (
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
        if (string.IsNullOrWhiteSpace(tableName))
        {
            throw new ArgumentException("O nome da tabela não pode ser vazio", nameof(tableName));
        }

        // Escapar colchetes no nome da tabela para uso seguro no SQL
        var escapedTableName = tableName.Replace("]", "]]");

        var dropTableSql = $@"
            IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[{escapedTableName}]') AND type in (N'U'))
            BEGIN
                DROP TABLE [dbo].[{escapedTableName}]
            END";

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        
        await using var command = new SqlCommand(dropTableSql, connection);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<bool> TableExistsAsync(string connectionString, string tableName)
    {
        if (string.IsNullOrWhiteSpace(tableName))
        {
            return false;
        }

        // Escapar colchetes no nome da tabela para uso seguro no SQL
        var escapedTableName = tableName.Replace("]", "]]");

        var checkTableSql = $@"
            SELECT COUNT(*) 
            FROM sys.objects 
            WHERE object_id = OBJECT_ID(N'[dbo].[{escapedTableName}]') 
            AND type in (N'U')";

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        
        await using var command = new SqlCommand(checkTableSql, connection);
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
        if (string.IsNullOrWhiteSpace(columnName))
        {
            return $"COL{existingNames.Count + 1:D3}";
        }

        // Contrato 8.8: Substituir espaços e pontuação por _
        var sanitized = Regex.Replace(columnName, @"[^\w]", "_");
        
        // Contrato 8.8: Remover caracteres inválidos para SQL Server
        sanitized = Regex.Replace(sanitized, @"[^a-zA-Z0-9_]", "");
        
        // Remover underscores múltiplos consecutivos
        sanitized = Regex.Replace(sanitized, @"_+", "_");
        
        // Remover underscores no início e fim
        sanitized = sanitized.Trim('_');
        
        // Contrato 8.8: Colunas sem nome devem ser renomeadas para COL###
        if (string.IsNullOrWhiteSpace(sanitized) || sanitized.All(c => c == '_'))
        {
            sanitized = $"COL{existingNames.Count + 1:D3}";
        }

        // Garantir que não comece com número (SQL Server não permite)
        if (sanitized.Length > 0 && char.IsDigit(sanitized[0]))
        {
            sanitized = $"C_{sanitized}";
        }

        // Limitar tamanho do nome (SQL Server tem limite de 128 caracteres para identificadores)
        if (sanitized.Length > 128)
        {
            sanitized = sanitized.Substring(0, 128);
        }

        // Contrato 8.8: Colunas duplicadas devem receber sufixos _2, _3, etc.
        var finalName = sanitized;
        var suffix = 2;
        while (existingNames.Contains(finalName))
        {
            finalName = $"{sanitized}_{suffix}";
            suffix++;
            
            // Garantir que o nome final não exceda 128 caracteres
            if (finalName.Length > 128)
            {
                var maxBaseLength = 128 - (suffix.ToString().Length + 1);
                finalName = $"{sanitized.Substring(0, Math.Min(sanitized.Length, maxBaseLength))}_{suffix}";
            }
        }

        return finalName;
    }

    private string SanitizeForTableName(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            return $"TABLE_{DateTime.Now:yyyyMMddHHmmss}";
        }

        // Similar à sanitização de coluna, mas para nomes de tabela
        // Remover caracteres especiais e substituir por underscore
        var sanitized = Regex.Replace(name, @"[^\w]", "_");
        
        // Remover caracteres que não são letras, números ou underscore
        sanitized = Regex.Replace(sanitized, @"[^a-zA-Z0-9_]", "");
        
        // Remover underscores múltiplos consecutivos
        sanitized = Regex.Replace(sanitized, @"_+", "_");
        
        // Remover underscores no início e fim
        sanitized = sanitized.Trim('_');
        
        // Garantir que não comece com número (SQL Server não permite)
        if (sanitized.Length > 0 && char.IsDigit(sanitized[0]))
        {
            sanitized = $"T_{sanitized}";
        }
        
        if (string.IsNullOrWhiteSpace(sanitized))
        {
            sanitized = $"TABLE_{DateTime.Now:yyyyMMddHHmmss}";
        }

        // Limitar tamanho do nome (SQL Server tem limite de 128 caracteres para identificadores)
        if (sanitized.Length > 128)
        {
            sanitized = sanitized.Substring(0, 128);
        }

        return sanitized;
    }
}

