using System.Data;
using System.Globalization;
using Microsoft.Data.SqlClient;
using CSV2SQL_Migrator.Application.Ports;

namespace CSV2SQL_Migrator.Infrastructure.SqlServer;

/// <summary>
/// Implementação de bulk insert usando SqlBulkCopy.
/// AP 3: Estratégia obrigatória de ingestão de dados = streaming + bulk insert
/// </summary>
public class BulkInsertService : IBulkInsertService
{
    public async Task<long> BulkInsertAsync(
        string connectionString,
        string tableName,
        string[] columns,
        SqlColumnType[]? columnTypes,
        IEnumerable<string[]> dataRows,
        Func<string[], int, string, Task> onRowError,
        int batchSize = 1000,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        // Criar DataTable com tipos corretos baseados nos tipos inferidos
        var dataTable = new DataTable();
        for (int i = 0; i < columns.Length; i++)
        {
            var columnName = columns[i];
            var columnType = columnTypes != null && i < columnTypes.Length ? columnTypes[i] : null;
            
            // Determinar o tipo .NET correspondente ao tipo SQL
            Type dotNetType = GetDotNetType(columnType);
            dataTable.Columns.Add(columnName, dotNetType);
        }

        long insertedCount = 0;
        var batch = new List<string[]>();
        var rowIndex = 0;

        foreach (var row in dataRows)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                break;
            }

            batch.Add(row);
            rowIndex++;

            if (batch.Count >= batchSize)
            {
                var inserted = await InsertBatchAsync(connection, tableName, columns, columnTypes, batch, dataTable, onRowError, rowIndex - batch.Count);
                insertedCount += inserted;
                batch.Clear();
                dataTable.Clear();
            }
        }

        // Inserir lote restante
        if (batch.Count > 0)
        {
            var inserted = await InsertBatchAsync(connection, tableName, columns, columnTypes, batch, dataTable, onRowError, rowIndex - batch.Count);
            insertedCount += inserted;
        }

        return insertedCount;
    }

    private async Task<long> InsertBatchAsync(
        SqlConnection connection,
        string tableName,
        string[] columns,
        SqlColumnType[]? columnTypes,
        List<string[]> batch,
        DataTable dataTable,
        Func<string[], int, string, Task> onRowError,
        int startRowIndex)
    {
        // Preencher DataTable
        foreach (var row in batch)
        {
            try
            {
                var dataRow = dataTable.NewRow();
                for (int i = 0; i < Math.Min(columns.Length, row.Length); i++)
                {
                    var value = row[i];
                    var columnType = columnTypes != null && i < columnTypes.Length ? columnTypes[i] : null;
                    
                    // Converter valor para o tipo inferido, ou usar valor padrão se falhar
                    var convertedValue = ConvertValueToType(value, columnType);
                    dataRow[i] = convertedValue;
                }
                dataTable.Rows.Add(dataRow);
            }
            catch (Exception ex)
            {
                await onRowError(row, startRowIndex + batch.IndexOf(row), $"Erro ao preparar linha: {ex.Message}");
            }
        }

        if (dataTable.Rows.Count == 0)
        {
            return 0;
        }

        try
        {
            // Escapar colchetes no nome da tabela para uso seguro no SQL
            var escapedTableName = tableName.Replace("]", "]]");

            using var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, null)
            {
                DestinationTableName = $"[dbo].[{escapedTableName}]",
                BatchSize = batch.Count,
                BulkCopyTimeout = 300 // 5 minutos
            };

            // Mapear colunas (escapar colchetes também)
            foreach (var column in columns)
            {
                var escapedColumn = column.Replace("]", "]]");
                bulkCopy.ColumnMappings.Add(escapedColumn, escapedColumn);
            }

            await bulkCopy.WriteToServerAsync(dataTable);
            return dataTable.Rows.Count;
        }
        catch (Exception ex)
        {
            // Em caso de erro no bulk insert, tentar inserir linha por linha para identificar o problema
            for (int i = 0; i < batch.Count; i++)
            {
                await onRowError(batch[i], startRowIndex + i, $"Erro no bulk insert: {ex.Message}");
            }
            return 0;
        }
    }

    /// <summary>
    /// Converte um valor string para o tipo inferido da coluna.
    /// Regra: Todos os valores inválidos, independente do tipo, são convertidos para NULL.
    /// Retorna o valor no tipo .NET correspondente ao tipo SQL.
    /// </summary>
    private object? ConvertValueToType(string? value, SqlColumnType? columnType)
    {
        // Se não há tipo definido, retornar valor original ou NULL
        if (columnType == null)
        {
            return string.IsNullOrWhiteSpace(value) ? DBNull.Value : value;
        }

        // Se valor está vazio, retornar NULL
        if (string.IsNullOrWhiteSpace(value))
        {
            return DBNull.Value;
        }

        var trimmedValue = value.Trim();
        var typeName = columnType.TypeName.ToLowerInvariant();

        // Tentar converter baseado no tipo inferido
        // Se a conversão falhar, retornar NULL (independente do tipo)
        switch (typeName)
        {
            case "bit":
                var lowerValue = trimmedValue.ToLowerInvariant();
                if (lowerValue == "true" || lowerValue == "1" || lowerValue == "sim" || lowerValue == "yes")
                    return true;
                if (lowerValue == "false" || lowerValue == "0" || lowerValue == "não" || lowerValue == "no")
                    return false;
                // Valor inválido → NULL
                return DBNull.Value;

            case "int":
                if (int.TryParse(trimmedValue, NumberStyles.Integer, CultureInfo.InvariantCulture, out var intValue))
                    return intValue;
                // Valor inválido → NULL
                return DBNull.Value;

            case "bigint":
                if (long.TryParse(trimmedValue, NumberStyles.Integer, CultureInfo.InvariantCulture, out var longValue))
                    return longValue;
                // Valor inválido → NULL
                return DBNull.Value;

            case "decimal":
                if (decimal.TryParse(trimmedValue, NumberStyles.Number, CultureInfo.InvariantCulture, out var decimalValue))
                    return decimalValue;
                // Valor inválido → NULL
                return DBNull.Value;

            case "date":
            case "datetime":
                // Tentar parse como data
                if (DateTime.TryParse(trimmedValue, CultureInfo.InvariantCulture, DateTimeStyles.None, out var dateValue))
                    return dateValue;
                // Valor inválido → NULL
                return DBNull.Value;

            case "nvarchar":
            default:
                // Para nvarchar, retornar valor original (sempre válido como string)
                return trimmedValue;
        }
    }

    /// <summary>
    /// Obtém o tipo .NET correspondente ao tipo SQL inferido.
    /// </summary>
    private Type GetDotNetType(SqlColumnType? columnType)
    {
        if (columnType == null)
            return typeof(string);

        var typeName = columnType.TypeName.ToLowerInvariant();
        return typeName switch
        {
            "bit" => typeof(bool),
            "int" => typeof(int),
            "bigint" => typeof(long),
            "decimal" => typeof(decimal),
            "float" => typeof(double),
            "real" => typeof(float),
            "date" => typeof(DateTime),
            "datetime" => typeof(DateTime),
            "nvarchar" => typeof(string),
            _ => typeof(string)
        };
    }

    private bool IsNumericType(string typeName)
    {
        if (string.IsNullOrEmpty(typeName))
            return false;

        var lowerType = typeName.ToLowerInvariant();
        return lowerType == "int" || 
               lowerType == "bigint" || 
               lowerType == "decimal" ||
               lowerType == "float" ||
               lowerType == "real" ||
               lowerType == "money" ||
               lowerType == "smallmoney" ||
               lowerType == "tinyint" ||
               lowerType == "smallint" ||
               lowerType == "bit";
    }
}

