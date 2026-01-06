using System.Data;
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
        IEnumerable<string[]> dataRows,
        Func<string[], int, string, Task> onRowError,
        int batchSize = 1000,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        var dataTable = new DataTable();
        foreach (var columnName in columns)
        {
            dataTable.Columns.Add(columnName, typeof(string));
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
                var inserted = await InsertBatchAsync(connection, tableName, columns, batch, dataTable, onRowError, rowIndex - batch.Count);
                insertedCount += inserted;
                batch.Clear();
                dataTable.Clear();
            }
        }

        // Inserir lote restante
        if (batch.Count > 0)
        {
            var inserted = await InsertBatchAsync(connection, tableName, columns, batch, dataTable, onRowError, rowIndex - batch.Count);
            insertedCount += inserted;
        }

        return insertedCount;
    }

    private async Task<long> InsertBatchAsync(
        SqlConnection connection,
        string tableName,
        string[] columns,
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
                    dataRow[i] = string.IsNullOrEmpty(row[i]) ? DBNull.Value : row[i];
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
}

