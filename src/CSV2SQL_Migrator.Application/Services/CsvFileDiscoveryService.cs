namespace CSV2SQL_Migrator.Application.Services;

/// <summary>
/// Serviço para descoberta de arquivos CSV.
/// RF01: Indicar pasta raiz de arquivos CSV
/// RF02: Descobrir automaticamente arquivos CSV
/// RF03: Filtrar arquivos não elegíveis
/// </summary>
public class CsvFileDiscoveryService
{
    /// <summary>
    /// Descobre todos os arquivos CSV na pasta raiz e subpastas.
    /// </summary>
    public List<string> DiscoverCsvFiles(string rootFolder)
    {
        if (!Directory.Exists(rootFolder))
        {
            throw new DirectoryNotFoundException($"Pasta não encontrada: {rootFolder}");
        }

        var csvFiles = new List<string>();

        try
        {
            // RF02: Descobrir automaticamente arquivos CSV incluindo todas as subpastas, sem limitação de profundidade
            var files = Directory.GetFiles(rootFolder, "*.csv", SearchOption.AllDirectories);
            
            // RF03: Filtrar arquivos não elegíveis (já filtrado pela extensão .csv)
            csvFiles.AddRange(files);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Erro ao descobrir arquivos CSV: {ex.Message}", ex);
        }

        return csvFiles;
    }
}

