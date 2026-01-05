using System.Text;
using System.Text.RegularExpressions;
using CSV2SQL_Migrator.Application.Ports;

namespace CSV2SQL_Migrator.Infrastructure.Csv;

/// <summary>
/// Implementação de leitura de CSV em streaming com autodetecção de delimitador e encoding.
/// Contrato 8.1: Autodetecção heurística de delimitador
/// Contrato 8.2: Detecção automática de encoding
/// Contrato 8.3: Cabeçalho obrigatório
/// Contrato 8.4: Tratamento de linhas
/// </summary>
public class CsvReader : ICsvReader
{
    private static readonly char[] DelimiterCandidates = { ';', ',', '\t', '|', ':', ' ' };
    private static readonly string[] MultiCharDelimiters = { "||", ";;" };
    private const int SampleLinesForDelimiterDetection = 10;

    public async Task<string[]> ReadHeaderAsync(string filePath)
    {
        if (!File.Exists(filePath))
        {
            throw new StructuralFailureException($"Arquivo não encontrado: {filePath}");
        }

        if (!HasReadPermission(filePath))
        {
            throw new StructuralFailureException($"Sem permissão de leitura: {filePath}");
        }

        var encoding = await DetectEncodingAsync(filePath);
        var delimiter = await DetectDelimiterAsync(filePath, encoding);

        using var reader = new StreamReader(filePath, encoding);
        var headerLine = await reader.ReadLineAsync();

        if (string.IsNullOrWhiteSpace(headerLine))
        {
            throw new StructuralFailureException($"Cabeçalho inválido ou ausente: {filePath}");
        }

        var header = ParseLine(headerLine, delimiter);
        
        if (header.Length == 0)
        {
            throw new StructuralFailureException($"Cabeçalho vazio: {filePath}");
        }

        return header;
    }

    public async Task<long> ReadLinesAsync(
        string filePath,
        Func<string[], int, Task> onLineRead,
        Func<string, int, string, Task> onError,
        CancellationToken cancellationToken = default)
    {
        if (!File.Exists(filePath))
        {
            throw new StructuralFailureException($"Arquivo não encontrado: {filePath}");
        }

        var encoding = await DetectEncodingAsync(filePath);
        var delimiter = await DetectDelimiterAsync(filePath, encoding);

        using var reader = new StreamReader(filePath, encoding);
        
        // Pular cabeçalho
        var headerLine = await reader.ReadLineAsync();
        if (string.IsNullOrWhiteSpace(headerLine))
        {
            throw new StructuralFailureException($"Cabeçalho inválido ou ausente: {filePath}");
        }

        var expectedColumnCount = ParseLine(headerLine, delimiter).Length;
        long lineNumber = 1; // Começa em 1 porque já lemos o cabeçalho
        long linesRead = 0;

        string? line;
        while ((line = await reader.ReadLineAsync()) != null && !cancellationToken.IsCancellationRequested)
        {
            lineNumber++;

            // Contrato 8.4: Linhas vazias devem ser ignoradas
            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            try
            {
                var fields = ParseLine(line, delimiter);

                // Contrato 8.4: Linhas com número de colunas diferente do cabeçalho são erro de linha
                if (fields.Length != expectedColumnCount)
                {
                    await onError($"Número de colunas incorreto. Esperado: {expectedColumnCount}, Encontrado: {fields.Length}", 
                        (int)lineNumber, filePath);
                    continue;
                }

                await onLineRead(fields, (int)lineNumber);
                linesRead++;
            }
            catch (Exception ex)
            {
                await onError($"Erro ao processar linha: {ex.Message}", (int)lineNumber, filePath);
            }
        }

        return linesRead;
    }

    private async Task<Encoding> DetectEncodingAsync(string filePath)
    {
        // Contrato 8.2: Tentar detecção automática de encoding
        try
        {
            using var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
            var buffer = new byte[Math.Min(4096, fileStream.Length)];
            await fileStream.ReadAsync(buffer, 0, buffer.Length);

            // Tentar detectar BOM
            if (buffer.Length >= 3 && buffer[0] == 0xEF && buffer[1] == 0xBB && buffer[2] == 0xBF)
            {
                return Encoding.UTF8;
            }
            if (buffer.Length >= 2 && buffer[0] == 0xFF && buffer[1] == 0xFE)
            {
                return Encoding.Unicode;
            }
            if (buffer.Length >= 2 && buffer[0] == 0xFE && buffer[1] == 0xFF)
            {
                return Encoding.BigEndianUnicode;
            }

            // Tentar detectar UTF-8 sem BOM
            try
            {
                var utf8String = Encoding.UTF8.GetString(buffer);
                if (IsValidUtf8(buffer))
                {
                    return Encoding.UTF8;
                }
            }
            catch { }

            // Contrato 8.2: Fallback para Windows-1252
            return Encoding.GetEncoding(1252);
        }
        catch
        {
            // Contrato 8.2: Fallback para Windows-1252 em caso de erro
            return Encoding.GetEncoding(1252);
        }
    }

    private bool IsValidUtf8(byte[] buffer)
    {
        try
        {
            var text = Encoding.UTF8.GetString(buffer);
            var bytes = Encoding.UTF8.GetBytes(text);
            return bytes.SequenceEqual(buffer.Take(bytes.Length));
        }
        catch
        {
            return false;
        }
    }

    private async Task<char> DetectDelimiterAsync(string filePath, Encoding encoding)
    {
        // Contrato 8.1: Autodetecção heurística de delimitador
        using var reader = new StreamReader(filePath, encoding);
        
        // Ler primeiras linhas para análise
        var sampleLines = new List<string>();
        for (int i = 0; i < SampleLinesForDelimiterDetection; i++)
        {
            var line = await reader.ReadLineAsync();
            if (line == null) break;
            if (!string.IsNullOrWhiteSpace(line))
            {
                sampleLines.Add(line);
            }
        }

        if (sampleLines.Count == 0)
        {
            throw new StructuralFailureException($"Não foi possível determinar o delimitador: arquivo vazio ou sem linhas válidas");
        }

        // Verificar delimitadores de múltiplos caracteres primeiro
        foreach (var multiDelim in MultiCharDelimiters)
        {
            if (IsConsistentDelimiter(sampleLines, multiDelim))
            {
                return multiDelim[0]; // Retornar primeiro caractere como representante
            }
        }

        // Avaliar delimitadores de caractere único
        var delimiterScores = new Dictionary<char, DelimiterScore>();

        foreach (var candidate in DelimiterCandidates)
        {
            var score = EvaluateDelimiter(sampleLines, candidate);
            if (score.IsValid)
            {
                delimiterScores[candidate] = score;
            }
        }

        if (delimiterScores.Count == 0)
        {
            throw new StructuralFailureException($"Não foi possível determinar o delimitador: nenhum delimitador candidato produziu resultado consistente");
        }

        // Contrato 8.1: Em caso de empate, priorizar menor variância e ordem de prioridade
        var bestDelimiter = delimiterScores
            .OrderByDescending(kvp => kvp.Value.ConsistencyScore)
            .ThenBy(kvp => GetPriority(kvp.Key))
            .First().Key;

        return bestDelimiter;
    }

    private bool IsConsistentDelimiter(List<string> lines, string delimiter)
    {
        if (lines.Count == 0) return false;

        var firstLineColumns = lines[0].Split(new[] { delimiter }, StringSplitOptions.None).Length;
        
        return lines.All(line => 
        {
            var columns = line.Split(new[] { delimiter }, StringSplitOptions.None).Length;
            return columns == firstLineColumns && columns > 1;
        });
    }

    private DelimiterScore EvaluateDelimiter(List<string> lines, char delimiter)
    {
        if (lines.Count == 0) return new DelimiterScore { IsValid = false };

        var columnCounts = new List<int>();
        foreach (var line in lines)
        {
            var fields = ParseLine(line, delimiter);
            if (fields.Length > 1) // Deve ter pelo menos 2 colunas
            {
                columnCounts.Add(fields.Length);
            }
        }

        if (columnCounts.Count == 0) return new DelimiterScore { IsValid = false };

        var avgColumns = columnCounts.Average();
        var variance = columnCounts.Select(c => Math.Pow(c - avgColumns, 2)).Average();
        var consistency = 1.0 / (1.0 + variance); // Maior consistência = menor variância

        return new DelimiterScore
        {
            IsValid = true,
            ConsistencyScore = consistency,
            AverageColumns = avgColumns
        };
    }

    private int GetPriority(char delimiter)
    {
        // Contrato 8.1: Ordem de prioridade: ; , \t | : espaço
        return delimiter switch
        {
            ';' => 1,
            ',' => 2,
            '\t' => 3,
            '|' => 4,
            ':' => 5,
            ' ' => 6,
            _ => 99
        };
    }

    private string[] ParseLine(string line, char delimiter)
    {
        // Contrato 8.4: Todos os campos devem sofrer trim
        return line.Split(delimiter)
            .Select(field => field.Trim())
            .ToArray();
    }

    private bool HasReadPermission(string filePath)
    {
        try
        {
            using var stream = File.OpenRead(filePath);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private class DelimiterScore
    {
        public bool IsValid { get; set; }
        public double ConsistencyScore { get; set; }
        public double AverageColumns { get; set; }
    }
}

