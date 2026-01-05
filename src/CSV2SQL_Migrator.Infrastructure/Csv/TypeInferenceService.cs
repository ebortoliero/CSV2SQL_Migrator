using System.Globalization;
using System.Text.RegularExpressions;
using CSV2SQL_Migrator.Application.Ports;

namespace CSV2SQL_Migrator.Infrastructure.Csv;

/// <summary>
/// Implementação de inferência determinística de tipos de dados.
/// Contrato 8.6: Inferência Determinística de Tipos
/// - Amostragem determinística de até 5.000 linhas válidas
/// - Inferência não confiável se menos de 90% dos valores forem compatíveis
/// - Tipos permitidos: int, bigint, decimal(p,s), datetime, bit, nvarchar(255), nvarchar(max)
/// - Em caso de empate, priorizar tipo mais conservador (nvarchar)
/// </summary>
public class TypeInferenceService : ITypeInferenceService
{
    private const int MaxSampleSize = 5000;
    private const double ReliabilityThreshold = 0.90; // 90%

    public SqlColumnType InferType(IEnumerable<string> columnValues, string columnName)
    {
        var samples = columnValues
            .Take(MaxSampleSize)
            .Where(v => !string.IsNullOrWhiteSpace(v))
            .ToList();

        if (samples.Count == 0)
        {
            // Contrato 8.6: Fallback para tipo genérico quando não confiável
            return new SqlColumnType
            {
                TypeName = "nvarchar(255)",
                IsReliable = false
            };
        }

        // Avaliar cada tipo candidato
        var candidates = new List<TypeCandidate>
        {
            EvaluateBit(samples),
            EvaluateInt(samples),
            EvaluateBigInt(samples),
            EvaluateDecimal(samples),
            EvaluateDateTime(samples),
            EvaluateNVarChar(samples)
        };

        // Filtrar apenas candidatos confiáveis (>= 90%)
        var reliableCandidates = candidates
            .Where(c => c.Reliability >= ReliabilityThreshold)
            .OrderByDescending(c => c.Reliability)
            .ToList();

        if (reliableCandidates.Count == 0)
        {
            // Contrato 8.6: Fallback para tipo genérico quando não confiável
            return new SqlColumnType
            {
                TypeName = "nvarchar(255)",
                IsReliable = false
            };
        }

        // Contrato 8.6: Em caso de empate, priorizar tipo mais conservador (nvarchar)
        var bestCandidate = reliableCandidates
            .OrderByDescending(c => c.Reliability)
            .ThenBy(c => GetTypePriority(c.TypeName))
            .First();

        return new SqlColumnType
        {
            TypeName = bestCandidate.TypeName,
            Precision = bestCandidate.Precision,
            Scale = bestCandidate.Scale,
            IsReliable = true
        };
    }

    private TypeCandidate EvaluateBit(IEnumerable<string> samples)
    {
        var validCount = 0;
        var total = 0;

        foreach (var sample in samples)
        {
            total++;
            var trimmed = sample.Trim().ToLowerInvariant();
            if (trimmed == "0" || trimmed == "1" || 
                trimmed == "true" || trimmed == "false" ||
                trimmed == "sim" || trimmed == "não" ||
                trimmed == "yes" || trimmed == "no")
            {
                validCount++;
            }
        }

        return new TypeCandidate
        {
            TypeName = "bit",
            Reliability = total > 0 ? (double)validCount / total : 0
        };
    }

    private TypeCandidate EvaluateInt(IEnumerable<string> samples)
    {
        var validCount = 0;
        var total = 0;

        foreach (var sample in samples)
        {
            total++;
            if (int.TryParse(sample.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var value))
            {
                validCount++;
            }
        }

        return new TypeCandidate
        {
            TypeName = "int",
            Reliability = total > 0 ? (double)validCount / total : 0
        };
    }

    private TypeCandidate EvaluateBigInt(IEnumerable<string> samples)
    {
        var validCount = 0;
        var total = 0;

        foreach (var sample in samples)
        {
            total++;
            if (long.TryParse(sample.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var value))
            {
                // Verificar se não cabe em int
                if (value < int.MinValue || value > int.MaxValue)
                {
                    validCount++;
                }
                else
                {
                    // Se cabe em int, não é bigint
                    validCount = 0;
                    break;
                }
            }
        }

        return new TypeCandidate
        {
            TypeName = "bigint",
            Reliability = total > 0 ? (double)validCount / total : 0
        };
    }

    private TypeCandidate EvaluateDecimal(IEnumerable<string> samples)
    {
        var validCount = 0;
        var total = 0;
        var maxPrecision = 0;
        var maxScale = 0;

        foreach (var sample in samples)
        {
            total++;
            var trimmed = sample.Trim();
            
            // Tentar parse como decimal
            if (decimal.TryParse(trimmed, NumberStyles.Number, CultureInfo.InvariantCulture, out var value))
            {
                validCount++;
                
                // Calcular precisão e escala
                var parts = trimmed.Split('.');
                var integerPart = parts[0].Replace("-", "").Replace("+", "");
                var decimalPart = parts.Length > 1 ? parts[1] : "";
                
                var precision = integerPart.Length + decimalPart.Length;
                var scale = decimalPart.Length;
                
                maxPrecision = Math.Max(maxPrecision, precision);
                maxScale = Math.Max(maxScale, scale);
            }
        }

        // Ajustar precisão mínima (SQL Server requer pelo menos 1)
        maxPrecision = Math.Max(maxPrecision, 1);
        maxScale = Math.Min(maxScale, maxPrecision);

        return new TypeCandidate
        {
            TypeName = "decimal",
            Precision = maxPrecision > 0 ? maxPrecision : 18,
            Scale = maxScale,
            Reliability = total > 0 ? (double)validCount / total : 0
        };
    }

    private TypeCandidate EvaluateDateTime(IEnumerable<string> samples)
    {
        var validCount = 0;
        var total = 0;
        var dateFormats = new[]
        {
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd",
            "dd/MM/yyyy HH:mm:ss",
            "dd/MM/yyyy",
            "MM/dd/yyyy HH:mm:ss",
            "MM/dd/yyyy",
            "yyyy-MM-ddTHH:mm:ss",
            "yyyy-MM-ddTHH:mm:ss.fff"
        };

        foreach (var sample in samples)
        {
            total++;
            var trimmed = sample.Trim();
            
            if (DateTime.TryParse(trimmed, CultureInfo.InvariantCulture, DateTimeStyles.None, out _) ||
                DateTime.TryParseExact(trimmed, dateFormats, CultureInfo.InvariantCulture, DateTimeStyles.None, out _))
            {
                validCount++;
            }
        }

        return new TypeCandidate
        {
            TypeName = "datetime",
            Reliability = total > 0 ? (double)validCount / total : 0
        };
    }

    private TypeCandidate EvaluateNVarChar(IEnumerable<string> samples)
    {
        var validCount = 0;
        var total = 0;
        var maxLength = 0;

        foreach (var sample in samples)
        {
            total++;
            validCount++; // Qualquer string é válida para nvarchar
            
            var length = sample.Length;
            maxLength = Math.Max(maxLength, length);
        }

        // Contrato 8.9: Tamanho padrão nvarchar(255), usar nvarchar(max) se maior
        var typeName = maxLength > 255 ? "nvarchar(max)" : "nvarchar(255)";

        return new TypeCandidate
        {
            TypeName = typeName,
            Precision = maxLength > 255 ? null : maxLength,
            Reliability = total > 0 ? (double)validCount / total : 0
        };
    }

    private int GetTypePriority(string typeName)
    {
        // Contrato 8.6: Priorizar tipo mais conservador (nvarchar)
        // Menor número = maior prioridade
        return typeName.ToLowerInvariant() switch
        {
            "nvarchar(max)" => 1,
            "nvarchar(255)" => 2,
            "nvarchar" => 3,
            "datetime" => 4,
            "bit" => 5,
            "decimal" => 6,
            "int" => 7,
            "bigint" => 8,
            _ => 99
        };
    }

    private class TypeCandidate
    {
        public string TypeName { get; set; } = string.Empty;
        public int? Precision { get; set; }
        public int? Scale { get; set; }
        public double Reliability { get; set; }
    }
}

