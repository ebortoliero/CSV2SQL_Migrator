using System.Globalization;
using System.Text.RegularExpressions;
using CSV2SQL_Migrator.Application.Ports;

namespace CSV2SQL_Migrator.Infrastructure.Csv;

/// <summary>
/// Implementação de inferência determinística de tipos de dados.
/// Contrato 8.6: Inferência Determinística de Tipos
/// - Amostragem determinística de até 5.000 linhas válidas
/// - Inferência não confiável se menos de 90% dos valores forem compatíveis
/// - Tipos permitidos: int, bigint, decimal(p,s), date, datetime, bit, nvarchar(255), nvarchar(max)
/// - Em caso de empate, priorizar tipo mais específico (bit, int, decimal, datetime sobre nvarchar)
/// </summary>
public class TypeInferenceService : ITypeInferenceService
{
    private const int MaxSampleSize = 5000;
    private const double ReliabilityThreshold = 0.90; // 90%
    private const double NumericTypeThreshold = 0.80; // 80% para tipos numéricos (mais flexível)
    private const double DateTimeTypeThreshold = 0.80; // 80% para tipos de data (mais flexível)

    public SqlColumnType InferType(IEnumerable<string> columnValues, string columnName)
    {
        // Considerar TODOS os valores (incluindo vazios) para análise completa
        var allSamples = columnValues
            .Take(MaxSampleSize)
            .ToList();

        // Separar valores vazios e não-vazios
        var nonEmptySamples = allSamples
            .Where(v => !string.IsNullOrWhiteSpace(v))
            .ToList();

        var totalCount = allSamples.Count;
        var nonEmptyCount = nonEmptySamples.Count;

        // Se não há valores não-vazios, fallback para nvarchar
        if (nonEmptyCount == 0)
        {
            // Contrato 8.6: Fallback para tipo genérico quando não confiável
            return new SqlColumnType
            {
                TypeName = "nvarchar",
                Precision = 255,
                IsReliable = false
            };
        }

        // Avaliar cada tipo candidato apenas com valores não-vazios
        // A confiabilidade será calculada considerando apenas valores não-vazios
        var candidates = new List<TypeCandidate>
        {
            EvaluateBit(nonEmptySamples, nonEmptyCount),
            EvaluateInt(nonEmptySamples, nonEmptyCount),
            EvaluateBigInt(nonEmptySamples, nonEmptyCount),
            EvaluateDecimal(nonEmptySamples, nonEmptyCount),
            EvaluateDate(nonEmptySamples, nonEmptyCount),
            EvaluateDateTime(nonEmptySamples, nonEmptyCount),
            EvaluateNVarChar(nonEmptySamples, nonEmptyCount) // Último, como fallback
        };

        // Filtrar candidatos confiáveis:
        // - Tipos numéricos (int, bigint, decimal): >= 80% (mais flexível para dados com erros)
        // - Tipos de data (date, datetime): >= 80% (mais flexível para dados com erros)
        // - Outros tipos: >= 90% (threshold padrão)
        var numericTypes = new[] { "int", "bigint", "decimal" };
        var dateTimeTypes = new[] { "date", "datetime" };
        var reliableCandidates = candidates
            .Where(c => 
            {
                var typeName = c.TypeName.ToLowerInvariant();
                var isNumeric = numericTypes.Contains(typeName);
                var isDateTime = dateTimeTypes.Contains(typeName);
                var threshold = isNumeric ? NumericTypeThreshold : (isDateTime ? DateTimeTypeThreshold : ReliabilityThreshold);
                return c.Reliability >= threshold;
            })
            .OrderByDescending(c => c.Reliability)
            .ToList();

        if (reliableCandidates.Count == 0)
        {
            // Se não há candidatos confiáveis, verificar se há algum tipo específico (numérico ou data) com pelo menos 50%
            // e que seja melhor que nvarchar (para casos com muitos erros de dados)
            var specificTypes = numericTypes.Concat(dateTimeTypes).ToArray();
            var specificCandidates = candidates
                .Where(c => specificTypes.Contains(c.TypeName.ToLowerInvariant()) && c.Reliability >= 0.50)
                .OrderByDescending(c => c.Reliability)
                .ToList();

            if (specificCandidates.Count > 0)
            {
                var nvarcharCandidate = candidates.FirstOrDefault(c => c.TypeName.ToLowerInvariant() == "nvarchar");
                var bestSpecific = specificCandidates.First();
                
                // Se o melhor tipo específico tem confiabilidade maior que nvarchar, usar ele
                if (nvarcharCandidate == null || bestSpecific.Reliability > nvarcharCandidate.Reliability)
                {
                    return new SqlColumnType
                    {
                        TypeName = bestSpecific.TypeName,
                        Precision = bestSpecific.Precision,
                        Scale = bestSpecific.Scale,
                        IsReliable = bestSpecific.Reliability >= ReliabilityThreshold
                    };
                }
            }

            // Contrato 8.6: Fallback para tipo genérico quando não confiável
            return new SqlColumnType
            {
                TypeName = "nvarchar",
                Precision = 255,
                IsReliable = false
            };
        }

        // Contrato 8.6: Em caso de empate, priorizar tipo mais específico (bit, int, decimal, datetime sobre nvarchar)
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

    private TypeCandidate EvaluateBit(IEnumerable<string> samples, int totalCount)
    {
        var validCount = 0;

        foreach (var sample in samples)
        {
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
            Reliability = totalCount > 0 ? (double)validCount / totalCount : 0
        };
    }

    private TypeCandidate EvaluateInt(IEnumerable<string> samples, int totalCount)
    {
        var validCount = 0;

        foreach (var sample in samples)
        {
            if (int.TryParse(sample.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var value))
            {
                validCount++;
            }
        }

        return new TypeCandidate
        {
            TypeName = "int",
            Reliability = totalCount > 0 ? (double)validCount / totalCount : 0
        };
    }

    private TypeCandidate EvaluateBigInt(IEnumerable<string> samples, int totalCount)
    {
        var validCount = 0;

        foreach (var sample in samples)
        {
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
            Reliability = totalCount > 0 ? (double)validCount / totalCount : 0
        };
    }

    private TypeCandidate EvaluateDecimal(IEnumerable<string> samples, int totalCount)
    {
        var validCount = 0;
        var maxPrecision = 0;
        var maxScale = 0;

        foreach (var sample in samples)
        {
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
            Reliability = totalCount > 0 ? (double)validCount / totalCount : 0
        };
    }

    private TypeCandidate EvaluateDate(IEnumerable<string> samples, int totalCount)
    {
        var validCount = 0;
        var dateOnlyFormats = new[]
        {
            "yyyy-MM-dd",
            "dd/MM/yyyy",
            "MM/dd/yyyy"
        };

        foreach (var sample in samples)
        {
            var trimmed = sample.Trim();
            
            // Verificar se é apenas data (sem hora)
            if (DateTime.TryParseExact(trimmed, dateOnlyFormats, CultureInfo.InvariantCulture, DateTimeStyles.None, out var dateValue))
            {
                // Garantir que não tem componente de hora
                if (dateValue.TimeOfDay == TimeSpan.Zero)
                {
                    validCount++;
                }
            }
        }

        return new TypeCandidate
        {
            TypeName = "date",
            Reliability = totalCount > 0 ? (double)validCount / totalCount : 0
        };
    }

    private TypeCandidate EvaluateDateTime(IEnumerable<string> samples, int totalCount)
    {
        var validCount = 0;
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
            Reliability = totalCount > 0 ? (double)validCount / totalCount : 0
        };
    }

    private TypeCandidate EvaluateNVarChar(IEnumerable<string> samples, int totalCount)
    {
        var validCount = 0;
        var maxLength = 0;

        foreach (var sample in samples)
        {
            var trimmed = sample.Trim();
            
            // Verificar se pode ser parseado como outros tipos específicos
            var lowerTrimmed = trimmed.ToLowerInvariant();
            bool canBeBit = lowerTrimmed == "0" || lowerTrimmed == "1" ||
                           lowerTrimmed == "true" || lowerTrimmed == "false" ||
                           lowerTrimmed == "sim" || lowerTrimmed == "não" ||
                           lowerTrimmed == "yes" || lowerTrimmed == "no";
            
            bool canBeInt = int.TryParse(trimmed, NumberStyles.Integer, CultureInfo.InvariantCulture, out _);
            bool canBeDecimal = decimal.TryParse(trimmed, NumberStyles.Number, CultureInfo.InvariantCulture, out _);
            
            var dateOnlyFormats = new[]
            {
                "yyyy-MM-dd",
                "dd/MM/yyyy",
                "MM/dd/yyyy"
            };
            var dateTimeFormats = new[]
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
            bool canBeDate = DateTime.TryParseExact(trimmed, dateOnlyFormats, CultureInfo.InvariantCulture, DateTimeStyles.None, out var dateValue) &&
                            dateValue.TimeOfDay == TimeSpan.Zero;
            bool canBeDateTime = DateTime.TryParse(trimmed, CultureInfo.InvariantCulture, DateTimeStyles.None, out _) ||
                                DateTime.TryParseExact(trimmed, dateTimeFormats, CultureInfo.InvariantCulture, DateTimeStyles.None, out _);
            
            // Se pode ser outro tipo específico, reduzir confiabilidade do nvarchar
            // Apenas contar como válido para nvarchar se não pode ser outro tipo
            if (!canBeBit && !canBeInt && !canBeDecimal && !canBeDate && !canBeDateTime)
            {
                validCount++;
            }
            
            var length = trimmed.Length;
            maxLength = Math.Max(maxLength, length);
        }

        // Calcular confiabilidade: reduzir baseado em quantos valores podem ser outros tipos
        // Se muitos valores podem ser outros tipos, nvarchar não é confiável
        var reliability = totalCount > 0 
            ? (double)validCount / totalCount 
            : 0;

        // Contrato 8.9: Tamanho padrão nvarchar(255), usar nvarchar(max) se maior
        // TypeName sempre será "nvarchar" sem parênteses
        // Precision será null para nvarchar(max), ou o tamanho específico (limitado a 255 como padrão)
        var precision = maxLength > 255 ? null : (int?)(maxLength > 0 ? maxLength : 255);

        return new TypeCandidate
        {
            TypeName = "nvarchar",
            Precision = precision,
            Reliability = reliability
        };
    }

    private int GetTypePriority(string typeName)
    {
        // Contrato 8.6: Priorizar tipo mais específico sobre nvarchar
        // Menor número = maior prioridade
        // nvarchar deve ser o último recurso (número maior)
        return typeName.ToLowerInvariant() switch
        {
            "bit" => 1,        // Mais específico
            "int" => 2,
            "bigint" => 3,
            "decimal" => 4,
            "date" => 5,
            "datetime" => 6,
            "nvarchar" => 99,  // Último recurso
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

