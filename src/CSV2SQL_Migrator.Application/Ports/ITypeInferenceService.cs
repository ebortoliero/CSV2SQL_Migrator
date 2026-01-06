using System;

namespace CSV2SQL_Migrator.Application.Ports;

/// <summary>
/// Interface para inferência determinística de tipos de dados.
/// RF12: Inferir automaticamente tipos de dados
/// RF13: Aplicar fallback para inferência não confiável
/// Contrato 8.6: Inferência Determinística de Tipos
/// </summary>
public interface ITypeInferenceService
{
    /// <summary>
    /// Infere o tipo SQL para uma coluna baseado em amostragem determinística.
    /// </summary>
    /// <param name="columnValues">Valores amostrados da coluna (até 5.000 linhas)</param>
    /// <param name="columnName">Nome da coluna (para logging)</param>
    /// <returns>Tipo SQL inferido</returns>
    SqlColumnType InferType(IEnumerable<string> columnValues, string columnName);
}

/// <summary>
/// Representa um tipo de coluna SQL com precisão e escala quando aplicável.
/// </summary>
public class SqlColumnType
{
    public string TypeName { get; set; } = string.Empty;
    public int? Precision { get; set; }
    public int? Scale { get; set; }
    public bool IsReliable { get; set; }

    public string ToSqlDefinition()
    {
        // Para nvarchar, quando Precision é null, significa nvarchar(max)
        if (TypeName.Equals("nvarchar", StringComparison.OrdinalIgnoreCase) && !Precision.HasValue)
        {
            return "nvarchar(max)";
        }
        
        if (Precision.HasValue && Scale.HasValue)
        {
            return $"{TypeName}({Precision.Value},{Scale.Value})";
        }
        if (Precision.HasValue)
        {
            return $"{TypeName}({Precision.Value})";
        }
        return TypeName;
    }
}

