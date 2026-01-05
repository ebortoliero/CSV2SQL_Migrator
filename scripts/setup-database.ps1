# Script PowerShell para configuração e verificação do banco de dados SQL Server
# CSV2SQL_Migrator - Database Setup Script

param(
    [string]$ServerInstance = "localhost",
    [string]$DatabaseName = "CSV2SQL_Migrator",
    [string]$ConnectionString = ""
)

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "CSV2SQL_Migrator - Database Setup" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Função para testar conexão SQL Server
function Test-SqlConnection {
    param(
        [string]$ServerInstance,
        [string]$DatabaseName
    )
    
    try {
        $connectionString = "Server=$ServerInstance;Database=$DatabaseName;Integrated Security=true;TrustServerCertificate=true;Connection Timeout=5;"
        $connection = New-Object System.Data.SqlClient.SqlConnection($connectionString)
        $connection.Open()
        $connection.Close()
        return $true
    }
    catch {
        Write-Host "Erro ao conectar: $($_.Exception.Message)" -ForegroundColor Red
        return $false
    }
}

# Função para criar banco de dados
function New-SqlDatabase {
    param(
        [string]$ServerInstance,
        [string]$DatabaseName
    )
    
    try {
        $connectionString = "Server=$ServerInstance;Integrated Security=true;TrustServerCertificate=true;"
        $connection = New-Object System.Data.SqlClient.SqlConnection($connectionString)
        $connection.Open()
        
        $query = "IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '$DatabaseName') CREATE DATABASE [$DatabaseName]"
        $command = New-Object System.Data.SqlClient.SqlCommand($query, $connection)
        $command.ExecuteNonQuery()
        
        $connection.Close()
        Write-Host "Banco de dados '$DatabaseName' criado com sucesso!" -ForegroundColor Green
        return $true
    }
    catch {
        Write-Host "Erro ao criar banco de dados: $($_.Exception.Message)" -ForegroundColor Red
        return $false
    }
}

# Verificar se SQL Server está acessível
Write-Host "1. Verificando conectividade com SQL Server..." -ForegroundColor Yellow
if (Test-SqlConnection -ServerInstance $ServerInstance -DatabaseName "master") {
    Write-Host "   ✓ SQL Server está acessível" -ForegroundColor Green
} else {
    Write-Host "   ✗ Não foi possível conectar ao SQL Server" -ForegroundColor Red
    Write-Host ""
    Write-Host "Possíveis soluções:" -ForegroundColor Yellow
    Write-Host "  - Verifique se o SQL Server está em execução" -ForegroundColor White
    Write-Host "  - Verifique se você tem permissões de acesso" -ForegroundColor White
    Write-Host "  - Tente usar autenticação SQL Server em vez de Windows Authentication" -ForegroundColor White
    exit 1
}

# Verificar se o banco de dados existe
Write-Host ""
Write-Host "2. Verificando se o banco de dados existe..." -ForegroundColor Yellow
if (Test-SqlConnection -ServerInstance $ServerInstance -DatabaseName $DatabaseName) {
    Write-Host "   ✓ Banco de dados '$DatabaseName' existe e está acessível" -ForegroundColor Green
} else {
    Write-Host "   ! Banco de dados '$DatabaseName' não existe ou não está acessível" -ForegroundColor Yellow
    Write-Host ""
    $create = Read-Host "Deseja criar o banco de dados? (S/N)"
    if ($create -eq "S" -or $create -eq "s") {
        if (New-SqlDatabase -ServerInstance $ServerInstance -DatabaseName $DatabaseName) {
            Write-Host "   ✓ Banco de dados criado com sucesso" -ForegroundColor Green
        } else {
            Write-Host "   ✗ Falha ao criar banco de dados" -ForegroundColor Red
            exit 1
        }
    } else {
        Write-Host "   Operação cancelada pelo usuário" -ForegroundColor Yellow
        exit 0
    }
}

# Exibir informações da conexão
Write-Host ""
Write-Host "3. Informações da conexão:" -ForegroundColor Yellow
Write-Host "   Server: $ServerInstance" -ForegroundColor White
Write-Host "   Database: $DatabaseName" -ForegroundColor White
Write-Host "   Authentication: Windows Integrated Security" -ForegroundColor White
Write-Host "   User: $env:USERDOMAIN\$env:USERNAME" -ForegroundColor White

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Configuração concluída!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan

