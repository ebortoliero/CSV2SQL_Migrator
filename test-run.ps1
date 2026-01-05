$ErrorActionPreference = "Stop"
Set-Location "D:\USR\Sistemas\ProjetosDev\CSV2SQL_Migrator\src\CSV2SQL_Migrator.Web"

Write-Host "Compilando e executando aplicação..." -ForegroundColor Cyan
try {
    $job = Start-Job -ScriptBlock {
        Set-Location "D:\USR\Sistemas\ProjetosDev\CSV2SQL_Migrator\src\CSV2SQL_Migrator.Web"
        dotnet run 2>&1
    }
    
    Start-Sleep -Seconds 10
    
    $output = Receive-Job -Job $job
    Write-Host $output
    
    Stop-Job -Job $job
    Remove-Job -Job $job
} catch {
    Write-Host "Erro: $_" -ForegroundColor Red
    exit 1
}

