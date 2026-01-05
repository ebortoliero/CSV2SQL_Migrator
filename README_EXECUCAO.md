# Como Executar o Sistema CSV2SQL Migrator

## Pré-requisitos

1. **.NET 8 SDK** instalado
2. **SQL Server** (2008 ou superior) instalado e em execução
3. **Banco de dados** criado (ou o sistema criará automaticamente)

## Passo 1: Configurar a Connection String

Edite o arquivo `src/CSV2SQL_Migrator.Web/appsettings.json` e ajuste a connection string:

```json
{
  "ConnectionStrings": {
    "DefaultConnection": "Server=SEU_SERVIDOR;Database=CSV2SQL_Migrator;Integrated Security=true;TrustServerCertificate=true;"
  }
}
```

**Opções de autenticação:**

- **Autenticação Integrada (Windows):**
  ```
  Server=localhost;Database=CSV2SQL_Migrator;Integrated Security=true;TrustServerCertificate=true;
  ```

- **Autenticação SQL Server:**
  ```
  Server=localhost;Database=CSV2SQL_Migrator;User Id=seu_usuario;Password=sua_senha;TrustServerCertificate=true;
  ```

## Passo 2: Criar o Banco de Dados (Opcional)

O sistema criará automaticamente as tabelas necessárias na primeira execução. Se preferir criar o banco manualmente:

```sql
CREATE DATABASE CSV2SQL_Migrator;
```

## Passo 3: Executar o Sistema

### Opção A: Via Visual Studio / Rider
1. Abra a solução `src/CSV2SQL_Migrator.sln`
2. Defina `CSV2SQL_Migrator.Web` como projeto de inicialização
3. Pressione F5 ou clique em "Executar"

### Opção B: Via Terminal/Command Prompt

```bash
# Navegar até o diretório do projeto Web
cd src/CSV2SQL_Migrator.Web

# Restaurar pacotes NuGet (se necessário)
dotnet restore

# Executar a aplicação
dotnet run
```

### Opção C: Via Terminal na raiz do projeto

```bash
# Na raiz do projeto
dotnet run --project src/CSV2SQL_Migrator.Web/CSV2SQL_Migrator.Web.csproj
```

## Passo 4: Acessar a Aplicação

Após a execução, o sistema estará disponível em:

- **HTTP:** http://localhost:5104
- **HTTPS:** https://localhost:7126

O navegador será aberto automaticamente. Se não abrir, acesse manualmente uma das URLs acima.

## Passo 5: Usar o Sistema

1. **Configurar Migração:**
   - Acesse a página "Configuração"
   - Informe a pasta raiz onde estão os arquivos CSV
   - Configure os parâmetros de conexão com o banco de destino
   - Teste a conexão
   - Clique em "Iniciar Migração"

2. **Acompanhar Jobs:**
   - Acesse a página "Jobs" para ver todos os jobs criados
   - Clique em "Ver Detalhes" para monitorar o progresso

3. **Monitorar Execução:**
   - Na página de monitoramento, você verá:
     - Progresso em tempo real
     - Arquivos processados
     - Erros ocorridos
     - Métricas de execução
     - Percentual de aproveitamento

## Estrutura de URLs

- `/` - Página inicial
- `/Config` - Configuração de migração
- `/Jobs` - Lista de jobs
- `/Monitoring?jobId={id}` - Monitoramento de job específico

## Solução de Problemas

### Erro: "Connection string 'DefaultConnection' not found"
- Verifique se a connection string está configurada no `appsettings.json`

### Erro: "Cannot open database"
- Verifique se o SQL Server está em execução
- Verifique se o banco de dados existe (ou deixe o sistema criar)
- Verifique as credenciais na connection string

### Erro: "TrustServerCertificate"
- Se estiver usando certificado SSL, remova `TrustServerCertificate=true` ou configure o certificado adequadamente

## Notas Importantes

- O sistema cria automaticamente as tabelas de controle (`Jobs`, `JobFiles`, `JobErrors`, `JobMetrics`) no banco configurado
- Os dados migrados serão criados em tabelas no formato `TB_<NomeArquivo>`
- O processamento ocorre em background - você pode acompanhar o progresso na página de monitoramento
- A página de monitoramento atualiza automaticamente a cada 5 segundos quando um job está em execução

