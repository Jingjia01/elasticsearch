param(
    [string]$Environment = "local"
)

$envFile = ".env"
Write-Host "Deploying to $Environment environment using $envFile..."

Write-Host "Stopping existing containers..."
cmd /c "docker compose -f docker\docker-compose.yml -f docker\docker-compose.kibana.yml --env-file $envFile down"

Write-Host "Starting services..."
cmd /c "docker compose -f docker\docker-compose.yml -f docker\docker-compose.kibana.yml --env-file $envFile up -d"

if ($LASTEXITCODE -eq 0) {
    Write-Host "Services started. Waiting 30 seconds for initialization..."
    Start-Sleep -Seconds 30
    
    Write-Host "Checking services..."
    & docker ps
    
    Write-Host "`nServices are available at:"
    Write-Host "- Elasticsearch: http://localhost:9200"
    Write-Host "- Kibana: http://localhost:5601"
} else {
    Write-Host "Deployment failed!"
}
