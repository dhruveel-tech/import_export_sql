# AI Spark API - No Celery Version

This is a simplified version of AI Spark API that removes Celery and Redis dependencies, using FastAPI's built-in `BackgroundTasks` for async job processing instead.

## Key Changes from Original

### Removed Components
- ❌ Celery task queue
- ❌ Redis broker/backend
- ❌ Separate worker containers
- ❌ Celery Beat scheduler

### New Architecture
- ✅ FastAPI `BackgroundTasks` for async processing
- ✅ Simplified deployment (single API container + MongoDB)
- ✅ No external message broker required
- ✅ Easier development and debugging

## Architecture

```
┌─────────────────┐
│   FastAPI App   │
│                 │
│  ┌───────────┐  │
│  │Background │  │
│  │  Tasks    │  │
│  └───────────┘  │
└────────┬────────┘
         │
         ▼
    ┌─────────┐
    │ MongoDB │
    └─────────┘
```

## Quick Start

### Prerequisites
- Python 3.10+
- MongoDB
- Docker & Docker Compose (optional)

### Installation

1. **Clone and navigate to the project:**
```bash
cd ai-spark-api-no-celery
```

2. **Create virtual environment:**
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install dependencies:**
```bash
pip install -r requirements.txt
```

4. **Configure environment:**
```bash
cp .env.example .env
# Edit .env with your configuration
```

5. **Run with Docker Compose:**
```bash
docker-compose up -d
```

Or run locally:
```bash
# Make sure MongoDB is running
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

## API Endpoints

### Export Operations
- `POST /spark` - Create export job
- `GET /spark/{spark_id}` - Get export details
- `GET /spark/{spark_id}/status` - Get export status
- `GET /spark?repo_guid={id}` - List exports by repo

### Import Operations
- `POST /spark/import` - Create import job
- `GET /spark/import/{import_id}` - Get import details
- `GET /spark/import/{import_id}/status` - Get import status

### Health Check
- `GET /health` - System health check

## How Background Processing Works

Instead of Celery workers, this version uses FastAPI's `BackgroundTasks`:

```python
from fastapi import BackgroundTasks

@router.post("/spark")
async def create_export(
    work_order: ExportWorkOrderCreate,
    background_tasks: BackgroundTasks,
):
    job = await service.create_export_job(work_order)
    
    # Add task to background queue
    background_tasks.add_task(process_export_background, str(job.spark_id))
    
    return job
```

Background tasks run in the same process but don't block the response. They're suitable for:
- ✅ Short to medium duration tasks (< 5 minutes)
- ✅ Tasks that don't require complex retry logic
- ✅ Development and small-scale deployments

## Limitations & Considerations

### When to Use This Version
- Small to medium workload
- Simplified deployment requirements
- Development/testing environments
- Don't need distributed task processing

### When to Use Celery Version
- High volume of concurrent jobs
- Long-running tasks (> 5 minutes)
- Need distributed workers across multiple machines
- Require advanced retry/failure handling
- Need task prioritization and rate limiting

### Important Notes
1. **Process Restart**: Background tasks are lost if the API server restarts
2. **Scalability**: Limited by single server resources
3. **Monitoring**: Less sophisticated than Celery's monitoring tools
4. **Job Status**: Stored in MongoDB, can be queried anytime

## Project Structure

```
ai-spark-api-no-celery/
├── app/
│   ├── api/                  # API routes
│   │   ├── export_routes.py
│   │   ├── import_routes.py
│   │   └── health_routes.py
│   ├── background/           # Background task processors (NEW)
│   │   ├── __init__.py
│   │   └── tasks.py
│   ├── core/                 # Configuration
│   ├── db/                   # Database connections
│   ├── models/               # Data models
│   ├── schemas/              # Pydantic schemas
│   ├── services/             # Business logic
│   │   ├── export_service.py
│   │   ├── import_service.py
│   │   ├── artifact_generator.py
│   │   └── fabric_client.py
│   └── main.py              # Application entry point
├── docker-compose.yml        # Simplified (no Redis/Celery)
├── Dockerfile
├── requirements.txt          # No Celery/Redis dependencies
├── .env.example
└── README.md
```

## Environment Variables

Key environment variables (see `.env.example` for full list):

```bash
# MongoDB
MONGODB_URL=mongodb://localhost:27017
MONGODB_DB_NAME=ai_spark_db

# Storage
EXPORT_BASE_PATH=/var/spark/exports
IMPORT_BASE_PATH=/var/spark/imports

# Fabric Integration
FABRIC_API_URL=https://fabric-api.example.com
FABRIC_API_KEY=your-api-key

# Server
HOST=0.0.0.0
PORT=8000
WORKERS=4
```

## Development

### Running Tests
```bash
pytest tests/
```

### Code Formatting
```bash
black app/
isort app/
```

### Debugging
Since background tasks run in the same process, debugging is straightforward:
- Use breakpoints in your IDE
- Check logs with `structlog` output
- Monitor job status in MongoDB

## Migration from Celery Version

If migrating from the Celery version:

1. **Database**: Same MongoDB schema, no migration needed
2. **API**: Same endpoints and responses
3. **Configuration**: Remove Redis/Celery settings from `.env`
4. **Deployment**: Remove Redis and Celery containers
5. **Monitoring**: Update monitoring to check FastAPI process instead of Celery workers

## Monitoring

Monitor background tasks through:
1. **Job Status API**: Check job status via API endpoints
2. **Application Logs**: Structured logs via `structlog`
3. **MongoDB**: Query job documents directly
4. **Health Endpoint**: Check overall system health

## Docker Deployment

The docker-compose setup includes:
- MongoDB with persistent storage
- FastAPI application with volume mounts
- Health checks for both services

```bash
# Start services
docker-compose up -d

# View logs
docker-compose logs -f api

# Stop services
docker-compose down

# Stop and remove volumes
docker-compose down -v
```

## Support & Troubleshooting

### Common Issues

1. **Jobs not processing**: Check MongoDB connection and logs
2. **Slow performance**: Consider scaling horizontally or reverting to Celery
3. **Memory issues**: Monitor application memory usage, adjust worker count

### Getting Help

Check logs for detailed error messages:
```bash
docker-compose logs -f api
```

## License

[Your License Here]

## Contributing

[Your Contributing Guidelines Here]
