# APScheduler Optimization for Digital Ocean Deployment

## Issues Identified and Fixed

### 1. **Excessive max_instances Configuration**
**Problem**: `max_instances=10000` was causing resource exhaustion and potential memory leaks.

**Solution**: Reduced to `max_instances=1` and added proper job configuration:
```python
scheduler = BackgroundScheduler(
    job_defaults={
        'coalesce': True,  # Combine missed executions
        'max_instances': 1,  # Only allow 1 instance per job
        'misfire_grace_time': 300  # 5 minutes grace time for missed executions
    },
    timezone='America/Sao_Paulo'  # Set timezone for all jobs
)
```

### 2. **Subprocess Calls in Jobs**
**Problem**: Using `subprocess.run()` to call external Python scripts was inefficient and caused blocking.

**Solution**: Direct function imports and calls:
```python
def job_embu():
    try:
        logger.info("Starting Embu SLA update job")
        from incentivosEmbu import main as embu_main
        embu_main()
        logger.info("Embu SLA update completed successfully")
    except Exception as e:
        logger.error(f"Error in Embu job: {e}")
```

### 3. **Scheduler Starting in @app.before_request**
**Problem**: This could start the scheduler multiple times, causing conflicts.

**Solution**: Proper initialization function:
```python
def initialize_scheduler():
    """Initialize and start the scheduler once"""
    if not scheduler.running:
        try:
            scheduler.start()
            logger.info("APScheduler started successfully")
        except Exception as e:
            logger.error(f"Failed to start scheduler: {e}")
```

### 4. **Gunicorn Compatibility Issues**
**Problem**: Scheduler wasn't starting properly with Gunicorn due to worker process management.

**Solution**: Created dedicated WSGI entry point and Gunicorn configuration:
- `wsgi.py` - Proper WSGI entry point for Gunicorn
- `gunicorn.conf.py` - Optimized Gunicorn configuration

## Gunicorn-Specific Fixes

### **New Files Created:**

1. **`wsgi.py`** - WSGI entry point that properly initializes the scheduler
2. **`gunicorn.conf.py`** - Gunicorn configuration optimized for APScheduler

### **How to Use:**

Instead of running:
```bash
gunicorn app:app
```

Use:
```bash
gunicorn -c gunicorn.conf.py wsgi:application
```

Or simply:
```bash
gunicorn wsgi:application
```

### **Key Configuration Changes:**

1. **Single Worker**: Set `workers = 1` to avoid scheduler conflicts
2. **Preload App**: Enabled `preload_app = True` for better performance
3. **Proper Initialization**: Scheduler starts in the worker process

### **Why This Works:**

- **Single Worker**: Prevents multiple schedulers from running simultaneously
- **WSGI Entry Point**: Ensures scheduler is initialized when the app is created
- **Proper Lifecycle**: Scheduler starts in the correct process context

## Additional Recommendations for Digital Ocean

### 1. **Use a Process Manager**
Consider using a process manager like `supervisor` or `systemd` to ensure your application stays running:

```bash
# Install supervisor
sudo apt-get install supervisor

# Create configuration file
sudo nano /etc/supervisor/conf.d/cubbo-control-center.conf
```

```ini
[program:cubbo-control-center]
command=/path/to/venv/bin/gunicorn -c gunicorn.conf.py wsgi:application
directory=/path/to/cubbo-control-center
user=www-data
autostart=true
autorestart=true
stderr_logfile=/var/log/cubbo-control-center.err.log
stdout_logfile=/var/log/cubbo-control-center.out.log
environment=PYTHONPATH="/path/to/cubbo-control-center"
```

### 2. **Use Gunicorn for Production**
Replace Flask's development server with Gunicorn:

```bash
pip install gunicorn
```

Run with Gunicorn:
```bash
gunicorn -c gunicorn.conf.py wsgi:application
```

### 3. **Add Health Checks**
Implement health check endpoints:

```python
@app.route('/health')
def health_check():
    return jsonify({
        'status': 'healthy',
        'scheduler_running': scheduler.running,
        'redis_connected': redis_client.ping(),
        'timestamp': datetime.now().isoformat()
    })
```

### 4. **Monitor Resource Usage**
Add monitoring for CPU and memory usage:

```python
import psutil

@app.route('/metrics')
def metrics():
    return jsonify({
        'cpu_percent': psutil.cpu_percent(),
        'memory_percent': psutil.virtual_memory().percent,
        'disk_usage': psutil.disk_usage('/').percent
    })
```

### 5. **Consider Using Celery for Heavy Jobs**
For very resource-intensive jobs, consider using Celery with Redis as a broker:

```python
from celery import Celery

celery_app = Celery('cubbo_tasks', broker='redis://localhost:6379/0')

@celery_app.task
def heavy_job():
    # Your heavy computation here
    pass
```

### 6. **Add Job Monitoring**
Implement job monitoring and alerting:

```python
def job_with_monitoring(job_func, job_name):
    """Wrapper to add monitoring to jobs"""
    start_time = time.time()
    try:
        result = job_func()
        duration = time.time() - start_time
        logger.info(f"{job_name} completed successfully in {duration:.2f}s")
        return result
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"{job_name} failed after {duration:.2f}s: {e}")
        # Send alert here
        raise
```

### 7. **Environment-Specific Configuration**
Create different configurations for development and production:

```python
# config.py
import os

class Config:
    SCHEDULER_TIMEZONE = 'America/Sao_Paulo'
    SCHEDULER_COALESCE = True
    SCHEDULER_MAX_INSTANCES = 1
    SCHEDULER_MISFIRE_GRACE_TIME = 300

class DevelopmentConfig(Config):
    DEBUG = True
    SCHEDULER_MAX_INSTANCES = 3  # Allow more instances in dev

class ProductionConfig(Config):
    DEBUG = False
    SCHEDULER_MAX_INSTANCES = 1  # Strict limit in production
```

### 8. **Database Backend for Job Persistence**
For better reliability, consider using a database backend:

```python
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

jobstores = {
    'default': SQLAlchemyJobStore(url='sqlite:///jobs.sqlite')
}

scheduler = BackgroundScheduler(
    jobstores=jobstores,
    job_defaults={
        'coalesce': True,
        'max_instances': 1,
        'misfire_grace_time': 300
    },
    timezone='America/Sao_Paulo'
)
```

## Performance Monitoring

### 1. **Add Performance Logging**
```python
import time
from functools import wraps

def log_performance(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            duration = time.time() - start_time
            logger.info(f"{func.__name__} completed in {duration:.2f}s")
            return result
        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"{func.__name__} failed after {duration:.2f}s: {e}")
            raise
    return wrapper
```

### 2. **Monitor Job Execution Times**
Track how long jobs take to execute and alert if they exceed thresholds:

```python
JOB_TIMEOUTS = {
    'embu_sla': 300,  # 5 minutes
    'extrema_sla': 300,
    'bonus_calc': 180,  # 3 minutes
    'store_status': 60,  # 1 minute
}
```

## Deployment Checklist

- [ ] Use Gunicorn with the new WSGI entry point
- [ ] Use the provided Gunicorn configuration
- [ ] Set up process manager (supervisor/systemd)
- [ ] Configure proper logging
- [ ] Set up health checks
- [ ] Monitor resource usage
- [ ] Test job reliability
- [ ] Set up alerts for job failures
- [ ] Configure timezone properly
- [ ] Test application restart scenarios

## Expected Improvements

After implementing these changes, you should see:

1. **Reduced Memory Usage**: No more subprocess overhead
2. **Better Reliability**: Jobs won't stack up or conflict
3. **Improved Performance**: Direct function calls are faster
4. **Better Monitoring**: Proper logging and health checks
5. **Easier Debugging**: Clear error messages and job status
6. **Production Ready**: Proper process management and monitoring
7. **Gunicorn Compatible**: Scheduler works properly with multiple workers 