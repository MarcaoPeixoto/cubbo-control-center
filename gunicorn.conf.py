#!/usr/bin/env python3
"""
Gunicorn configuration file
Optimized for the cubbo-control-center application with APScheduler
"""

import multiprocessing
import os

# Server socket
bind = "0.0.0.0:8080"
backlog = 2048

# Worker processes
workers = 1  # Use only 1 worker to avoid scheduler conflicts
worker_class = "sync"
worker_connections = 1000
max_requests = 1000
max_requests_jitter = 50
timeout = 30
keepalive = 2

# Restart workers after this many requests, to help prevent memory leaks
max_requests = 1000
max_requests_jitter = 50

# Logging
accesslog = "-"
errorlog = "-"
loglevel = "info"
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s" %(D)s'

# Process naming
proc_name = "cubbo-control-center"

# Server mechanics
daemon = False
pidfile = "/tmp/gunicorn.pid"
user = None
group = None
tmp_upload_dir = None

# SSL (if needed)
# keyfile = None
# certfile = None

# Preload app for better performance
preload_app = True

def when_ready(server):
    """Called just after the server is started"""
    server.log.info("Server is ready. Spawning workers")

def worker_int(worker):
    """Called just after a worker has been initialized"""
    worker.log.info("Worker spawned (pid: %s)", worker.pid)

def pre_fork(server, worker):
    """Called just before a worker has been forked"""
    server.log.info("Worker spawned (pid: %s)", worker.pid)

def post_fork(server, worker):
    """Called just after a worker has been forked"""
    server.log.info("Worker spawned (pid: %s)", worker.pid)

def post_worker_init(worker):
    """Called just after a worker has initialized the application"""
    worker.log.info("Worker initialized (pid: %s)", worker.pid)

def worker_abort(worker):
    """Called when a worker has been aborted"""
    worker.log.info("Worker aborted (pid: %s)", worker.pid)

def on_starting(server):
    """Called just before the master process is initialized"""
    server.log.info("Starting Gunicorn server")

def on_reload(server):
    """Called to reload the application"""
    server.log.info("Reloading application")

def on_exit(server):
    """Called just before exiting Gunicorn"""
    server.log.info("Exiting Gunicorn") 