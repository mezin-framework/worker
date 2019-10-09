from worker.worker import RefreshWorker
from prometheus_client import start_http_server

start_http_server(8000)

worker = RefreshWorker()
worker.run()
