import logging
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from config import settings
from api_v1 import api_router
from session import DatabaseManager
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from limiter import limiter

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("JudiQ.Main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initializing JudiQ Infrastructure...")
    DatabaseManager.init_db()
    logger.info("Infrastructure ready.")
    yield
    logger.info("Shutting down JudiQ Infrastructure...")


app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    description="JudiQ AI Section 138 NI Act Litigation Intelligence Platform Backend",
    lifespan=lifespan
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


from metrics import prometheus_metrics_endpoint, JUDIQ_REQUESTS_TOTAL, JUDIQ_REQUEST_DURATION_SECONDS

@app.middleware("http")
async def add_process_time_and_metrics(request: Request, call_next):
    start_time = time.time()
    origin = request.headers.get("origin")
    
    # Handle preflight OPTIONS requests immediately
    if request.method == "OPTIONS":
        response = Response(status_code=200)
        if origin:
            response.headers["Access-Control-Allow-Origin"] = origin
            response.headers["Access-Control-Allow-Credentials"] = "true"
            response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS, PATCH, HEAD"
            response.headers["Access-Control-Allow-Headers"] = request.headers.get("access-control-request-headers", "*")
            response.headers["Access-Control-Max-Age"] = "86400"
        return response

    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    
    if origin and "Access-Control-Allow-Origin" not in response.headers:
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Access-Control-Allow-Credentials"] = "true"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS, PATCH, HEAD"
        response.headers["Access-Control-Allow-Headers"] = "*"
    
    # Record Prometheus Metrics
    try:
        endpoint = request.url.path
        method = request.method
        status = str(response.status_code)
        JUDIQ_REQUESTS_TOTAL.labels(method=method, endpoint=endpoint, status_code=status).inc()
        JUDIQ_REQUEST_DURATION_SECONDS.labels(method=method, endpoint=endpoint).observe(process_time)
    except Exception:
        pass

    # Prevent browser from serving stale cached static files in development
    if not request.url.path.startswith("/api/"):
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response

import hmac

# Prometheus scraping endpoint (protected if METRICS_TOKEN is configured in environment)
async def metrics_endpoint_wrapper(request: Request):
    metrics_token = os.environ.get("METRICS_TOKEN", "")
    if metrics_token:
        auth_header = request.headers.get("Authorization", "")
        provided = request.headers.get("X-Metrics-Token", "")
        if not provided and auth_header.startswith("Bearer "):
            provided = auth_header.split(" ", 1)[1].strip()
        if not provided or not hmac.compare_digest(provided, metrics_token):
            raise HTTPException(status_code=403, detail="Forbidden: Valid Metrics Token Required")
    return await prometheus_metrics_endpoint()

app.add_api_route("/metrics", metrics_endpoint_wrapper, methods=["GET"], tags=["Observability"])


# CORS Configuration supporting explicit production origins and Netlify/Workers deployments
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.BACKEND_CORS_ORIGINS,
    allow_origin_regex=r"^https?://(.*\.)?(netlify\.app|workers\.dev|pages\.dev|vercel\.app|github\.io|onrender\.com|localhost|127\.0\.0\.1)(:\d+)?$",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    if isinstance(exc, HTTPException):
        return JSONResponse(status_code=exc.status_code, content={"success": False, "error": exc.detail})
    logger.error(f"Global error: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"success": False, "error": "Internal Server Error"}
    )


@app.api_route("/ping", methods=["GET", "HEAD"], tags=["Observability"])
async def ping():
    return {"status": "ok", "pong": True}


@app.api_route("/health", methods=["GET", "HEAD"])
async def health_check():
    health_data = {"status": "healthy", "version": settings.VERSION, "timestamp": time.time()}
    try:
        import psutil
        health_data["cpu_percent"] = psutil.cpu_percent(interval=0.1)
        health_data["memory"] = psutil.virtual_memory()._asdict()
    except ImportError:
        health_data["cpu_percent"] = "psutil not installed"
    try:
        from engine_core import registry
        health_data["engine_registry_size"] = len(registry._instances)
    except Exception:
        pass
    return health_data


from pathlib import Path
from fastapi.staticfiles import StaticFiles

frontend_dir = Path(__file__).resolve().parent.parent / "frontend"

@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    fav_svg = frontend_dir / "favicon.svg"
    if fav_svg.exists():
        return FileResponse(str(fav_svg), media_type="image/svg+xml")
    fav_icon = frontend_dir / "favicon.ico"
    if fav_icon.exists():
        return FileResponse(str(fav_icon))
    return JSONResponse(status_code=204, content={})

# All routes are served under /api/v1 prefix
app.include_router(api_router, prefix="/api/v1")

# Mount frontend directory for seamless local development & single-port hosting
if frontend_dir.exists():
    app.mount("/", StaticFiles(directory=str(frontend_dir), html=True), name="frontend")
else:
    @app.get("/")
    @app.head("/")
    async def root_endpoint():
        return {"status": "online", "service": settings.PROJECT_NAME, "version": settings.VERSION}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)

