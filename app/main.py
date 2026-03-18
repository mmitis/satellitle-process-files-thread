import logging
import uuid

import yaml
from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel

from .config import settings
from .tasks import pack_files, render_stl

logger = logging.getLogger("process-files-thread")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

app = FastAPI(title="satellite-process-files-thread")

# In-memory job status — good enough since NestJS uses webhook callbacks
# and PackingTimeoutService handles stalled jobs after 10 min.
_jobs: dict[str, dict] = {}


class PackFileEntry(BaseModel):
    key: str
    name: str


class PackRequest(BaseModel):
    files: list[PackFileEntry]
    webhook_url: str


class PackResponse(BaseModel):
    job_id: str
    status: str


class RenderRequest(BaseModel):
    key: str
    webhook_url: str


class RenderResponse(BaseModel):
    job_id: str
    status: str


def _run_pack(job_id: str, file_dicts: list[dict], webhook_url: str) -> None:
    _jobs[job_id] = {"status": "processing"}
    try:
        result = pack_files(job_id, file_dicts, webhook_url)
        _jobs[job_id] = {"status": "completed", "result": result}
    except Exception as e:
        logger.error(f"pack_job_failed job_id={job_id} error={e}")
        _jobs[job_id] = {"status": "failed", "error": str(e)}


def _run_render(job_id: str, file_key: str, webhook_url: str) -> None:
    _jobs[job_id] = {"status": "processing"}
    try:
        result = render_stl(job_id, file_key, webhook_url)
        _jobs[job_id] = {"status": "completed", "result": result}
    except Exception as e:
        logger.error(f"render_job_failed job_id={job_id} error={e}")
        _jobs[job_id] = {"status": "failed", "error": str(e)}


@app.post("/render", response_model=RenderResponse, status_code=202)
def render(request: RenderRequest, background_tasks: BackgroundTasks):
    job_id = str(uuid.uuid4())
    logger.info(f"render_request_received job_id={job_id} key={request.key} webhook={request.webhook_url}")
    background_tasks.add_task(_run_render, job_id, request.key, request.webhook_url)
    logger.info(f"render_job_queued job_id={job_id}")
    return RenderResponse(job_id=job_id, status="queued")


@app.post("/pack", response_model=PackResponse, status_code=202)
def pack(request: PackRequest, background_tasks: BackgroundTasks):
    file_keys = [f.key for f in request.files]
    logger.info(f"pack_request_received files={len(request.files)} webhook={request.webhook_url} keys={file_keys}")
    if not request.files:
        logger.warning(f"pack_request_no_files webhook={request.webhook_url}")
        raise HTTPException(status_code=400, detail="No files provided")

    job_id = str(uuid.uuid4())
    file_dicts = [{"key": f.key, "name": f.name} for f in request.files]
    logger.info(f"pack_enqueueing job_id={job_id} files={file_dicts}")
    background_tasks.add_task(_run_pack, job_id, file_dicts, request.webhook_url)
    logger.info(f"pack_job_queued job_id={job_id}")
    return PackResponse(job_id=job_id, status="queued")


@app.get("/pack/{job_id}")
def get_pack_status(job_id: str):
    logger.info(f"pack_status_request job_id={job_id}")
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"job_id": job_id, **job}


@app.get("/openapi.yaml", include_in_schema=False)
def get_openapi_yaml():
    return Response(yaml.dump(app.openapi(), allow_unicode=True), media_type="text/yaml")
