import logging
import uuid

import yaml
from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel
from redis import Redis
from rq import Queue
from rq.job import Job, NoSuchJobError

from .config import settings
from .tasks import pack_files, render_stl, RENDERED_PREFIX

logger = logging.getLogger("process-files-thread")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

app = FastAPI(title="satellite-process-files-thread")

redis_conn = Redis.from_url(settings.redis_url)
queue = Queue("thread", connection=redis_conn)


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


@app.post("/render", response_model=RenderResponse, status_code=202)
def render(request: RenderRequest):
    job_id = str(uuid.uuid4())
    logger.info(f"render_request_received job_id={job_id} key={request.key} webhook={request.webhook_url}")
    queue.enqueue(render_stl, job_id, request.key, request.webhook_url, job_id=job_id)
    logger.info(f"render_job_enqueued job_id={job_id}")
    return RenderResponse(job_id=job_id, status="queued")


@app.post("/pack", response_model=PackResponse, status_code=202)
def pack(request: PackRequest):
    file_keys = [f.key for f in request.files]
    logger.info(f"pack_request_received files={len(request.files)} webhook={request.webhook_url} keys={file_keys}")
    if not request.files:
        logger.warning(f"pack_request_no_files webhook={request.webhook_url}")
        raise HTTPException(status_code=400, detail="No files provided")

    job_id = str(uuid.uuid4())
    file_dicts = [{"key": f.key, "name": f.name} for f in request.files]
    logger.info(f"pack_enqueueing job_id={job_id} files={file_dicts}")
    queue.enqueue(pack_files, job_id, file_dicts, request.webhook_url, job_id=job_id)
    logger.info(f"pack_job_enqueued job_id={job_id}")

    return PackResponse(job_id=job_id, status="queued")


@app.get("/pack/{job_id}")
def get_pack_status(job_id: str):
    logger.info(f"pack_status_request job_id={job_id}")
    try:
        job = Job.fetch(job_id, connection=redis_conn)
    except NoSuchJobError:
        logger.warning(f"pack_status_job_not_found job_id={job_id}")
        raise HTTPException(status_code=404, detail="Job not found")

    status = job.get_status()
    logger.info(f"pack_status_response job_id={job_id} status={status}")
    response = {"job_id": job_id, "status": str(status)}

    if job.is_finished:
        response["result"] = job.result
    elif job.is_failed:
        response["error"] = str(job.exc_info)

    return response


@app.get("/openapi.yaml", include_in_schema=False)
def get_openapi_yaml():
    return Response(yaml.dump(app.openapi(), allow_unicode=True), media_type="text/yaml")
