import io
import zipfile
import boto3
import urllib.request
import urllib.parse
import json
from botocore.exceptions import ClientError

from .config import settings


def _resolve_webhook_url(webhook_url: str) -> str:
    """Replace the origin of webhook_url with settings.backend_url so the worker
    can reach the backend even when running inside Docker."""
    parsed = urllib.parse.urlparse(webhook_url)
    base = urllib.parse.urlparse(settings.backend_url)
    resolved = parsed._replace(scheme=base.scheme, netloc=base.netloc)
    return urllib.parse.urlunparse(resolved)


def _make_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        region_name=settings.s3_region,
    )


def _fire_webhook(webhook_url: str, payload: dict) -> None:
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        webhook_url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10):
        pass


RENDERED_PREFIX = "rendered-files"


def render_stl(job_id: str, file_key: str, webhook_url: str) -> dict:
    """Downloads STL from S3, renders a gray PNG on a dark background, uploads PNG, fires webhook."""
    import trimesh
    import numpy as np
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from mpl_toolkits.mplot3d.art3d import Poly3DCollection

    s3 = _make_s3_client()

    response = s3.get_object(Bucket=settings.s3_bucket, Key=file_key)
    stl_bytes = response["Body"].read()

    loaded = trimesh.load(io.BytesIO(stl_bytes), file_type="stl", force="mesh")
    if isinstance(loaded, trimesh.Scene):
        meshes = list(loaded.geometry.values())
        mesh = trimesh.util.concatenate(meshes) if len(meshes) > 1 else meshes[0]
    else:
        mesh = loaded

    bounds = mesh.bounds  # shape (2, 3)
    scale = float(np.max(bounds[1] - bounds[0])) or 1.0

    face_normals = np.array(mesh.face_normals)
    light_dir = np.array([0.5, 0.3, 1.0])
    light_dir = light_dir / np.linalg.norm(light_dir)
    intensity = np.dot(face_normals, light_dir).clip(0, 1) * 0.55 + 0.3
    colors = np.stack([intensity, intensity, intensity, np.ones_like(intensity)], axis=1)

    fig = plt.figure(figsize=(8, 8), dpi=128)
    fig.patch.set_facecolor("#1e1e1e")
    ax = fig.add_subplot(111, projection="3d")
    ax.set_facecolor("#1e1e1e")

    triangles = mesh.vertices[mesh.faces]
    poly = Poly3DCollection(triangles, facecolors=colors, linewidths=0, edgecolors="none", shade=False)
    ax.add_collection3d(poly)

    margin = scale * 0.12
    ax.set_xlim(bounds[0][0] - margin, bounds[1][0] + margin)
    ax.set_ylim(bounds[0][1] - margin, bounds[1][1] + margin)
    ax.set_zlim(bounds[0][2] - margin, bounds[1][2] + margin)
    ax.set_axis_off()
    ax.view_init(elev=25, azim=225)
    plt.subplots_adjust(left=0, right=1, top=1, bottom=0)

    out_buffer = io.BytesIO()
    plt.savefig(out_buffer, format="png", dpi=128, bbox_inches="tight", facecolor="#1e1e1e", edgecolor="none")
    plt.close(fig)
    out_buffer.seek(0)

    output_key = f"{RENDERED_PREFIX}/{job_id}.png"
    s3.put_object(
        Bucket=settings.s3_bucket,
        Key=output_key,
        Body=out_buffer.getvalue(),
        ContentType="image/png",
    )

    _fire_webhook(_resolve_webhook_url(webhook_url), {"id": job_id, "output_key": output_key})
    return {"job_id": job_id, "output_key": output_key, "status": "completed"}


def pack_files(job_id: str, files: list[dict], webhook_url: str) -> dict:
    """
    Downloads each file from S3, packs them into a zip, uploads the zip
    back to S3 under packed-files/{job_id}.zip, then POSTs {"id": job_id}
    to webhook_url.

    Each entry in `files` must have `key` (S3 key) and `name` (archive filename).
    """
    s3 = _make_s3_client()

    zip_buffer = io.BytesIO()
    failed = []

    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for entry in files:
            key = entry["key"]
            archive_name = entry["name"]
            try:
                response = s3.get_object(Bucket=settings.s3_bucket, Key=key)
                file_data = response["Body"].read()
                zf.writestr(archive_name, file_data)
            except ClientError as e:
                failed.append({"key": key, "error": str(e)})

    if failed and not zip_buffer.getbuffer().nbytes:
        raise RuntimeError(f"All files failed to download: {failed}")

    zip_buffer.seek(0)
    output_key = f"{settings.packed_files_prefix}/{job_id}.zip"

    s3.put_object(
        Bucket=settings.s3_bucket,
        Key=output_key,
        Body=zip_buffer.getvalue(),
        ContentType="application/zip",
    )

    result = {"job_id": job_id, "output_key": output_key, "status": "completed"}
    if failed:
        result["failed_files"] = failed

    _fire_webhook(_resolve_webhook_url(webhook_url), {"id": job_id, "output_key": output_key})

    return result
