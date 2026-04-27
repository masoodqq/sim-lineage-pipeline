import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import FastAPI, HTTPException
from consumer.metadata_store import query_by_run_id, query_by_delta_version

app = FastAPI(
    title="Sim Lineage API",
    description="Query dataset provenance for simulation pipeline",
    version="1.0.0"
)

@app.get("/")
def root():
    return {"status": "ok", "message": "Sim Lineage API is running"}

@app.get("/lineage/{run_id}")
def get_lineage_by_run(run_id: str):
    rows = query_by_run_id(run_id)
    if not rows:
        raise HTTPException(status_code=404, detail=f"run_id '{run_id}' not found")
    return {
        "run_id": run_id,
        "results": [
            {
                "run_id":        r[0],
                "git_sha":       r[1],
                "delta_version": r[2],
                "row_count":     r[3],
                "processed_at":  r[4]
            } for r in rows
        ]
    }

@app.get("/datasets/{delta_version}")
def get_runs_by_version(delta_version: int):
    rows = query_by_delta_version(delta_version)
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"delta_version '{delta_version}' not found"
        )
    return {
        "delta_version": delta_version,
        "results": [
            {
                "run_id":        r[0],
                "git_sha":       r[1],
                "delta_version": r[2],
                "row_count":     r[3],
                "processed_at":  r[4]
            } for r in rows
        ]
    }