"""Explicit local demo only. Plans stay in memory here; production uses private S3."""
from fastapi import FastAPI
import meet_plans
meet_plans._store=meet_plans.MemoryStore()
import meet_routes
app=FastAPI();app.include_router(meet_routes.router)
@app.get('/')
async def root():return await meet_routes.meet_side()
