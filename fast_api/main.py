from fastapi import FastAPI, HTTPException, Depends
from typing import Annotated
import os
from dotenv import load_dotenv
import asyncpg

load_dotenv()

app = FastAPI()

URL_DATABASE = os.getenv('URL_DATABASE')

async def get_db():
    conn = await asyncpg.connect(URL_DATABASE)
    try:
        yield conn
    finally:
        await conn.close()

db_dependency = Annotated[asyncpg.Connection, Depends(get_db)]

@app.get("/nearby-properties/r={radius}&lat={lat}&lon={lon}")
async def get_nearby_properties(
    radius: int,
    lat: float,
    lon: float,
    db: db_dependency
):
    if radius > 1000:
        raise HTTPException(status_code=400, detail="Radius must be less than 1000.")
    nearby_properties = await db.fetch("""SELECT a.property_id, a.address, a.city FROM address_table a
                                 JOIN property_geometry p
                                 USING(property_id)
                                 WHERE ST_DWithin(ST_Transform(ST_SetSRID(ST_MakePoint($1, $2),4326),26910),
                                 ST_Transform(p.geom, 26910), $3);""", lon, lat, radius)
    property_data = [{"property_id": prop[0], "address": prop[1], "city": prop[2]} for prop in nearby_properties]
    return property_data