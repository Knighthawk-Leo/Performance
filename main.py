from fastapi import FastAPI
import pandas as pd
from fastapi.responses import JSONResponse

app = FastAPI()

df = pd.read_csv('May2023StatementRun_EMUQTECHInc-royalty_product_and_asset.csv')
@app.get("/performance/streams/{Name}")
async def get_stream(Name):
    print(Name)
    streams = df[df['Asset Artist'] == Name].groupby(['DSP'])['Asset Quantity'].sum().sort_values(ascending=False)
    json_response = streams.reset_index().to_json(orient='records')
    
    return JSONResponse(content=json_response)

@app.get("/performance/trends/{Name}")
async def get_trends(Name):
    print(Name)
    trends= df[df['Asset Artist'] == Name].groupby(['Sale Start date'])['Asset Quantity'].sum()
    json_response = trends.reset_index().to_json(orient='records')
    
    return JSONResponse(content=json_response)



@app.get("/performance/map/{Name}")
async def get_mapName(Name):
    print(Name)
    map= df[df['Asset Artist'] == "Adam Faith"].groupby(['Territory'])['Asset Quantity'].sum()
    json_response =map.reset_index().to_json(orient='records')
    
    return JSONResponse(content=json_response)

@app.post("/")
async def post():
    return {"message": "the post route"}


@app.put("/")
async def put():
    return {"message": "the put route"}
