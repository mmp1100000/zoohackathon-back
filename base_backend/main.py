import io

from fastapi import FastAPI, File, Form, UploadFile
from fastapi.middleware.cors import CORSMiddleware

from controllers.etl import data_cleaning, csv_to_pandas

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/columns/specs")
async def root():
    return {"Category": {"type": "categorical", "is_obligatory": 0},
            "Country_origin": {"type": "numerical", "is_obligatory": 1},
            "Date": {"type": "timestamp", "is_obligatory": 1},
            "Scientific_Name": {"type": "categorical", "is_obligatory": 0},
            "Item": {"type": "categorical", "is_obligatory": 0},
            "Count": {"type": "numerical", "is_obligatory": 0},
            "Kingdom": {"type": "categorical", "is_obligatory": 0},
            "Phylum": {"type": "categorical", "is_obligatory": 0},
            "Class": {"type": "categorical", "is_obligatory": 0},
            "Order": {"type": "categorical", "is_obligatory": 0},
            "Family": {"type": "categorical", "is_obligatory": 0},
            "Genus": {"type": "categorical", "is_obligatory": 0},
            "Species": {"type": "categorical", "is_obligatory": 0},
            "Common_Name": {"type": "categorical", "is_obligatory": 0},
            "Role": {"type": "categorical", "is_obligatory": 0},
            "Order_in_Trade_Route": {"type": "categorical", "is_obligatory": 0},
            "City": {"type": "categorical", "is_obligatory": 0},
            "Region": {"type": "categorical", "is_obligatory": 0},
            "Country": {"type": "categorical", "is_obligatory": 0},
            "LatitudeLongitude": {"type": "latlong", "is_obligatory": 0},
            "Primary_Source": {"type": "categorical", "is_obligatory": 0},
            "Source_Type": {"type": "categorical", "is_obligatory": 0},
            "Outcome": {"type": "categorical", "is_obligatory": 0}
            }


@app.post("/api/data/upload")
async def create_file(file: UploadFile = Form(...)):
    print(file)
    results = await data_cleaning(csv_to_pandas(file.file), csv_to_pandas('assets/country_iso_codes.csv'), csv_to_pandas('assets/country_coor.csv'))
    return results
