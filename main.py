#Saksa fra tutorialen her: https://fastapi.tiangolo.com/tutorial/first-steps/
from fastapi import FastAPI, Path
from kjoretoy import kjoretoy_tabell
from dotenv import load_dotenv
from sqlalchemy import create_engine, literal, select
import os


load_dotenv()
connstr = os.environ.get("CONN")
if connstr is None:
    connstr = "postgresql+psycopg2://postgres:mysecretpassword@localhost/postgres"

kjoretoy = kjoretoy_tabell()
engine = create_engine(connstr)
app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/regdato/{regdato}")
async def read_regdato(regdato: str = Path(..., title="Registreringsdato", description="Dato for registrering i formatet YYYY-MM-DD")):
    with engine.connect() as conn:
        res = conn.execute(
            kjoretoy.select().with_only_columns(
                kjoretoy.c.merke_navn,
                kjoretoy.c.farge_navn,
                kjoretoy.c.tekn_modell,
                kjoretoy.c.elbil
            ).where(
                kjoretoy.c.tekn_reg_f_g_n == literal(regdato))
        )

        out_list = []
        for r in res:
            out = {
                "merke": r[0],
                "farge": r[1],
                "modell": r[2],
                "elbil": r[3]
            }
            out_list.append(out)

        return out_list

@app.get("/pkkdato/{pkk_dato}")
async def pkkdato(pkk_dato: str = Path(..., title="Dato for neste PKK", description="Dato for neste planlagte EU-kontroll i formatet YYYY-MM-DD")):
    with engine.connect() as conn:
        res = conn.execute(
            kjoretoy.select().with_only_columns(
                 kjoretoy.c.farge_navn,
                 kjoretoy.c.tekn_modell,
                 kjoretoy.c.merke_navn,
                 kjoretoy.c.elbil,
                 kjoretoy.c.tekn_reg_f_g_n
            ).where(kjoretoy.c.tekn_neste_pkk == pkk_dato)  # Anta at pkk_dato er i riktig format
        )

        out_list = []
        for r in res:
            out = {
                "farge": r[0],
                "modell": r[1],
                "merke": r[2],
                "elbil": r[3],
                "forstegangsregistrering": r[4].isoformat() if r[4] else None
            }
            out_list.append(out)

        return out_list

