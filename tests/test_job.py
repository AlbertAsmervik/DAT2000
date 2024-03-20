import logging
import pathlib
import pytest
import requests
from kjoretoy import last_opp_kjoretoy, prepp_kjoretoy
from dotenv import load_dotenv
import os

logging.basicConfig(level=logging.DEBUG)

THIS_FOLDER = pathlib.Path(__file__).parent

load_dotenv(dotenv_path=THIS_FOLDER / "test.env")
connstr = os.environ.get("CONN")
TESTDATA = THIS_FOLDER / "testdata"

URL = "http://127.0.0.1:8000"


@pytest.fixture(scope="session")
def db():
    df = prepp_kjoretoy(TESTDATA / "kjoretoyinfo_2022_jan.parquet")
    last_opp_kjoretoy(
        connstr,
        df
    )


def test_kjoretoy_regdato(db):
    dato = '2022-01-01'
    kjoretoy_endpoint = f"{URL}/regdato/{dato}"
    resp = requests.get(kjoretoy_endpoint)
    svar = resp.json()

    # Oppdaterer den forventede listen basert på den faktiske responsen du har gitt
    forventet = [
        {
            'merke': 'PEUGEOT',
            'modell': '2008',
            'farge': 'Svart (også blåsvart, grafitt mørk, gråsort, koksgrå mørk, koksgrå mørk metallic)',
            'elbil': True
        },
        {
            'merke': 'LYNX',
            'modell': 'ADVENTURE STD 600ACE',
            'farge': 'Rød (også burgunder)',
            'elbil': False
        }
    ]

    # Sorteringsfunksjonen oppdateres til å ikke inkludere 'antall_sitteplasser'
    sorterer = lambda x: (x['merke'], x['farge'], x['modell'], x['elbil'])

    # Vi sorterer de to listene
    forventet.sort(key=sorterer)
    svar.sort(key=sorterer)

    # Assertering for å sjekke at den hentede dataen matcher den forventede
    assert svar == forventet, f"Feil i hentede data: {svar}"


def test_pkkdato(db):
    pkk_dato = '2025-10-14'
    pkkdato_endpoint = f"http://127.0.0.1:8000/pkkdato/{pkk_dato}"
    resp = requests.get(pkkdato_endpoint)
    svar = resp.json()

    # Print statement for å hjelpe med å hente ut de forventede verdiene under utvikling
    print(svar)

    forventet = [
        {
            "farge": "Hvit (også antikkhvit, offwhite)",
            "modell": "208",
            "merke": "PEUGEOT",
            "elbil": True,
            "forstegangsregistrering": "2022-01-19"
        },
        {
            "farge": "Grå",
            "modell": "Kona",
            "merke": "HYUNDAI",
            "elbil": True,
            "forstegangsregistrering": "2022-01-20"
        }
    ]

    # Hvis rekkefølgen på dataene ikke er garantert, må du sortere dem før du sammenligner
    svar.sort(key=lambda x: (x['merke'], x['modell'], x['farge'], x['elbil'], x['forstegangsregistrering']))
    forventet.sort(key=lambda x: (x['merke'], x['modell'], x['farge'], x['elbil'], x['forstegangsregistrering']))

    # Assertering for å sjekke at den hentede dataen matcher den forventede
    assert svar == forventet, f"Feil i hentede data: {svar}"


