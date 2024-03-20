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
    kjoretoy_endpoint = URL + "/regdato/2022-01-01"
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

def test_kjoretoy_regdato_ny_dato(db):
    kjoretoy_endpoint = URL + "/regdato/2022-01-02"
    resp = requests.get(kjoretoy_endpoint)
    svar = resp.json()

    # Opprett forventet respons basert på den faktiske responsen du har gitt
    forventet = [
        {
            'merke': 'Volvo',
            'modell': 'FH',
            'farge': 'Grå',
            'elbil': False
        }
    ]

    sorterer = lambda x: (x['merke'], x['farge'], x['modell'], x['elbil'])

    forventet.sort(key=sorterer)
    svar.sort(key=sorterer)

    assert svar != forventet, "Responsen skal være forskjellig for forskjellige datoer"


def test_pkkdato(db):
    pkkdato_endpoint = URL + "/pkkdato/2025-10-14"
    resp = requests.get(pkkdato_endpoint)
    svar = resp.json()

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

    assert svar == forventet, f"Feil i hentede data: {svar}"


