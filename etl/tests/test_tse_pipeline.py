from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from icarus_etl.pipelines.tse import TSEPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> TSEPipeline:
    driver = MagicMock()
    pipeline = TSEPipeline(driver, data_dir=str(FIXTURES.parent))
    return pipeline


def _extract_from_fixtures(pipeline: TSEPipeline) -> None:
    """Extract from test fixtures instead of data_dir/tse/."""
    import pandas as pd

    pipeline._raw_candidatos = pd.read_csv(
        FIXTURES / "tse_candidatos.csv", encoding="latin-1", dtype=str
    )
    pipeline._raw_doacoes = pd.read_csv(
        FIXTURES / "tse_doacoes.csv", encoding="latin-1", dtype=str
    )


def test_pipeline_metadata() -> None:
    pipeline = _make_pipeline()
    assert pipeline.name == "tse"
    assert pipeline.source_id == "tribunal_superior_eleitoral"


def test_transform_produces_candidates() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    assert len(pipeline.candidates) == 2
    cpfs = {c["cpf"] for c in pipeline.candidates}
    assert "123.456.789-01" in cpfs
    assert "987.654.321-00" in cpfs


def test_transform_normalizes_names() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    names = {c["name"] for c in pipeline.candidates}
    assert "JOAO DA SILVA" in names
    assert "MARIA JOSE SANTOS" in names


def test_transform_creates_elections() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    assert len(pipeline.elections) == 3
    years = {e["year"] for e in pipeline.elections}
    assert years == {2022, 2024}


def test_transform_parses_donation_values() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    assert len(pipeline.donations) == 3
    valores = sorted(d["valor"] for d in pipeline.donations)
    assert valores[0] == 200.00
    assert valores[1] == 1500.50
    assert valores[2] == 50000.00


def test_transform_identifies_company_donors() -> None:
    pipeline = _make_pipeline()
    _extract_from_fixtures(pipeline)
    pipeline.transform()

    company_donations = [d for d in pipeline.donations if d["donor_is_company"]]
    person_donations = [d for d in pipeline.donations if not d["donor_is_company"]]

    assert len(company_donations) == 1
    assert company_donations[0]["donor_name"] == "EMPRESA ABC LTDA"
    assert len(person_donations) == 2
