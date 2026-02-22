from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

from icarus_etl.pipelines.cnpj import CNPJPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline(data_dir: str = str(FIXTURES.parent)) -> CNPJPipeline:
    driver = MagicMock()
    return CNPJPipeline(driver=driver, data_dir=data_dir)


def _load_and_transform(pipeline: CNPJPipeline) -> None:
    """Load raw CSVs from fixtures into pipeline and run transform."""
    pipeline._raw_empresas = pd.read_csv(
        FIXTURES / "cnpj_empresas.csv", dtype=str, keep_default_na=False,
    )
    pipeline._raw_socios = pd.read_csv(
        FIXTURES / "cnpj_socios.csv", dtype=str, keep_default_na=False,
    )
    pipeline.transform()


def test_pipeline_metadata() -> None:
    pipeline = _make_pipeline()
    assert pipeline.name == "cnpj"
    assert pipeline.source_id == "receita_federal"


def test_transform_produces_correct_company_dicts() -> None:
    pipeline = _make_pipeline()
    _load_and_transform(pipeline)

    assert len(pipeline.companies) == 3
    first = pipeline.companies[0]
    assert "cnpj" in first
    assert "razao_social" in first
    assert "cnae_principal" in first
    assert "capital_social" in first
    assert "uf" in first
    assert "municipio" in first


def test_transform_normalizes_names() -> None:
    pipeline = _make_pipeline()
    _load_and_transform(pipeline)

    names = [c["razao_social"] for c in pipeline.companies]
    for name in names:
        assert name == name.upper()
        assert "  " not in name


def test_transform_formats_cnpj() -> None:
    pipeline = _make_pipeline()
    _load_and_transform(pipeline)

    cnpjs = [c["cnpj"] for c in pipeline.companies]
    for cnpj in cnpjs:
        assert "/" in cnpj
        assert "-" in cnpj


def test_transform_deduplicates_by_cnpj() -> None:
    pipeline = _make_pipeline()
    pipeline._raw_empresas = pd.DataFrame([
        {
            "cnpj": "00000000000191",
            "razao_social": "Banco do Brasil",
            "cnae_principal": "6421200",
            "capital_social": "7500000000",
            "uf": "DF",
            "municipio": "Brasilia",
        },
        {
            "cnpj": "00000000000191",
            "razao_social": "Banco do Brasil (duplicate)",
            "cnae_principal": "6421200",
            "capital_social": "7500000000",
            "uf": "DF",
            "municipio": "Brasilia",
        },
    ])
    pipeline._raw_socios = pd.DataFrame(columns=["cnpj", "nome_socio", "cpf_socio", "tipo_socio"])
    pipeline.transform()

    assert len(pipeline.companies) == 1


def test_transform_extracts_partners() -> None:
    pipeline = _make_pipeline()
    _load_and_transform(pipeline)

    assert len(pipeline.partners) == 3
    partner_names = [p["name"] for p in pipeline.partners]
    assert "JOAO DA SILVA" in partner_names
    assert "MARIA SANTOS" in partner_names


def test_transform_extracts_relationships() -> None:
    pipeline = _make_pipeline()
    _load_and_transform(pipeline)

    assert len(pipeline.relationships) == 3
    rel = pipeline.relationships[0]
    assert "source_key" in rel
    assert "target_key" in rel
    assert "tipo_socio" in rel
