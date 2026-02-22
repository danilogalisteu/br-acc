from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from icarus_etl.base import Pipeline

if TYPE_CHECKING:
    from neo4j import Driver
from icarus_etl.loader import Neo4jBatchLoader
from icarus_etl.transforms import deduplicate_rows, format_cnpj, normalize_name


class CNPJPipeline(Pipeline):
    """ETL pipeline for Receita Federal CNPJ open data."""

    name = "cnpj"
    source_id = "receita_federal"

    def __init__(self, driver: Driver, data_dir: str = "./data") -> None:
        super().__init__(driver, data_dir)
        self._raw_empresas: pd.DataFrame = pd.DataFrame()
        self._raw_socios: pd.DataFrame = pd.DataFrame()
        self.companies: list[dict[str, Any]] = []
        self.partners: list[dict[str, Any]] = []
        self.relationships: list[dict[str, Any]] = []

    def extract(self) -> None:
        cnpj_dir = Path(self.data_dir) / "cnpj"
        self._raw_empresas = pd.read_csv(
            cnpj_dir / "empresas.csv",
            dtype=str,
            keep_default_na=False,
        )
        self._raw_socios = pd.read_csv(
            cnpj_dir / "socios.csv",
            dtype=str,
            keep_default_na=False,
        )

    def transform(self) -> None:
        companies: list[dict[str, Any]] = []
        for _, row in self._raw_empresas.iterrows():
            companies.append({
                "cnpj": format_cnpj(str(row["cnpj"])),
                "razao_social": normalize_name(str(row["razao_social"])),
                "cnae_principal": str(row["cnae_principal"]),
                "capital_social": str(row["capital_social"]),
                "uf": str(row["uf"]),
                "municipio": str(row["municipio"]),
            })
        self.companies = deduplicate_rows(companies, ["cnpj"])

        partners: list[dict[str, Any]] = []
        relationships: list[dict[str, Any]] = []
        for _, row in self._raw_socios.iterrows():
            cnpj = format_cnpj(str(row["cnpj"]))
            nome = normalize_name(str(row["nome_socio"]))
            cpf = str(row["cpf_socio"])
            tipo = str(row["tipo_socio"])

            partners.append({"name": nome, "cpf": cpf})
            relationships.append({
                "source_key": cpf,
                "target_key": cnpj,
                "tipo_socio": tipo,
            })

        self.partners = deduplicate_rows(partners, ["cpf"])
        self.relationships = relationships

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if self.companies:
            loader.load_nodes("Company", self.companies, key_field="cnpj")

        if self.partners:
            loader.load_nodes("Person", self.partners, key_field="cpf")

        if self.relationships:
            loader.load_relationships(
                rel_type="SOCIO_DE",
                rows=self.relationships,
                source_label="Person",
                source_key="cpf",
                target_label="Company",
                target_key="cnpj",
                properties=["tipo_socio"],
            )
