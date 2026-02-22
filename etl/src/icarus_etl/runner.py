import click
from neo4j import GraphDatabase

from icarus_etl.pipelines.cnpj import CNPJPipeline
from icarus_etl.pipelines.sanctions import SanctionsPipeline
from icarus_etl.pipelines.transparencia import TransparenciaPipeline
from icarus_etl.pipelines.tse import TSEPipeline

PIPELINES: dict[str, type] = {
    "cnpj": CNPJPipeline,
    "tse": TSEPipeline,
    "transparencia": TransparenciaPipeline,
    "sanctions": SanctionsPipeline,
}


@click.group()
def cli() -> None:
    """ICARUS ETL — Data ingestion pipelines for Brazilian public data."""


@cli.command()
@click.option("--source", required=True, help="Pipeline: cnpj, tse, transparencia, sanctions")
@click.option("--neo4j-uri", default="bolt://localhost:7687", help="Neo4j URI")
@click.option("--neo4j-user", default="neo4j", help="Neo4j user")
@click.option("--neo4j-password", required=True, help="Neo4j password")
@click.option("--data-dir", default="./data", help="Directory for downloaded data")
def run(source: str, neo4j_uri: str, neo4j_user: str, neo4j_password: str, data_dir: str) -> None:
    """Run an ETL pipeline."""
    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

    if source not in PIPELINES:
        available = ", ".join(PIPELINES.keys())
        raise click.ClickException(f"Unknown source: {source}. Available: {available}")

    pipeline_cls = PIPELINES[source]
    pipeline = pipeline_cls(driver=driver, data_dir=data_dir)
    pipeline.run()

    driver.close()


@cli.command()
def sources() -> None:
    """List available data sources."""
    click.echo("Available pipelines:")
    click.echo("  cnpj          - Receita Federal (Company Registry)")
    click.echo("  tse           - Tribunal Superior Eleitoral (Elections)")
    click.echo("  transparencia - Portal da Transparência (Federal Spending)")
    click.echo("  sanctions     - CEIS/CNEP/CEPIM/CEAF (Sanctions)")


if __name__ == "__main__":
    cli()
