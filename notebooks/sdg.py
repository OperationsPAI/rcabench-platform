#!/usr/bin/env -S uv run marimo edit

import marimo

__generated_with = "0.13.11"
app = marimo.App(width="full", app_title="SDG Visualization")


@app.cell
def _():
    import marimo as mo

    import polars as pl

    return mo, pl


@app.cell
def _():
    from rcabench_platform.v1.spec.data import dataset_index_path

    return (dataset_index_path,)


@app.cell
def _(mo):
    mo.md(r"""# SDG Visualization""")
    return


@app.cell
def _(mo):
    all_datasets = [
        "rcaeval_re2_tt",
    ]
    dataset_dropdown = mo.ui.dropdown(
        options=all_datasets,
        searchable=True,
        label="dataset",
        value=all_datasets[0],
    )
    mo.output.append(dataset_dropdown)
    return (dataset_dropdown,)


@app.cell
def _(dataset_dropdown, dataset_index_path, mo, pl):
    dataset = dataset_dropdown.value
    _index_df = pl.read_parquet(dataset_index_path(dataset))

    _df = _index_df.select("datapack")

    datapack_table = mo.ui.table(_df, selection="single")
    mo.output.append(datapack_table)
    return datapack_table, dataset


@app.cell
def _(datapack_table, dataset, mo):
    from rcabench_platform.v1.graphs.sdg.build_ import build_sdg

    datapack = datapack_table.value[0, "datapack"]
    mo.stop(
        not isinstance(datapack, str), mo.md(f"## {mo.icon('ant-design:warning-outlined')} Please select a datapack")
    )

    mo.output.append("Building SDG ...")
    sdg = build_sdg(dataset, datapack)
    mo.output.append("Done!")

    mo.output.append({"|V|": sdg.num_nodes(), "|E|": sdg.num_edges()})
    return (sdg,)


@app.cell
def _(mo):
    neo4j_button = mo.ui.run_button(label="Export SDG to Neo4j")
    mo.output.append(neo4j_button)
    return (neo4j_button,)


@app.cell
def _(mo, neo4j_button, sdg):
    from rcabench_platform.v1.graphs.sdg.neo4j import export_sdg_to_neo4j

    if neo4j_button.value:
        mo.output.append("Exporting SDG to Neo4j ...")
        export_sdg_to_neo4j(sdg)
        mo.output.append("Done!")
        mo.output.append(mo.md("<http://localhost:7474/browser/>"))
    return


@app.cell
def _(mo):
    mo.md(r"""## Info""")
    return


@app.cell
def _(mo, sdg):
    inject_time = sdg.data["inject_time"]
    mo.output.append(mo.md("### inject time"))
    mo.output.append(inject_time)
    return


if __name__ == "__main__":
    app.run()
