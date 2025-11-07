####
## Gramedia Digital - Data Engineer Take Home Test
## by Mario Caesar // caesarmario87@gmail.com
## Python script to render DAGs and ETL scripts
####

# Importing Libraries
from jinja2 import Environment, FileSystemLoader

import os
import click
import yaml


def get_config(path):
    """
    Load YAML configuration file.

    Args:
        path (str): Filesystem path to the YAML config.

    Returns:
        dict: Parsed config or empty dict if file is missing or invalid.
    """
    if not os.path.isfile(path):
        click.echo(f"[WARN] Config not found: {path}", err=True)
        return {}
    with open(path, "r") as f:
        return yaml.safe_load(f)


def get_template(search_path, name):
    """
    Retrieve a Jinja2 template by name.

    Args:
        search_path (str): Directory where templates reside.
        name (str): Template filename to load.

    Returns:
        jinja2.Template: Compiled template object.
    """
    env = Environment(
        loader = FileSystemLoader(os.path.abspath(search_path)),
        trim_blocks = True,
        lstrip_blocks = True,
    )
    return env.get_template(name)


@click.group()
def cli():
    """
    CLI group for rendering DAGs and scripts.
    """
    pass

# --------------------------------------------------------------------------------
# DAG rendering commands
# --------------------------------------------------------------------------------
@cli.command()
def gendagextract():

    orc_tpl_name = "extract_orchestrator_fakestore_dag.py.j2"
    dag_tpl_name = "extract_fakestore_dag.py.j2"

    cfg_base = "template/dag/config/extract_fakestore_resources"
    cfg_path = f"{cfg_base}.yaml"
    cfg = get_config(cfg_path)

    project   = cfg.get("project")
    base_url  = cfg.get("base_url")
    resources = cfg.get("resources")

    enriched = []
    for i, r in enumerate(resources, start=1):
        rr = dict(r)
        rr.setdefault("order", i)
        enriched.append(rr)

    tpl_dir = "template/dag"
    orc_template = get_template(tpl_dir, orc_tpl_name)
    dag_template = get_template(tpl_dir, dag_tpl_name)

    ds_literal = "{{ ds }}"

    class _MacroConf:
        def get(self, *args, **kwargs):
            return "{{ dag_run.conf.get('ds', ds) }}"
    class _MacroDagRun:
        def __init__(self):
            self.conf = _MacroConf()

    ctx_common = {
        "project": project,
        "base_url": base_url,
        "resources": enriched,
        "ds": ds_literal,
        "dag_run": _MacroDagRun(),
    }

    outdir = os.path.join("airflow", "dags")
    os.makedirs(outdir, exist_ok=True)

    orch_out = os.path.join(outdir, f"00_dag_{project}_orchestrator.py")
    with open(orch_out, "w", encoding="utf-8") as f:
        f.write(orc_template.render(ctx_common))
    click.echo(f"Rendered orchestrator → {orch_out}")

    for r in enriched:
        content = dag_template.render({**ctx_common, "resource": r})
        dag_out = os.path.join(
            outdir,
            f"00_{r['order']:02d}_dag_{project}_extract_{r['name']}.py"
        )
        with open(dag_out, "w", encoding="utf-8") as f:
            f.write(content)
        click.echo(f"Rendered DAG → {dag_out}")

# --------------------------------------------------------------------------------
# ETL script rendering command
# --------------------------------------------------------------------------------
@cli.command()
def genscriptextract():

    filename  = "extract_fakestore_template"
    config    = "extract_fakestore_config"
    
    # Load script config
    cfg_path  = f"template/script/config/{config}.yaml"
    cfg       = get_config(cfg_path)
    extracts = cfg.get("extract", {})

    # Load script template
    tpl_dir  = "template/script"
    tpl_name = f"{filename}.py.j2"
    template = get_template(tpl_dir, tpl_name)

    out_dir = "scripts/extract"
    os.makedirs(out_dir, exist_ok=True)

    # Loop & render one file per process
    for key, props in extracts.items():
        resource_name = props.get("resource_name", key)
        class_name    = props.get("class_name", resource_name.capitalize())
        endpoint_path = props.get("endpoint_path", resource_name)

        out_file = f"extract_{resource_name}.py"
        out_path = os.path.join(out_dir, out_file)

        template.stream(
            # names expected by your Jinja template
            class_name=class_name,
            resource_name=resource_name,
            endpoint_path=endpoint_path,
        ).dump(out_path)

        click.echo(f"Rendered script → {out_path}")

    click.echo("Done rendering ETL scripts.")


@cli.command()
def genscripttransform() -> None:
    cfg_path = "template/script/config/extract_fakestore_config.yaml"
    cfg = get_config(cfg_path)

    extract = (cfg or {}).get("extract", {})
    if not extract:
        click.echo(f"No resources found in {cfg_path}", err=True)
        return

    tpl_dir = "template/script"
    tpl_name = "transform_fakestore_to_parquet.py.j2"
    template = get_template(tpl_dir, tpl_name)

    outdir = os.path.join("scripts", "transform")
    os.makedirs(outdir, exist_ok=True)

    for key, r in extract.items():
        # prefer explicit resource_name from config; fallback to dict key
        res_name = (r or {}).get("resource_name") or key
        endpoint = (r or {}).get("endpoint_path") or ""

        ctx = {
            "resource": {
                "name": res_name,
                "endpoint_path": endpoint,
            }
        }

        out_path = os.path.join(outdir, f"transform_{res_name}_to_parquet.py")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(template.render(ctx))
        click.echo(f"Rendered transform script → {out_path}")


if __name__ == "__main__":
    cli()
