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
    """
    CLI command to generate extract dag (extract data from api for each sources and orchestartor)
    py .\template\render_scripts.py gendagextract
    """

    # Jinja template name
    orc_tpl_name = "extract_orchestrator_fakestore_dag.py.j2"
    dag_tpl_name = "extract_fakestore_dag.py.j2"

    # Yaml template name & location
    cfg_base = "template/dag/config/extract_fakestore_resources"
    cfg_path = f"{cfg_base}.yaml"
    cfg = get_config(cfg_path)

    # Get the meta
    project   = cfg.get("project")
    base_url  = cfg.get("base_url")
    resources = cfg.get("resources")

    # For numerical purposes in dag name
    enriched = []
    for i, r in enumerate(resources, start=1):
        rr = dict(r)
        rr.setdefault("order", i)
        enriched.append(rr)

    # Template directory
    tpl_dir = "template/dag"
    orc_template = get_template(tpl_dir, orc_tpl_name)
    dag_template = get_template(tpl_dir, dag_tpl_name)

    # ds
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

    # Output location
    outdir = os.path.join("airflow", "dags")
    os.makedirs(outdir, exist_ok=True)

    # Orchestrator
    orch_out = os.path.join(outdir, f"00_dag_{project}_orchestrator.py")
    with open(orch_out, "w", encoding="utf-8") as f:
        f.write(orc_template.render(ctx_common))
    click.echo(f"Rendered orchestrator → {orch_out}")

    # Per-resource DAGs
    for r in enriched:
        content = dag_template.render({**ctx_common, "resource": r})
        dag_out = os.path.join(
            outdir,
            f"00_{r['order']:02d}_dag_{project}_extract_{r['name']}.py"
        )
        with open(dag_out, "w", encoding="utf-8") as f:
            f.write(content)
        click.echo(f"Rendered DAG → {dag_out}")


@cli.command()
def gendagtransform():
    """
    CLI command to generate transform dag (transform for each json sources data to parquet and orchestartor)
    py .\template\render_scripts.py gendagtransform
    """

    # Jinja template name
    orc_tpl_name = "transform_orchestrator_fakestore_dag.py.j2"
    dag_tpl_name = "transform_fakestore_dag.py.j2"

    # Yaml template name & location
    cfg_path = "template/dag/config/extract_fakestore_resources.yaml"
    cfg = get_config(cfg_path)

    # Get the meta
    project = cfg.get("project")
    resources = cfg.get("resources") or []

    # For numerical purposes in dag name
    enriched = []
    for i, r in enumerate(resources, start=1):
        rr = dict(r)
        rr.setdefault("order", i)
        enriched.append(rr)

    # Template directory
    tpl_dir = "template/dag"
    orc_template = get_template(tpl_dir, orc_tpl_name)
    dag_template = get_template(tpl_dir, dag_tpl_name)

    # ds
    ds_literal = "{{ ds }}"

    class _MacroConf:
        def get(self, *_, **__):
            return "{{ dag_run.conf.get('ds', ds) }}"

    class _MacroDagRun:
        def __init__(self):
            self.conf = _MacroConf()

    ctx_common = {
        "project": project,
        "resources": enriched,
        "ds": ds_literal,
        "dag_run": _MacroDagRun(),
    }

    # Output location
    outdir = os.path.join("airflow", "dags")
    os.makedirs(outdir, exist_ok=True)

    # Orchestrator
    orch_out = os.path.join(outdir, f"01_dag_{project}_transform_orchestrator.py")
    with open(orch_out, "w", encoding="utf-8") as f:
        f.write(orc_template.render(ctx_common))
    click.echo(f"Rendered orchestrator → {orch_out}")

    # Per-resource DAGs
    for r in enriched:
        content = dag_template.render({**ctx_common, "resource": r})
        dag_out = os.path.join(
            outdir,
            f"01_{r['order']:02d}_dag_{project}_transform_{r['name']}.py",
        )
        with open(dag_out, "w", encoding="utf-8") as f:
            f.write(content)
        click.echo(f"Rendered DAG → {dag_out}")


@cli.command()
def gendagload() -> None:
    """
    CLI command to generate load dag (load for each sources to db and orchestartor)
    py .\template\render_scripts.py gendagextract
    """

    # --- Template names ---
    orc_tpl_name = "load_orchestrator_fakestore_dag.py.j2"
    dag_tpl_name = "load_fakestore_dag.py.j2"

    # --- Config (reuse the same file used by extract/transform) ---
    cfg_base = "template/dag/config/extract_fakestore_resources"
    cfg_path = f"{cfg_base}.yaml"
    cfg = get_config(cfg_path)

    project: str = cfg.get("project", "fakestore")
    resources: list[dict] = cfg.get("resources", [])

    # Enrich with stable ordering (1-based)
    enriched: list[dict] = []
    for i, r in enumerate(resources, start=1):
        rr = dict(r)
        rr.setdefault("order", i)
        enriched.append(rr)

    # --- Load templates ---
    tpl_dir = "template/dag"
    orc_template = get_template(tpl_dir, orc_tpl_name)
    dag_template = get_template(tpl_dir, dag_tpl_name)

    # Airflow macro placeholders so Jinja in the DAG body resolves correctly
    ds_literal = "{{ ds }}"

    class _MacroConf:
        def get(self, *_args, **_kwargs):
            # keep extract/transform style (allow dag_run.conf['ds'] override)
            return "{{ dag_run.conf.get('ds', ds) }}"

    class _MacroDagRun:
        def __init__(self):
            self.conf = _MacroConf()

    ctx_common = {
        "project": project,
        "resources": enriched,
        "ds": ds_literal,
        "dag_run": _MacroDagRun(),
    }

    outdir = os.path.join("airflow", "dags")
    os.makedirs(outdir, exist_ok=True)

    # --- Orchestrator ---
    orch_out = os.path.join(outdir, f"02_dag_{project}_load_orchestrator.py")
    with open(orch_out, "w", encoding="utf-8") as f:
        f.write(orc_template.render(ctx_common))
    click.echo(f"Rendered orchestrator → {orch_out}")

    # --- Per-resource DAGs ---
    for r in enriched:
        content = dag_template.render({**ctx_common, "resource": r})
        dag_out = os.path.join(
            outdir,
            f"02_{r['order']:02d}_dag_{project}_load_{r['name']}.py",
        )
        with open(dag_out, "w", encoding="utf-8") as f:
            f.write(content)
        click.echo(f"Rendered DAG → {dag_out}")

# --------------------------------------------------------------------------------
# ETL script rendering command
# --------------------------------------------------------------------------------
@cli.command()
def genscriptextract():
    """
    Render extract scripts from Jinja
    """
    # filename
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
    
    # Output directory
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
def genscripttransform():
    # yaml config file location & get config
    cfg_path = "template/script/config/extract_fakestore_config.yaml"
    cfg = get_config(cfg_path)

    extract = (cfg or {}).get("extract", {})
    if not extract:
        click.echo(f"No resources found in {cfg_path}", err=True)
        return
    
    # template location & name
    tpl_dir = "template/script"
    tpl_name = "transform_fakestore_to_parquet.py.j2"
    template = get_template(tpl_dir, tpl_name)

    outdir = os.path.join("scripts", "transform")
    os.makedirs(outdir, exist_ok=True)

    # Render transform script
    for key, r in extract.items():
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


@cli.command()
def genscriptload():
    """
    Render loader scripts from Jinja
    """

    # Template dir. & name & config yaml
    tpl_dir = "template/script"
    tpl_name = "load_fakestore_to_l1.py.j2"
    cfg_path = "template/script/config/extract_fakestore_config.yaml"

    # Load script config
    cfg = get_config(cfg_path)
    resources = cfg.get("extract", {})

    # Defaults per resource --> can be adjusted in yaml file
    load_defaults = {
        "products": {
            "target_table": "dim_products",
            "pk": ["product_id"],
            "load_mode": "upsert",
        },
        "users": {
            "target_table": "dim_users",
            "pk": ["user_id"],
            "load_mode": "upsert",
        },
        "carts": {
            "target_table": "fct_cart_items",
            "pk": ["cart_id", "product_id", "ds"],
            "load_mode": "upsert",
        },
    }

    # Get template & location output
    template = get_template(tpl_dir, tpl_name)

    outdir = os.path.join("scripts", "load")
    os.makedirs(outdir, exist_ok=True)

    # Render process
    for key, spec in resources.items():
        name = spec.get("resource_name", key)

        meta = load_defaults.get(name, {})
        ctx = {
            "resource": {
                "name": name,
                "target_table": meta.get("target_table", name),
                "pk": meta.get("pk", ["id"]),
                "load_mode": meta.get("load_mode", "upsert"),
            }
        }

        outpath = os.path.join(outdir, f"load_{name}_parquet_to_l1.py")
        with open(outpath, "w", encoding="utf-8") as f:
            f.write(template.render(ctx))
        click.echo(f"Rendered loader → {outpath}")


if __name__ == "__main__":
    cli()
