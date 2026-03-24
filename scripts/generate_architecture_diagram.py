#!/usr/bin/env python3
"""Generate architecture diagram matching reference style exactly:
- White background, dashed module borders, clear fill
- Real service icons (AWS architecture icons)
- Left-to-right data flow
- Clean grid layout
- No client names (NDA compliant)
"""

from diagrams import Cluster, Diagram, Edge
from diagrams.aws.storage import SimpleStorageServiceS3 as S3
from diagrams.aws.analytics import Glue, Athena, Redshift, LakeFormation
from diagrams.aws.compute import Lambda
from diagrams.aws.integration import SimpleNotificationServiceSns as SNS
from diagrams.aws.management import Cloudwatch
from diagrams.aws.security import IdentityAndAccessManagementIam as IAM
from diagrams.onprem.workflow import Airflow
from diagrams.onprem.iac import Terraform
from diagrams.custom import Custom

OUT = "/home/houta/Projects/aws_fintech_enterprise/upwork_media/00_architecture_diagram"
STREAMLIT_ICON = "/tmp/streamlit_icon.png"

# ── Reference-matching styles ────────────────────────────────────────────────
MODULE = {
    "style": "dashed",
    "bgcolor": "transparent",
    "color": "#64748B",
    "penwidth": "1.5",
    "fontsize": "13",
    "fontcolor": "#1E293B",
    "fontname": "Helvetica-Bold",
    "labeljust": "l",
    "margin": "24",
}

THIN = {"color": "#94A3B8", "penwidth": "1.5"}
LABEL = {"fontsize": "9", "fontcolor": "#64748B", "fontname": "Helvetica"}
INVIS = {"style": "invis", "minlen": "1"}

with Diagram(
    "",
    filename=OUT,
    show=False,
    direction="LR",
    outformat="png",
    graph_attr={
        "bgcolor": "white",
        "pad": "1.2",
        "ranksep": "1.6",
        "nodesep": "1.0",
        "fontname": "Helvetica-Bold",
        "fontsize": "22",
        "fontcolor": "#0F172A",
        "label": "AWS Data Engineering Pipeline\nEnd-to-End Fintech Card Transaction Processing\n ",
        "labelloc": "t",
        "dpi": "200",
        "size": "26,14!",
        "ratio": "compress",
        "newrank": "true",
        "compound": "true",
    },
    node_attr={
        "fontname": "Helvetica",
        "fontsize": "10",
        "fontcolor": "#334155",
    },
):

    # ── Row 1: Main data flow (L → R) ───────────────────────────────────

    with Cluster("Data Ingestion", graph_attr=MODULE):
        s3_raw = S3("S3 Raw Zone\n(CSV)")
        trigger = Lambda("Lambda\nTrigger")

    with Cluster("Data Processing (Glue)", graph_attr=MODULE):
        crawler = Glue("Glue\nCrawler")
        etl = Glue("Glue ETL\n(PySpark)")
        s3_cur = S3("S3 Curated\n(Parquet)")

    with Cluster("Data Warehouse", graph_attr=MODULE):
        redshift = Redshift("Redshift\nServerless")
        athena = Athena("Athena\nSQL")

    with Cluster("Analytics & BI", graph_attr=MODULE):
        dash = Custom("Streamlit\nDashboard", STREAMLIT_ICON)

    # ── Row 2: Supporting services ───────────────────────────────────────

    with Cluster("Orchestration & Alerts", graph_attr=MODULE):
        airflow = Airflow("Apache Airflow\n(Docker)")
        sns = SNS("SNS\nAlerts")

    with Cluster("Security & Governance", graph_attr=MODULE):
        lake = LakeFormation("Lake\nFormation")
        iam = IAM("IAM\nRoles")

    with Cluster("Infrastructure as Code", graph_attr=MODULE):
        tf = Terraform("Terraform\nIaC")
        cw = Cloudwatch("CloudWatch\nAlarms")

    # ── Main data flow edges (solid, L → R, high weight) ───────────────
    s3_raw >> Edge(label="event", weight="10", **LABEL, **THIN) >> trigger
    trigger >> Edge(label="starts", weight="10", **LABEL, **THIN) >> crawler
    crawler >> Edge(label="catalog", weight="10", **LABEL, **THIN) >> etl
    etl >> Edge(label="write", weight="10", **LABEL, **THIN) >> s3_cur
    s3_cur >> Edge(label="COPY", weight="10", **LABEL, **THIN) >> redshift
    s3_cur >> Edge(label="query", weight="5", **LABEL, **THIN) >> athena
    redshift >> Edge(weight="10", **THIN) >> dash
    athena >> Edge(weight="5", **THIN) >> dash

    # ── Supporting edges (dashed, low weight) ────────────────────────────
    airflow >> Edge(style="dashed", label="orchestrate", **LABEL, **THIN) >> etl
    airflow >> Edge(style="dashed", **THIN) >> sns
    lake >> Edge(style="dashed", label="govern", **LABEL, **THIN) >> redshift
    iam >> Edge(style="dashed", **THIN) >> lake
    tf >> Edge(style="dashed", label="provision", constraint="false", **LABEL, **THIN) >> s3_raw
    cw >> Edge(style="dashed", constraint="false", **THIN) >> sns

print(f"Saved: {OUT}.png")
