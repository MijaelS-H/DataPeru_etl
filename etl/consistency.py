#!/usr/bin/env python3

"""Consistency test generation and running for bamboo-lib ETL scripts.
"""

import argparse
import pickle
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlencode, urljoin

import numpy as np
import pandas as pd
from bamboo_lib.models import PipelineStep

REPORT = {}

def dump_report(target_file: str):
    """Saves the generated reports to a pickle file.

    Arguments:
        target_file {str} --
            A path to save the pickle file.

    Returns:
        {void}

    Raises:
        IOError --
            If the `target_file` argument points to a file that already
            exists.
    """
    
    target_path = Path(target_file).resolve()
    with target_path.open("w+b") as fileio:
        pickle.dump(REPORT, fileio)

def load_report(target_file: str):
    global REPORT

    target_path = Path(target_file).resolve()
    if not target_path.exists():
        dump_report(target_file)

    with target_path.open("r+b") as fileio:
        REPORT = pickle.load(fileio)

class AggregatorStep(PipelineStep):
    """Generate aggregation reports for datasets passed through a bamboo-lib Pipeline."""

    def __init__(self, table_name: str, measures: list = None):
        """Creates a new AggregatorStep instance.

        Arguments:
            table_name {str} --
                The name of the table on the database where the data will be ingested.
            measures {List[str]} --
                A list of the column names on the dataframe intended to be used
                as Measures in Tesseract.
                If this list is empty, this step does nothing.
        """

        super().__init__(table_name=table_name)
        self.table_name = table_name
        self.measures = set(measures) if measures is not None else set()
        self.levels = set()

    def run_step(self, prev_result: pd.DataFrame, params: dict):
        """Executes the main function of the step.

        Arguments:
            prev_result {pd.DataFrame} --
                The output of the previous step in the pipeline.
                For this step to work, must be a pandas DataFrame.
            params {Dict[str, Any]} --
                The original parameters passed into the pipeline.

        Returns:
            {pd.DataFrame} --
                An `object` that is the result of the processing of this step.
                For this step to work, must be exactly the same object received
                in the `prev_result` argument.
        """

        # skip step if no measures were set
        if len(self.measures) == 0:
            return prev_result

        df = prev_result
        columns = set(df.columns.values.tolist())
        self.levels = columns - self.measures

        levels = list(self.levels)
        measures = list(self.measures)

        reports = {}
        for level in levels:
            frame = [level, *measures]
            grouped = df[frame].groupby(level)
            grouped_sum = grouped.sum()
            reports[level] = {
                # "count": grouped.count().describe(),
                "groups": grouped_sum,
                "sum": grouped_sum.describe(),
            }

        REPORT[self.table_name] = {
            "reports": reports,
            "levels": levels,
            "measures": measures,
            "table": self.table_name,
            "total_count": df.size,
            # "total_mean": df[measures].mean(skipna=True).to_dict(),
            # "total_sum": df[measures].sum().to_dict(),
        }

        return df

    @staticmethod
    def load_report(target_file: str):
        load_report(target_file)

    @staticmethod
    def dump_report(target_file: str):
        dump_report(target_file)


class CubeTester:
    """Class to execute the testing for a specific cube, which maps to a table in the database."""

    def __init__(self, cube: ET.Element, shared_dims: List[ET.Element]):
        table_name = cube.find("Table").get("name")

        self.cube_name = cube.get("name")
        self.levels = [lvl for lvl in get_levels(cube, shared_dims)]
        self.measures = [msr.attrib for msr in cube.findall("Measure")]
        self.table_name = table_name

    def test_report(self, server_url: str, report: dict):
        """Executes the testing for the specified tesseract-olap server,
        comparing with the provided report.

        Arguments:
            server_url {str} --
                URL to the tesseract-olap server.
            report {Dict[str, any]} --
                The report generated during the ingestion process.

        Return:
            {void}
        """

        print(f"\nSe ejecutarán las pruebas para el cubo '{self.cube_name}'")

        report_results = report["reports"]
        report_measures = report["measures"]
        report_levels = report["levels"]

        measure_names = [
            mea["name"] for mea in self.measures if mea["column"] in report_measures
        ]

        request_url = urljoin(server_url, "cubes/%s/aggregate.csv" % self.cube_name)

        for level in self.levels:
            level_name = level["name"]

            if level["key_column"] not in report_levels:
                continue

            print(f"\nPrueba sobre datos agrupados en nivel '{level_name}'")

            query = {
                "drilldowns[]": [join_fullname(level)],
                "measures[]": measure_names,
                "parents": "true",
            }
            query_url = f"{request_url}?{urlencode(query, True)}"
            try:
                df = pd.read_csv(query_url, index_col=level_name)
            except:
                print("ERROR: Tesseract presenta error interno en la URL ", query_url)
                continue

            print(f"  Dataset obtenido desde API: {query_url}")

            report_df = report_results[level["key_column"]]

            for measure in self.measures:
                # print("")

                if measure["column"] not in report_measures:
                    continue
                if measure["aggregator"] != "sum":
                    print("  Saltando medición tipo {m[aggregator]}: '{m[name]}'".format(m=measure))
                    continue

                # print("  Para medición '%s':" % measure["name"])

                try:
                    series_raw = report_df[measure["aggregator"]][measure["column"]]
                except KeyError:
                    df = report_df[measure["aggregator"]]
                    missing_cols = set(msr["column"] for msr in self.measures) - set(df.columns)
                    print("ERROR: Faltan columnas en el reporte de ingestión: ", missing_cols)
                    break
                series_tes = df[measure["name"]].describe()

                for idx in series_raw.index:
                    print("")
                    print(f"    Estadístico: {idx.upper()}")

                    value_origin = series_raw[idx]
                    value_tesseract = series_tes[idx]
                    print(f"    Valor para dataset antes de ingestión: {value_origin}")
                    print(f"    Valor para dataset obtenido desde API: {value_tesseract}")

                    if np.isnan(value_origin) or np.isnan(value_tesseract):
                        is_match = "SI" if value_origin == value_tesseract else "NO"
                        print(f"    Ambos valores nulos: {is_match}")
                    else:
                        err = abs(value_tesseract - value_origin) / value_origin
                        is_match = "SI" if err < 0.0001 or () else "NO"
                        print(f"    Concordancia (Error < 0.01%): {is_match}")

    def test_raise(self):
        pass


def join_fullname(lvl):
    """Returns a Tesseract OLAP Level full name, according to naming instructions.

    Arguments:
        lvl {dict} --
            A dict with information about the level.

    Returns:
        {str}
    """
    tokens = (lvl["dimension"], lvl["hierarchy"], lvl["name"])
    if "." in "|".join(tokens):
        tokens = ("[{}]".format(token) for token in tokens)
    return ".".join(tokens)


def get_levels(cube: ET.Element, shared_dims: Dict[str, ET.Element]):
    """Generates a dict with info about each level for all dimensions in the cube.

    It also requires a dict with all the available SharedDimension in the
    schema.

    Arguments:
        cube {ET.Element} --
            The Cube ET.Element to work with.
        shared_dims {Dict[str, ET.Element]} --
            A dict where keys are the name a of a SharedDimension and the value
            is the SharedDimension itself.

    Returns:
        {Iterator[dict]} --
            A dict with information about the level.
    """

    def process_dimension(dimens: ET.Element, dimref: Optional[ET.Element] = None):
        """Generates a dict with info about each level in the dimension.

        Arguments:
            dimens {ET.Element} --
                The Dimension ET.Element which actually contains Hierarchies and
                Levels.
            dimref {Optional[ET.Element]} --
                The Dimension/DimensionUsage directly inside the Cube in the
                schema.

        Returns:
            {Iterator[dict]} --
                A dict with information about the level.
        """
        dimref = dimens if dimref is None else dimref
        dim_name = dimref.get("name")
        dim_fkey = dimref.get("foreign_key")

        for hie in dimens.findall("Hierarchy"):
            hie_name = hie.get("name")

            for lvl in hie.findall("Level"):
                yield {
                    "dim_fkey": dim_fkey,
                    "dimension": dim_name,
                    "hierarchy": hie_name,
                    **lvl.attrib,
                }

    for dim in cube.findall("DimensionUsage"):
        try:
            dimension = shared_dims[dim.get("source")]
            yield from process_dimension(dimension, dim)
        except KeyError:
            print("ERROR: No se encuentra SharedDimension ", dim.get("source"))
            continue

    for dim in cube.findall("Dimension"):
        yield from process_dimension(dim)


if __name__ == "__main__":
    argparser = argparse.ArgumentParser(
        description="Execute the consistency test, using the results from the ingestion process."
    )
    argparser.add_argument(
        "--report",
        help="The path to the folder containing the ingestion aggregator reports.",
        metavar="path/to/report.pickle",
        required=True,
        type=Path,
    )
    argparser.add_argument(
        "--schema",
        help="The path to Tesseract's XML schema file.",
        metavar="path/to/schema.xml",
        required=True,
        type=Path,
    )
    argparser.add_argument(
        "--server",
        help="The URL to the Tesseract OLAP server. Must be running the same schema as the parameter.",
        metavar="http://localhost:7777",
        type=str,
    )
    argparser.add_argument(
        "-o",
        "--output",
        default="./consistency.txt",
        help="The path to the file where the test report will be written.",
        metavar="./consistency.txt",
        type=Path,
    )
    args = argparser.parse_args()

    report_path = args.report.resolve()
    if not report_path.exists() or not report_path.is_file():
        print("The specified report file is not valid: %s" % report_path)
        sys.exit()

    schema_path = args.schema.resolve()
    if not schema_path.exists() or not schema_path.is_file():
        print("The specified schema file is not valid: %s" % schema_path)
        sys.exit()

    output_path = args.output.resolve()
    if output_path.exists():
        print("The specified output file already exists: %s" % output_path)
        sys.exit()

    reports = pickle.load(file=report_path.open("rb"))
    sys.stdout = output_path.open("w+")

    tree = ET.parse(schema_path)
    schema = tree.getroot()

    shared_dims = {sh.get("name"): sh for sh in schema.findall("SharedDimension")}

    print("Iniciando tests de consistencia")
    for cube in schema.findall("Cube"):
        table_name = cube.find("Table").get("name")
        try:
            report = reports[table_name]
        except KeyError:
            print("ERROR: El cubo %s no aparece en el reporte de ingestión." % cube.get("name"))
            continue

        tester = CubeTester(cube, shared_dims)
        tester.test_report("http://localhost:7777", report)
