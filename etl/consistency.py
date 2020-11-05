import json
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List
from urllib.parse import urlencode, urljoin

import numpy as np
import pandas as pd
import requests
from bamboo_lib.models import PipelineStep


class AggregatorStep(PipelineStep):
    def __init__(self, table_name: str, measures: list):
        self.table_name = table_name
        self.measures = set(measures)
        self.levels = []

    def run_step(self, prev_result: pd.DataFrame, params: dict):
        df = prev_result

        columns = set(df.columns.values.tolist())
        self.levels = columns - self.measures

        result = {}
        for label, content in df.items():
            if label not in self.measures:
                continue

            series = content.aggregate([np.sum, np.nanmean])
            series["count"] = content.size
            result[label] = series.to_dict()

        report = {
            "aggs": result,
            "table": self.table_name,
            "measures": list(self.measures),
            "levels": list(self.levels),
        }

        filename = "test_{}.json".format(self.table_name)
        with open(filename, "w+") as f:
            json.dump(report, fp=f)

        return prev_result


class CubeTester:
    def __init__(self, cube: ET.Element, shared_dims: List[ET.Element]):
        table_name = cube.find("Table").get("name")

        self.cube_name = cube.get("name")
        self.levels = [lvl for lvl in get_levels(cube, shared_dims)]
        self.measures = [msr.attrib for msr in cube.findall("Measure")]
        self.table_name = table_name

        with open("test_%s.json" % table_name, "r") as f:
            root = json.load(f)
            self.agg_results = root["aggs"]
            self.agg_measures = root["measures"]
            self.agg_levels = root["levels"]

    def run_test(self, base: str):
        measure_names = [
            mea["name"]
            for mea in self.measures
            if mea["column"] in self.agg_measures
        ]

        url = urljoin(base, "/cubes/%s/aggregate.csv" % cube.get("name"))
        search = urlencode({
            "drilldowns[]": [
                join_fullname(lvl)
                for lvl in self.levels
                if lvl["key_column"] in self.agg_levels
            ],
            "measures[]": measure_names,
            "parents": "true",
        }, True)
        df = pd.read_csv(url + "?" + search)
        
        result = {}
        for label, content in df.items():
            if label not in measure_names:
                continue

            series = content.aggregate([np.sum, np.nanmean])
            series["count"] = content.size
            result[label] = series.to_dict()

        print(self.agg_results)
        print(result)


def join_fullname(lvl):
    tokens = (lvl["dimension"], lvl["hierarchy"], lvl["name"])
    if "." in "|".join(tokens):
        tokens = ("[{}]".format(token) for token in tokens)
    return ".".join(tokens)


def get_levels(cube: ET.Element, shared_dims: Dict[str, ET.Element]):
    for dim in cube.findall("DimensionUsage"):
        dim_name = dim.get("name")
        dimension = shared_dims[dim.get("source")]

        for hie in dimension.findall("Hierarchy"):
            hie_name = hie.get("name")

            for lvl in hie.findall("Level"):
                lvl_name = lvl.get("name")

                level = {
                    "dimension_fkey": dim.get("foreign_key"),
                    "dimension": dim_name,
                    "hierarchy": hie_name,
                }
                level.update(lvl.attrib)
                yield level

    for dim in cube.findall("Dimension"):
        dim_name = dim.get("name")

        for hie in dim.findall("Hierarchy"):
            hie_name = hie.get("name")

            for lvl in hie.findall("Level"):
                lvl_name = lvl.get("name")

                level = {
                    "dimension_fkey": dim.get("foreign_key"),
                    "dimension": dim_name,
                    "hierarchy": hie_name,
                }
                level.update(lvl.attrib)
                yield level


if __name__ == "__main__":
    schema_path = Path(sys.argv[1]).absolute()

    if not schema_path.exists() or not schema_path.is_file():
        print("The specified schema file does not exist: %s" % schema_path)
        sys.exit()

    tree = ET.parse(schema_path)
    root = tree.getroot()

    shared_dims = {sh.get("name"): sh for sh in root.findall("SharedDimension")}

    for cube in root.findall("Cube"):
        tester = CubeTester(cube, shared_dims)
        tester.run_test("http://localhost:7777")
