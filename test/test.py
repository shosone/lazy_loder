# -*- coding: utf-8 -*-
import unittest
import sys
import os

from glob import glob
import pandas as pd


here = os.path.dirname(__file__)
sys.path.append(os.path.join(here, ".."))


from lazy_loder import LazyLoder


def create_node_0(loader, committer, cursor, updated):
    dfs = loader()
    df = pd.concat([dfs["leaf_0"], dfs["leaf_1"]], axis=0)
    committer(df)


def create_node_1(loader, committer, cursor, updated):
    dfs = loader()
    df = pd.concat([dfs["leaf_1"], dfs["node_0"]], axis=0)
    committer(df)


def create_node_2(loader, committer, cursor, updated):
    cursor.execute("""
    insert into node_2 
    select 
        leaf_2.name,
        leaf_2.age,
        node_0.id
    from 
        leaf_2 
        left join 
        node_0 
    on leaf_2.name = node_0.name;
    """)


kitchen = os.path.join(here, "data/cache/kitchen.db")
metadata = {
    "leaf_0": {
        "file_info": {
            "path": os.path.join(here, "data/leaf_0_*"),
            "format": "csv",
            "header": True,
            "encoding": "UTF-8"
        },
        "dtypes": [
            ("id", "INTEGER"),
            ("name", "TEXT")
        ]
    },

    "leaf_1": {
        "file_info": {
            "path": os.path.join(here, "data/leaf_1.csv"),
            "format": "csv",
            "header": False,
            "encoding": "UTF-8"
        },
        "recipe": None,
        "dtypes": [
            ("id", "INTEGER"),
            ("name", "TEXT")
        ]
    },

    "leaf_2": {
        "file_info": {
            "path": os.path.join(here, "data/leaf_2.csv"),
            "format": "csv",
            "header": False,
            "encoding": "UTF-8"
        },
        "recipe": None,
        "dtypes": [
            ("name", "TEXT"),
            ("age", "INTEGER")
        ]
    },

    "node_0": {
        "dependencies": [
            "leaf_0",
            "leaf_1"
        ],
        "recipe": create_node_0,
        "dtypes": [
            ("id", "INTEGER"),
            ("name", "TEXT")
        ],
        "if_exists": "replace"
    },

    "node_1": {
        "dependencies": [
            "leaf_1",
            "node_0"
        ],
        "recipe": create_node_1,
        "dtypes": [
            ("id", "INTEGER"),
            ("name", "TEXT")
        ],
        "if_exists": "replace"
    },

    "node_2": {
        "dependencies": [
            "node_0",
            "leaf_2",
            "node_2"
        ],
        "recipe": create_node_2,
        "dtypes": [
            ("name", "TEXT"),
            ("age", "INTEGER"),
            ("id", "INTEGER")
        ],
        "if_exists": "append"
    }
}
ll = LazyLoder(kitchen, metadata)


class Tester(unittest.TestCase):

    def test_leaf0(self):
        print(ll.load("leaf_0"))

    def test_leaf1(self):
        print(ll.load("leaf_1"))

    def test_leaf2(self):
        print(ll.load("leaf_2"))

    def test_leaf_multi(self):
        print(ll.multi_load(["leaf_0", "leaf_1", "leaf_2"]))

    def test_node_0(self):
        print(ll.load("node_0"))

    def test_node_1(self):
        print(ll.load("node_1"))

    def test_node_2(self):
        print(ll.load("node_2"))

    def test_node_multi(self):
        print(ll.multi_load(["node_0", "node_1", "node_2"]))

    def test_tables(self):
        print(ll.tables)

    def test_schema(self):
        print(ll.schema)

    def test_render_graph(self):
        ll.render_graph("data/graph")

    def test_update(self):
        ll.update("node_0")

    def test_output(self):
        ll.output("node_1", os.path.join(here, "data/node_1.csv"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
