# What is lazy_loder?

lazy_loder is a python library for building a pipeline for preprocessing table on a single machine.

## So what's the features?

The features of lazy_loder are as follows.

- Do not run unnecessary processing
- Create tables as parallel as possible
- Perform data processing with sqlite3 database

Thanks to the third feature, memory consumption is minimized.  
However, data processing is cumbersome due to the third feature.  
But don't worry. You can also use Pandas DataFrame when it comes to your needs.


# Anyway, sample code

```python
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


kitchen = "data/cache/kitchen.db"
metadata = {
    "leaf_0": {
        "file_info": {
            "path": "data/leaf_0_*",
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
            "path": "data/leaf_1.csv",
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
            "path": here, "data/leaf_2.csv",
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
ll.update("node_1")
ll.output("node_1", "node_1.csv")

```
