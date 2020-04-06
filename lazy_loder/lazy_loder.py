# -*- coding: utf-8 -*-
import logging
import re
import os
import time
from functools import lru_cache, reduce
from glob import glob
import sqlite3
import subprocess
from threading import Thread, Semaphore, current_thread
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from graphviz import Digraph


class LazyLoder(object):

    def __init__(self, kitchen, metadata):
        for name, meta in metadata.items():
            if "dtypes" not in meta:
                raise KeyError(f"{metadata['{name}']} must contain 'dtypes'.")

            if "file_info" in meta:
                file_info = meta["file_info"]
                if "path" not in file_info:
                    raise KeyError(f"metadata['{name}']['file_info'] must contain 'path'.")
                elif "format" not in file_info:
                    raise KeyError(f"metadata['{name}']['file_info'] must contain 'format'.")
                elif "header" not in file_info:
                    raise KeyError(f"metadata['{name}']['file_info'] must contain 'header'.")
                elif "encoding" not in file_info:
                    raise KeyError(f"metadata['{name}']['file_info'] must contain 'encoding'.")
                elif meta.get("dependencies", []) != []:
                    raise ValueError(f"metadata['{name}']['dependencies'] must be None or [] or not included.")
                elif meta.get("recipe") is not None:
                    raise ValueError(f"metadata['{name}']['recipe'] must be None or not included.")
                elif not glob(file_info["path"]) != []:
                    raise FileNotFoundError(f"metadata['{name}']['path'] not exists.")
                elif file_info["format"] not in ("csv", "tsv"):
                    raise ValueError(f"metadata['{name}']['file_info']['format'] must be 'csv' or 'tsv'.")
                elif type(file_info["header"]) is not bool:
                    raise TypeError(f"metadata['{name}']['file_info']['header'] must bool.")
            else:
                if "dependencies" not in meta:
                    raise KeyError(f"metadata['{name}'] must contain 'dependencies'.")
                elif "recipe" not in meta:
                    raise KeyError(f"metadata['{name}'] must contain 'recipe'.")
                elif "if_exists" not in meta:
                    raise KeyError(f"metadata['{name}'] must contain 'if_exists'.")
                elif set(list(metadata.keys()) + meta["dependencies"]) != set(metadata.keys()):
                    raise ValueError(f"metadata['{name}']['dependencies'] has unknown table name.")
                elif meta["recipe"].__code__.co_argcount != 4:
                    raise ValueError(f"""\
                        metadata['name']['dependencies'] must have only 3 artuments \
                        'loder', 'committer', 'cursor' and 'updated'.\
                    """.replace("\t", "").replace("  ", ""))
                elif meta["if_exists"] not in ("replace", "append"):
                    raise ValueError(f"metadata['{name}']['if_exists'] must be 'replace' or 'append'.")

        self.__kitchen = kitchen
        self.__metadata = metadata
        self.__running_threads = {}
        self.__logger = logging.getLogger()

        conn = sqlite3.connect(kitchen, detect_types=sqlite3.PARSE_DECLTYPES, isolation_level=None)
        c = conn.cursor()
        try:
            if not self.__is_in_kitchen("__memo", conn):
                c.execute("begin;")
                c.execute("create table if not exists __memo (name text primary key, mtime float, inode text);")
            else:
                # NOTE: metadataにないテーブルがあったとしても、壊れている訳ではないので消さずに残しておく
                c.execute("select name from __memo;")
                memo_names = set(c.fetchall())
                c.execute("select name from sqlite_master where type='table' and name != '__memo';")
                master_names = set(c.fetchall())
                for name in memo_names - master_names:
                    c.execute("delete from __memo where name = ?;", name)
                for name in master_names - memo_names:
                    c.execute(f"drop table {name[0]};")  # SQLインジェクション対策
        except Exception as e:
            conn.rollback()
            raise e
        else:
            conn.commit()
        conn.close()

    @property
    def metadata(self):
        return self.__metadata

    @property
    def kitchen(self):
        return self.__kitchen

    @property
    @lru_cache(maxsize=1)
    def tables(self):
        return list(self.metadata.keys())

    @property
    @lru_cache(maxsize=1)
    def leaf_tables(self):
        return list(filter(lambda name: "file_info" in self.metadata[name], self.tables))

    @property
    @lru_cache(maxsize=1)
    def schema(self):
        return dict(map(lambda kv: (kv[0], kv[1]["dtypes"]), self.metadata.items()))

    @property
    @lru_cache(maxsize=1)
    def graph(self):
        dg = Digraph()
        for name, meta in self.metadata.items():
            dg.node(name, shape="box")
            for child in meta.get("dependencies", []):
                dg.edge(name, child)
        return dg

    def render_graph(self, path, format="png"):
        # CONSIDER: 特定のノードに関連する依存関係だけを出力できるようにする。
        dg = self.graph
        dg.format = format
        dg.render(path)

    def load(self, name):
        thread = Thread(target=self.__update, args=(name, ), name=f"Thread-{name}")
        thread.start()
        thread.join()
        ret = self.__from_kitchen(name)
        return ret

    def update(self, name):
        thread = Thread(target=self.__update, args=(name, ), name=f"Thread-{name}")
        thread.start()
        thread.join()

    def output(self, name, path, sep=",", header=True):
        self.__update(name)
        header = "on" if header else "off"
        # NOTE: パイプで渡さないと改行コードがsqlite3側で認識されない
        cmd_1 = ["echo", f".header {header}\n.separator '{sep}'\n.output {path}\nselect * from {name}"]
        cmd_2 = f"sqlite3 {self.kitchen}"
        with subprocess.Popen(cmd_1, stdout=subprocess.PIPE) as proc_1:
            self.__logger.info(f"データベースの{name}テーブルを書き出します.")
            proc_2 = subprocess.Popen(cmd_2.split(), stdin=proc_1.stdout)
            proc_2.wait()

    def multi_load(self, names):
        for name in names:
            self.__running_threads[name] = Thread(
                # HACK: 高速化のために__updateと_from_kitchenを続けて処理. self.__update(name)はNoneを返す.
                target=lambda name: self.__update(name) or self.__from_kitchen(name),  
                args=(name, ),
                name=f"Thread-{name}"
            )
            self.__running_threads[name].start()
        for name in names:
            self.__running_threads[name].join()
        return {name: self.__from_kitchen(name) for name in names}

    def __update(self, name):
        self.__logger.info(f"{name}テーブルを更新します.")

        conn = sqlite3.connect(
            self.kitchen,
            check_same_thread=False,
            detect_types=sqlite3.PARSE_DECLTYPES,
            isolation_level=None
        )

        if self.__is_available(name, conn):
            self.__logger.info(f"{name}テーブルは現在最新の状態です.")
            conn.close()
            return

        is_leaf = name in self.leaf_tables
        semaphore = Semaphore()

        while True:
            semaphore.acquire()

            if name in self.__running_threads and self.__running_threads[name].is_alive():
                semaphore.release()
                self.__logger.info(f"{name}テーブルが他スレッドにより更新されるまで待機します.")
                self.__running_threads[name].join()
                conn.close()
                return

            loading_leafs = dict(filter(
                lambda kv:  kv[0] in self.leaf_tables and kv[1].is_alive(),
                self.__running_threads.items(),
            ))

            if is_leaf and loading_leafs != {}:
                semaphore.release()
                for nm, thread in loading_leafs.items():
                    if nm != name:
                        thread.join()
            else:
                self.__running_threads[name] = current_thread()
                semaphore.release()
                break

        if is_leaf:
            self.__update_leaf(name, conn)
        else:
            self.__update_node(name, conn)

        conn.close()

    def __from_kitchen(self, name):
        conn = sqlite3.connect(self.kitchen)
        ret = pd.read_sql_query(f"select * from {name};", conn)
        conn.close()
        return ret

    def __update_leaf(self, name, conn):
        c = conn.cursor()
        c.execute("begin;")

        try:
            if self.__is_in_kitchen(name, conn):
                c.execute(f"delete from {name};")
            else:
                dtypes = ",".join(map(lambda kv: f"'{kv[0]}' {kv[1]}", self.metadata[name]["dtypes"]))
                c.execute(f"create table {name} ({dtypes});")  # TODO: SQLインジェクション対策
        except Exception as e:
            conn.rollback()
            raise e
        else:
            conn.commit()

        try:
            self.__to_kitchen(name, conn)
            self.__memo(name, conn)
        except Exception as e:
            conn.rollback()
            c.execute(f"drop table if exists {name};")  # TODO: SQLインジェクション対策
            conn.commit()
            raise e
        else:
            conn.commit()

    def __update_node(self, name, conn):
        meta = self.metadata[name]
        dependencies = meta.get("dependencies", []).copy()
        depend_on_itself = name in dependencies
        if depend_on_itself:
            del dependencies[dependencies.index(name)]

        c = conn.cursor()
        c.execute("begin;")

        threads = [
            Thread(target=self.__update, args=(need, ), name=f"Thread-{need}")
            for need in filter(lambda x: x not in self.__running_threads, dependencies)
        ]

        self.__logger.info(f"{name}テーブルが依存するテーブル群を最新の状態にします.")

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        self.__logger.info(f"{name}テーブルの更新を再開します.")

        try:
            if self.__is_in_kitchen(name, conn) and meta["if_exists"] == "replace":
                c.execute(f"delete from {name};")
            dtypes = ",".join(map(lambda kv: f"'{kv[0]}' {kv[1]}", meta["dtypes"]))
            c.execute(f"create table if not exists {name} ({dtypes});")  # TODO: SQLインジェクション対策
            self.__cook(name, conn)
            self.__memo(name, conn)
        except Exception as e:
            conn.rollback()
            raise e
        else:
            conn.commit()

    def __to_kitchen(self, name, conn):
        file_info = self.metadata[name]["file_info"]
        from_paths = glob(file_info["path"])
        sep = "," if file_info["format"] == "csv" else "\t"
        header = file_info["header"]
        encoding = file_info["encoding"]
        c = conn.cursor()

        # TODO: sqlite3でcp932が使えないので、そのままimportすると文字化けしたり'"'が混じって
        #       一部レコードが抜けたりする. この問題を解決するために、iconvやsedを使って
        #       取り込み前にエンコードを変換してとりあえず動かせるようにした. 
        #       代替案をさがすこと.
        # NOTE: create_???での中間データ作成関数で文字化けしたデータが現れるともっと厄介なので
        #       取り込み時に既にエンコーディングの差分は吸収しておくのは絶対.
        if encoding not in  ["UTF-8", "utf-8", "UTF8", "utf8"]:
            to_dir = os.path.join(os.path.dirname(self.kitchen), f".lazy_loder_ws")
            # NOTE: listに変換していないとなぜかThreadPoolExecutor()の処理が終わった時点で[]になる
            to_paths = list(map(lambda x: os.path.join(to_dir, os.path.basename(x)), from_paths))  
            os.makedirs(to_dir, exist_ok=True)

            def transform(from_path, to_path):
                cmd_1 = f"iconv -f {encoding} -t utf8 {from_path}"
                cmd_2 = "sed s/\"//g"
                with subprocess.Popen(cmd_1.split(), stdout=subprocess.PIPE) as proc_1:
                    with open(to_path, "w") as f:
                        proc_2 = subprocess.Popen(cmd_2.split(), stdin=proc_1.stdout, stdout=f)
                        proc_2.wait()

            with ThreadPoolExecutor() as executor:
                for from_path, to_path in zip(from_paths, to_paths):
                    executor.submit(transform, from_path, to_path)

            from_paths = to_paths

        # NOTE: .importコマンドは並列化できなかった
        header_row_ids = []
        for from_path in from_paths:
            cmd = ["sqlite3", "-separator", sep, self.kitchen, f".import {from_path} {name}"]
            self.__logger.info(f"データベースに{name}テーブルを取り込みます.")
            with subprocess.Popen(cmd, stderr=subprocess.PIPE) as proc:
                proc.wait()
                # NOTE: エラーが起きたとしてもproc.returncodeは0になる
                stderr = proc.stderr.read().decode("utf-8")
                if stderr != "":
                    msg = f"""
                        sqlite3のimport処理でエラーが発生しました. 
                         一部レコードが取り込まれていない可能性があります. 
                        sqlite3のエラー内容: '{stderr}'
                    """
                    self.__logger.warning(re.sub("\n|\t|  ", "", msg))
            c.execute(f"select count(*) + 1 from {name};")
            header_row_ids.append(c.fetchone()[0])
        header_row_ids = ([1] + header_row_ids)[:-1]

        # TODO: そもそもheaderの内容がレコードに含まれないようなインポート方法があればそれに置き換える
        if header:
            for row_id in header_row_ids:
                c.execute(f"delete from {name} where ROWID = {row_id};")

    def __cook(self, name, conn):
        dependencies = self.metadata[name]["dependencies"]
        c = conn.cursor()
        c.execute("select name from __memo order by mtime desc;")
        updated = []
        for row in c.fetchall():
            newone = row[0]
            if newone == name:
                break
            elif newone in dependencies:
                updated.append(newone)

        # NOTE: tb1_loder, tb2_loderのようにテーブルを個別にロードするコールバック関数を作ろうとするとなぜか
        #       全ての全ての???_loderが最後に作成したloderと同じ挙動をするようになる.
        # NOTE: if_existsが常に"append"になるのは、metadataで"replace"が指定されていたとしても、
        #       所望のdtypeが指定された空のテーブルが既に作成されているから.
        params = {
            "cursor": c,
            "committer": (lambda df: df.to_sql(name, conn, index=None, if_exists="append")),
            "loader": (lambda : self.multi_load(dependencies)),
            "updated": updated
        }
        self.metadata[name]["recipe"](**params)

    def __memo(self, name, conn):
        if name in self.leaf_tables:
            path = self.metadata[name]["file_info"]["path"]
            mtime = LazyLoder.__get_file_mtime(path)
            inode = LazyLoder.__get_file_inode(path)
        else:
            mtime = time.time()
            inode = "NULL"

        c = conn.cursor()
        c.execute("select * from __memo where name = ?;", (name, ))
        if c.fetchone() is None:
            c.execute("insert into __memo values (?, ?, ?);", (name, mtime, inode))
        else:
            c.execute("update __memo set mtime = ?, inode = ? where name = ?;", (mtime, inode, name))

    def __is_available(self, name, conn):
        return self.__is_in_kitchen(name, conn) and self.__is_fresh(name, conn)

    def __is_in_kitchen(self, name, conn):
        c = conn.cursor()
        c.execute(u"select name from sqlite_master where type='table';")
        return (name, ) in c.fetchall()

    def __is_fresh(self, name, conn):
        meta = self.metadata[name]
        c = conn.cursor()

        dependencies = meta.get("dependencies", []).copy()
        if name in dependencies:
            del dependencies[dependencies.index(name)]

        if dependencies == []:
            path = meta["file_info"]["path"]
            c.execute(u"select mtime, inode from __memo where name = ?;", (name, ))
            mtime_kitchen, inode_kitchen = c.fetchone()
            mtime_file = LazyLoder.__get_file_mtime(path)
            inode_file = LazyLoder.__get_file_inode(path)
            return mtime_kitchen >= mtime_file and inode_kitchen == inode_file

        tmp_param = ",".join(map(lambda x: f"'{x}'", [name] + dependencies))
        # TODO: SQLインジェクション対策
        c.execute(f"select name from __memo where name in ({tmp_param}) order by mtime desc;")
        latest = c.fetchone()[0]

        if latest == name:
            return all(map(lambda x: self.__is_fresh(x, conn), dependencies))

        return False

    @staticmethod
    def __get_file_mtime(path):
        return sorted(map(lambda path: os.stat(path).st_mtime, glob(path)))[-1]

    @staticmethod
    def __get_file_inode(path):
        return reduce(
            lambda acc, x: acc + x,
            sorted(map(lambda path: str(os.stat(path).st_ino), glob(path))),
            ""
        )
