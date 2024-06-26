import logging
from pathlib import Path

from psycopg.sql import SQL, Identifier, Literal

from .utils import ADM0_JOIN, ADM_LEVELS, get_adm_id, get_src_ids, get_wld_ids

logger = logging.getLogger(__name__)

adm4_id = (
    [
        f"{get_adm_id(4)} = COALESCE({get_adm_id(4)}, {get_adm_id(3)}, {get_adm_id(2)}, {get_adm_id(1)})"
    ]
    if ADM_LEVELS >= 4
    else []
)
adm3_id = (
    [f"{get_adm_id(3)} = COALESCE({get_adm_id(3)}, {get_adm_id(2)}, {get_adm_id(1)})"]
    if ADM_LEVELS >= 3
    else []
)
adm2_id = (
    [f"{get_adm_id(2)} = COALESCE({get_adm_id(2)}, {get_adm_id(1)})"]
    if ADM_LEVELS >= 2
    else []
)

query_1 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    SELECT * FROM {table_in};
"""
query_2 = """
    ALTER TABLE {table_out}
    ADD COLUMN IF NOT EXISTS id_join VARCHAR DEFAULT {id_join};
"""
query_3 = """
    ALTER TABLE {table_out}
    ADD COLUMN IF NOT EXISTS {name} VARCHAR;
"""
query_4 = f"UPDATE {{table_out}} SET {','.join([*adm4_id, *adm3_id, *adm2_id])};"
query_5 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    SELECT
        {ids_src},
        {ids_wld},
        a.geom
    FROM {table_in1} AS a
    JOIN {table_in2} AS b
    ON {join} = a.id_join
    ORDER BY {adm_id};
    CREATE INDEX ON {table_out} USING GIST(geom);
"""
drop_tmp = """
    DROP TABLE IF EXISTS {table_out};
"""


def main(conn, file: Path):
    name = file.stem
    conn.execute(
        SQL(query_1).format(
            table_in=Identifier(f"admx_{name}"),
            table_out=Identifier(f"admx_{name}_tmp1"),
        )
    )
    conn.execute(
        SQL(query_2).format(
            id_join=Literal(name),
            table_out=Identifier(f"admx_{name}_tmp1"),
        )
    )
    for id in get_src_ids(conn, name):
        conn.execute(
            SQL(query_3).format(
                name=Identifier(id),
                table_out=Identifier(f"admx_{name}_tmp1"),
            )
        )
    conn.execute(
        SQL(query_4).format(
            table_out=Identifier(f"admx_{name}_tmp1"),
        )
    )
    conn.execute(
        SQL(query_5).format(
            table_in1=Identifier(f"admx_{name}_tmp1"),
            table_in2=Identifier("adm0_polygons"),
            adm_id=Identifier(get_adm_id(ADM_LEVELS)),
            join=Identifier("b", ADM0_JOIN),
            ids_src=SQL(",").join(
                map(lambda x: Identifier("a", x), get_src_ids(conn, name))
            ),
            ids_wld=SQL(",").join(map(lambda x: Identifier("b", x), get_wld_ids(conn))),
            table_out=Identifier(f"admx_{name}_1"),
        )
    )
    conn.execute(
        SQL(drop_tmp).format(
            table_out=Identifier(f"admx_{name}_tmp1"),
        )
    )
