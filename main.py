import sqlite3
THESAURUS_PATH = "rutez.db"


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except sqlite3.Error as e:
        print(e)

    return conn


def all_hyperonyms_by_id(conn, id):
    """
    returns list of ALL hyperonyms for -id-
    :param conn: Connection object returned by create_connection
    :param id: str
    :return: list of (integer, str)
    """
    cur = conn.cursor()
    cur.execute(
        "WITH RECURSIVE hyperonyms(id) AS "
        "(VALUES(?) UNION SELECT rel.link FROM rel, hyperonyms WHERE rel.name='ВЫШЕ' and rel.id=hyperonyms.id) "
        "SELECT sinset.id, sinset.name FROM sinset, hyperonyms WHERE hyperonyms.id = sinset.id", (id,)
    )
    rows = cur.fetchall()
    res = []
    for row in rows:
        res.append((row[0], row[1],))

    return res


def all_hyperonyms_by_name(conn, lemma):
    """
    returns list of ALL RECURSIVE hyperonyms for -lemma-
    :param conn: Connection object returned by create_connection
    :param lemma: str
    :return: list of (integer, str)
    """
    cur = conn.cursor()
    cur.execute(
        "WITH RECURSIVE hyperonyms(id) AS "
        "(SELECT sinset.id FROM word, sinset WHERE word.name=? and sinset.id=word.id "
        "UNION SELECT rel.link FROM rel, hyperonyms WHERE rel.name='ВЫШЕ' and rel.id=hyperonyms.id) "
        "SELECT sinset.id, sinset.name FROM sinset, hyperonyms WHERE hyperonyms.id = sinset.id", (lemma.upper(),)
    )
    rows = cur.fetchall()
    res = []
    for row in rows:
        res.append((row[0], row[1],))

    return res


with create_connection(THESAURUS_PATH) as conn:
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM sqlite_sequence"
    )
    res = cur.fetchall()
    print(res)


def main():
    with create_connection(THESAURUS_PATH) as conn:
        cur = conn.cursor()
        id = 45
        cur.execute(
            "SELECT * FROM word WHERE id = (?)", (id,)
        )
        name = cur.fetchall()
        # print('name')
        # print(name)
        # print('name')
        cur.execute(
            "WITH RECURSIVE hyperonyms(id) AS "
            "(VALUES(?) UNION SELECT rel.link FROM rel, hyperonyms WHERE rel.name='ВЫШЕ' and rel.id=hyperonyms.id) "
            "SELECT sinset.id, sinset.name FROM sinset, hyperonyms WHERE hyperonyms.id = sinset.id", (id,)
        )
        res = cur.fetchall()
        # print
        for k in range(1, 26355):
            id = k
            cur.execute(
                "SELECT * FROM word WHERE id = (?)", (id,)
            )
            name = cur.fetchall()
            cur.execute(
                "WITH RECURSIVE hyperonyms(id) AS "
                "(VALUES(?) UNION SELECT rel.link FROM rel, hyperonyms WHERE rel.name='ВЫШЕ' and rel.id=hyperonyms.id) "
                "SELECT sinset.id, sinset.name FROM sinset, hyperonyms WHERE hyperonyms.id = sinset.id", (id,)
            )
            res = cur.fetchall()
            print(k)
            for i in name:
                for j in res:
                    values = (id, i[1], j[0], j[1])
                    cur.execute("INSERT INTO hyperonyms VALUES(?, ?, ?, ?);", values)
                    conn.commit()


main()
