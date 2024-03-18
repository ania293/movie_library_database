import sqlite3

class SQLite:
    
    def __init__(self, database_name):
        self.database_name = database_name
        self.conn = None
    
    def __enter__(self):
        self.conn = sqlite3.connect(self.database_name)
        print("Connected to ", self.database_name)
        return self.conn

    def __exit__(self, *args):
        self.conn.close()
        print("Connection closed")


# Functions    
def insert_movie(conn, movie, actor_name):
    '''
    Create a new movie:
    :param conn: the Connection object
    :param movie: tuple with arguments for movie table
    :return: movie id
    '''
    res = select_where(conn, 'main_actors', "name == " + actor_name)
    sql = ("""INSERT INTO movies(actor_id, title, release, genre)
             VALUES(?,?,?,?)""")
    movie.insert(0, res[0][0])
    cur = conn.cursor()
    cur.execute(sql, movie)
    conn.commit()
    return cur.lastrowid

def insert_actor(conn, actor):
    '''
    Create a new actor/actress:
    :param conn: the Connection object
    :param actor: tuple with arguments for actor table
    :return: actor id
    '''
    sql = ("""INSERT INTO main_actors(name, gender)
            VALUES(?,?)""")
    cur = conn.cursor()
    cur.execute(sql, actor)
    conn.commit()
    return cur.lastrowid


def select_all(conn, table):
    '''
    Query all values and columns from selected table:
    :param conn: the Connection object
    :param table: name of the table
    :return: all rows of the table
    '''
    cur = conn.cursor()
    cur.execute("SELECT * FROM {}".format(table))
    return cur.fetchall()

def select_where(conn, table, *queries):
    '''
    Query information from selected table with the use of where query:
    :param conn: the Connection object
    :param table: name of the table
    :param *queries: all where queries
    :return: all rows of the table
    '''
    queries_list = [str(k) for k in queries]
    where_query = " AND ".join(queries_list)  

    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table} WHERE {where_query}")    
    return cur.fetchall()

def update_table(conn, table, id, **kwargs):
    '''
    Update information from selected table with new values:
    :param conn: the Connection object
    :param table: name of the table
    :param **kwargs: column names with new values
    '''
    parameters = [f"{k} = ?" for k in kwargs]
    parameters = ", ".join(parameters)
    values = tuple(v for v in kwargs.values())
    values += (id, )

    sql = f''' UPDATE {table}
             SET {parameters}
             WHERE id = ?'''

    cur = conn.cursor()
    cur.execute(sql, values)    
    conn.commit()

def delete_where(conn, table, *queries):
    '''
    Delete information from selected table with the use of where query:
    :param conn: the Connection object
    :param table: name of the table
    :param *queries: all where queries
    '''

    queries_list = [str(k) for k in queries]
    where_query = " AND ".join(queries_list) 
    cur = conn.cursor()
    cur.execute(f"DELETE FROM {table} WHERE {where_query}")    
    conn.commit()
    print("Deleted")

def delete_all(conn, table):
    '''
    Delete all information from selected table
    :param conn: the Connection object
    :param table: name of the table
    '''

    cur = conn.cursor()
    cur.execute(f"DELETE FROM {table}")
    conn.commit()
    print("Deleted")



if __name__ == '__main__':


    database = "movies_library.db"
    sql_tab1 = ("""
        CREATE TABLE IF NOT EXISTS main_actors(
        id           INTEGER PRIMARY KEY NOT NULL,
        name         TEXT NOT NULL,
        gender       TEXT NOT NULL        
        );""")

    sql_tab2 = ("""CREATE TABLE IF NOT EXISTS movies(
        id           INTEGER PRIMARY KEY NOT NULL,
        actor_id     INTEGER NOT NULL,
        title        TEXT NOT NULL,
        release      INTEGER,
        genre        TEXT,
        FOREIGN KEY (actor_id) REFERENCES main_actors (id)
        );""")
    
    actors = [["Brad Pitt", "Male"],
              ["Timothée Chalamet", "Male"],
              ["Morgan Freeman", "Male"],
              ["Saoirse Ronan", "Female"],
              ["Emma Stone", "Female"]]
    movies = [["Bullet train", 2022, "Action"],
              ["Dune 1", 2021, "Sci-fi/Adventure"],
              ["The Shawshank Redemption", 1994, "Thriller/Crime"],
              ["Little Woman", 2021, "Drama"],
              ["La La Land", 2016, "Musical"],
              ["Poor Things", 2023, "Comedy"],
              ["Troy", 2004, "Action"]]
    
    movie_actor = ["'Brad Pitt'", "'Timothée Chalamet'", "'Morgan Freeman'", "'Saoirse Ronan'",
                   "'Emma Stone'", "'Emma Stone'", "'Brad Pitt'"]
        
    ## Init database
    with SQLite(database) as conn:
        cur = conn.cursor()
        cur.execute(sql_tab1)
        cur.execute(sql_tab2)

    ## Insert values
    add_values = False
    if add_values:
        with SQLite(database) as conn:
            for actor in actors:
                insert_actor(conn, actor)
            for ind, movie in enumerate(movies):
                insert_movie(conn, movie, movie_actor[ind])
    
    ## Select/Update queries
    with SQLite(database) as conn:
        res = select_where(conn, 'movies', 'id == 1', 'genre = "Action"')
        print(res)
        update_table(conn, 'movies', 1, genre='Action/Adventure', title='Bullet trains')
        res = select_where(conn, 'movies', 'id == 1')
        print(res)
        delete_where(conn, 'movies', 'id == 3')
        #delete_all(conn, 'main_actors')
        #delete_all(conn, 'movies')

    
