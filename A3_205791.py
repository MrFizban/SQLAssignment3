import psycopg2
import random
import sys

TABLE_LENGTH = 10


with (psycopg2.connect(dbname='db_057', user='db_057', host='sci-didattica.unitn.it', password='pass_057')) as connection:
    cursor = connection.cursor()
    """1. Fa il drop delle due tabelle dalla base di dati ​ se sono già presenti​"""
    cursor.execute('DROP TABLE IF EXISTS "Boat";')
    connection.commit()
    cursor.execute('DROP TABLE IF EXISTS "Sailor";')
    connection.commit()

    """2. Crea le due tabelle come descritto sopra."""
    cursor.execute('CREATE TABLE "Sailor" ("id" INT PRIMARY KEY, "name" CHAR(50) NOT NULL, "address" CHAR(50) NOT NULL, "age" INT NOT NULL, "level" FLOAT NOT NULL);')
    connection.commit()
    cursor.execute('CREATE TABLE "Boat" ("bid" CHAR(25) PRIMARY KEY, "bname" CHAR(50) NOT NULL, "size" CHAR(30) NOT NULL, "captain" INT NOT NULL REFERENCES "Sailor"("id"));')
    connection.commit()


    """3. Genera 1 milione di tuple (casuali​ 1​ ), in modo tale che ogni tupla abbia un valore diverso per l’attributo level, e
      le inserisce nella tabella S ​ ailor.​ Assicurarsi inoltre che l’ultima tupla inserita, e solo quella, abbia come valore
      dell’attributo level, il valore 185."""
    level = random.sample(range(186, 3000000), TABLE_LENGTH-1)
    level.append(185)
    name = ["Liam", "Olivia", "Noah", "Emma", "Oliver", "Ava", "William", "Sophia", "Elijah", "Isabella", "James", "Charlotte", "Benjamin", "Amelia", "Lucas", "Mia", "Mason", "Harper", "Ethan", "Evelyn"]
    name_list = []
    for i in range(0,TABLE_LENGTH,1):
        name_list.append(random.randint(0,19))

    andress = "via Giangiorgio Oliviero,15"
    age = 42



    for i in range(0,TABLE_LENGTH,1): 
        temp = f'''INSERT INTO "Sailor" ("id", "name", "address", "age", "level") VALUES ({i}, '{name[name_list[i]]}', '{andress}', {age}, {level[i]})'''
        cursor.execute(temp)
        connection.commit()




    """4. Genera 1 ulteriore milione di tuple (casuali) e le inserisce nella tabella B ​ oat​ ."""
    bname = ["ABBEY", "ABBIE", "ABBY", "ABEL", "ABIGAIL", "ACE", "ADAM", "ADDIE", "ADMIRAL", "AGGIE", "AIRES", "AJ", "AJAX", "ALDO", "ALEX", "ALEXUS", "ALF", "ALFIE","ALLIE", "ALLY"]
    bname_list = []
    for i in range(0,TABLE_LENGTH,1):
        bname_list.append(random.randint(0,2))
    
    size = ["large", "medium", "small"]
    size_list = []
    for i in range(0,TABLE_LENGTH,1):
        size_list.append(random.randint(0,2))

    captain_list = []
    for i in range(0,TABLE_LENGTH,1):
        captain_list.append(random.randint(0,TABLE_LENGTH-1))



    for i in range(0,TABLE_LENGTH,1): 
        temp = f'''INSERT INTO "Boat" ("bid", "bname", "size", "captain") VALUES ({i}, '{bname[bname_list[i]]}', '{size[size_list[i]]}', {captain_list[i]})'''
        cursor.execute(temp)
        connection.commit()

    """5. Ottiene dal database tutti gli id ​ del milione di tuple della tabella Sailor e li stampa su stderr​ ."""
    
    cursor.execute(""" SELECT id FROM "Sailor" """)
    lista = cursor.fetchall()
    print(lista, file=sys.stderr)


    """6. Tutte le tuple con valore di level pari a 185 vengono modificate, cambiando il valore di level a 200 (la vostra
          query dovrà funzionare anche se la base di dati contiene più di una tupla con valore di level pari a 185). """

    cursor.execute('''UPDATE "Sailor" SET "level" = 200 WHERE "level" = 185''')
    connection.commit()

    """7. Seleziona l’id e l’address di tutte le tuple della tabella Sailor che hanno valore di level pari a 200, e li stampa su stderr."""
    cursor.execute('''SELECT id, address FROM "Sailor" as sl WHERE "level" = 200''')
    lista = cursor.fetchall()
    print(lista, file=sys.stderr)


    """8. Crea un indice B+tree sull’attributo level."""

    cursor.execute('CREATE INDEX index_level ON "Sailor" ("level");')
    connection.commit()
    """9. Ottiene dal database tutti gli id ​ del milione di tuple della tabella Sailor​ e li stampa su stderr​ ."""
    cursor.execute("""
            SELECT id
            FROM "Sailor"
    """)
    lista = cursor.fetchall()
    print(lista)
    """ per stampare in stderr: print("ciaone", file=sys.stderr)"""


    """10. Tutte le tuple con valore di level pari a 200 vengono modificate, cambiando il valore di level a 210 (la vostra
       query dovrà funzionare anche se la base di dati contiene più di una tupla con valore di level pari a 200)."""

    cursor.execute('''UPDATE "Sailor" SET "level" = 210 WHERE "level" = 200''')
    connection.commit()


    """11. Seleziona l’id e l’address di tutte le tuple della tabella Sailor che hanno valore di level pari a 210, e li stampa su stderr."""

    cursor.execute('''SELECT id, address FROM "Sailor" as sl WHERE "level" = 210''')
    lista = cursor.fetchall()
    print(lista, file=sys.stderr)