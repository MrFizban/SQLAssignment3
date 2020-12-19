import psycopg2
import random
import sys

TABLE_LENGTH = 10


def get_random_string(length):
    # Random string with the combination of lower and upper case
    letters = ['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','X','Y','Z','a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','x','y','z']
    return  ''.join(random.choice(letters) for i in range(length))
        
def assigmant_execute ():
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


        """3. Genera 1 milione di tuple (casuali 1), in modo tale che ogni tupla abbia un valore diverso per l’attributo level, e
        le inserisce nella tabella S  ailor. Assicurarsi inoltre che l’ultima tupla inserita, e solo quella, abbia come valore
        dell’attributo level, il valore 185."""
        id = random.sample(range(186, 3000000), TABLE_LENGTH)
        level = random.sample(range(186, 3000000), TABLE_LENGTH-1)
        level.append(185)
        tabella = []
        for i in range(0,TABLE_LENGTH,1):
            riga = {"id":id[i], "name": get_random_string(12), "address": get_random_string(42),"age": random.randint(0,60),"level": level[i]}
            tabella.append(riga)

        temp = f'''INSERT INTO "Sailor" ("id", "name", "address", "age", "level") VALUES (%(id)s, %(name)s,%(address)s, %(age)s, %(level)s )'''
        cursor.executemany(temp,tabella)
        connection.commit()

        """4. Genera 1 ulteriore milione di tuple (casuali) e le inserisce nella tabella B  oat ."""
        bid = random.sample(range(186, 3000000), TABLE_LENGTH)

        size = ["large", "medium", "small"]
        size_list = []
        for i in range(0,TABLE_LENGTH,1):
            size_list.append(random.randint(0,2))

        captain = []
        for i in range(0,TABLE_LENGTH,1):
            captain.append(id[random.randint(0,TABLE_LENGTH-1)])
        tabella = []
        for i in range(0,TABLE_LENGTH,1):
            riga = {"bid":id[i], "bname": get_random_string(12), "size": size_list[i],"captain": captain[i]}
            tabella.append(riga)

        """bid bname size captain"""
        temp = f'''INSERT INTO "Boat" ("bid", "bname", "size", "captain") VALUES (%(bid)s, %(bname)s,%(size)s, %(captain)s)'''
        cursor.executemany(temp, tabella)
        connection.commit()


        """5. Ottiene dal database tutti gli id  del milione di tuple della tabella Sailor e li stampa su stderr ."""
        
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


if __name__ == "__main__":
    assigmant_execute()