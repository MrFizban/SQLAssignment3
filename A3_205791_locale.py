import psycopg2
import random
import sys
from time import time_ns
TABLE_LENGTH = 1000000


def get_random_string(length):
    # Random string with the combination of lower and upper case
    letters = ['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','X','Y','Z','a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','x','y','z']
    return  ''.join(random.choice(letters) for i in range(length))

def product(*args, repeat=1):
    # product('ABCD', 'xy') --> Ax Ay Bx By Cx Cy Dx Dy
    # product(range(2), repeat=3) --> 000 001 010 011 100 101 110 111
    pools = [tuple(pool) for pool in args] * repeat
    result = [[]]
    for pool in pools:
        result = [x+[y] for x in result for y in pool]
    for prod in result:
        yield tuple(prod)
        

def get_random_bid(length=6):
    letters = ['A','B','C','D','E','F','G','H','I','J','K']
    lista = [''.join(x) for x in product(letters, repeat=6)]
    return random.sample(lista,TABLE_LENGTH)
    


def assigmant_execute ():
    with (psycopg2.connect(dbname='test', user='postgres',host='localhost' ,password='cavallo_bello')) as connection:
        cursor = connection.cursor()
        """1. Fa il drop delle due tabelle dalla base di dati ​ se sono già presenti​"""
        start_time = time_ns() #inizioe conteggio tempo
        cursor.execute('DROP TABLE IF EXISTS "Boat";')
        connection.commit()
        cursor.execute('DROP TABLE IF EXISTS "Sailor";')
        connection.commit()
        end_time = time_ns() # calcola fine
        print(f"Step 1 needs {end_time - start_time} ns") # calcola fine

        """2. Crea le due tabelle come descritto sopra."""
        start_time = time_ns() #inizioe conteggio tempo
        cursor.execute('CREATE TABLE "Sailor" ("id" INT PRIMARY KEY, "name" CHAR(50) NOT NULL, "address" CHAR(50) NOT NULL, "age" INT NOT NULL, "level" FLOAT NOT NULL);')
        connection.commit()
        cursor.execute('CREATE TABLE "Boat" ("bid" CHAR(25) PRIMARY KEY, "bname" CHAR(50) NOT NULL, "size" CHAR(30) NOT NULL, "captain" INT NOT NULL REFERENCES "Sailor"("id"));')
        connection.commit()
        end_time = time_ns() # calcola fine
        print(f"Step 2 needs {end_time - start_time} ns") # calcola fine

        """3. Genera 1 milione di tuple (casuali 1), in modo tale che ogni tupla abbia un valore diverso per l’attributo level, e
        le inserisce nella tabella S  ailor. Assicurarsi inoltre che l’ultima tupla inserita, e solo quella, abbia come valore
        dell’attributo level, il valore 185."""
        start_time = time_ns() #inizioe conteggio tempo
        id = range(0, TABLE_LENGTH)
        level = random.sample(range(18600, 3000000), TABLE_LENGTH-1)
        for i in range(0,len(level),1):
            level[i] = level[i]/100  
        level.append(185.00)
        tabella = []
        for i in range(0,TABLE_LENGTH,1):
            riga = {"id":id[i], "name": get_random_string(12), "address": get_random_string(42),"age": random.randint(0,60),"level": level[i]}
            tabella.append(riga)
        

        temp = f'''INSERT INTO "Sailor" ("id", "name", "address", "age", "level") VALUES (%(id)s, %(name)s,%(address)s, %(age)s, %(level)s )'''
        cursor.executemany(temp,tabella)
        connection.commit()
        end_time = time_ns() # calcola fine
        print(f"Step 3 needs {end_time - start_time} ns") # calcola fine
        
        """4. Genera 1 ulteriore milione di tuple (casuali) e le inserisce nella tabella B  oat ."""
        start_time = time_ns() #inizioe conteggio tempo
        bid = get_random_bid()
        size = ["large", "medium", "small"]
        size_list = []
        for i in range(0,TABLE_LENGTH,1):
            size_list.append(size[random.randint(0,2)])

        captain = []
        for i in range(0,TABLE_LENGTH,1):
            captain.append(id[random.randint(0,TABLE_LENGTH-1)])
        tabella = []
        for i in range(0,TABLE_LENGTH,1):
            riga = {"bid":bid[i], "bname": get_random_string(12), "size": size_list[i],"captain": captain[i]}
            tabella.append(riga)
        end_time = time_ns() # calcola fine
        print(f"Step 4 needs {end_time - start_time} ns") # calcola fine


        """bid bname size captain"""
        temp = f'''INSERT INTO "Boat" ("bid", "bname", "size", "captain") VALUES (%(bid)s, %(bname)s,%(size)s, %(captain)s)'''
        cursor.executemany(temp, tabella)
        connection.commit()
        end_time = time_ns() # calcola fine
        print(f"Step 5 needs {end_time - start_time} ns") # calcola fine


        """5. Ottiene dal database tutti gli id  del milione di tuple della tabella Sailor e li stampa su stderr ."""
        start_time = time_ns() #inizioe conteggio tempo
        cursor.execute(""" SELECT id FROM "Sailor" """)
        lista = cursor.fetchall()
        for i in lista:
            print(i[0],file=sys.stderr)
        end_time = time_ns() # calcola fine
        print(f"Step 6 needs {end_time - start_time} ns") # calcola fine


        """6. Tutte le tuple con valore di level pari a 185 vengono modificate, cambiando il valore di level a 200 (la vostra
            query dovrà funzionare anche se la base di dati contiene più di una tupla con valore di level pari a 185). """
        start_time = time_ns() #inizioe conteggio tempo
        cursor.execute('''UPDATE "Sailor" SET "level" = 200 WHERE "level" = 185''')
        connection.commit()
        end_time = time_ns() # calcola fine
        print(f"Step 7 needs {end_time - start_time} ns") # calcola fine

        """7. Seleziona l’id e l’address di tutte le tuple della tabella Sailor che hanno valore di level pari a 200, e li stampa su stderr."""
        start_time = time_ns() #inizioe conteggio tempo
        cursor.execute('''SELECT id, address FROM "Sailor" as sl WHERE "level" = 200''')
        lista = cursor.fetchall()
        for i in lista:
            print(f"{i[0]},{i[1]}",file=sys.stderr)

        end_time = time_ns() # calcola fine
        print(f"Step 8 needs {end_time - start_time} ns") # calcola fine

        """8. Crea un indice B+tree sull’attributo level."""
        start_time = time_ns() #inizioe conteggio tempo
        cursor.execute('CREATE INDEX index_level ON "Sailor" USING btree ("level");')
        connection.commit()
        """9. Ottiene dal database tutti gli id ​ del milione di tuple della tabella Sailor​ e li stampa su stderr​ ."""
        cursor.execute("""
                SELECT id
                FROM "Sailor"
        """)
        lista = cursor.fetchall()
        for i in lista:
            print(i[0],file=sys.stderr)

        end_time = time_ns() # calcola fine
        print(f"Step 9 needs {end_time - start_time} ns") # calcola fine

        """10. Tutte le tuple con valore di level pari a 200 vengono modificate, cambiando il valore di level a 210 (la vostra
        query dovrà funzionare anche se la base di dati contiene più di una tupla con valore di level pari a 200)."""
        start_time = time_ns() #inizioe conteggio tempo
        cursor.execute('''UPDATE "Sailor" SET "level" = 210 WHERE "level" = 200''')
        connection.commit()

        end_time = time_ns() # calcola fine
        print(f"Step 10 needs {end_time - start_time} ns") # calcola fine

        """11. Seleziona l’id e l’address di tutte le tuple della tabella Sailor che hanno valore di level pari a 210, e li stampa su stderr."""
        start_time = time_ns() #inizioe conteggio tempo
        cursor.execute('''SELECT id, address FROM "Sailor" as sl WHERE "level" = 210''')
        lista = cursor.fetchall()
        for i in lista:
            print(f"{i[0]},{i[1]}",file=sys.stderr)

        end_time = time_ns() # calcola fine
        print(f"Step 11 needs {end_time - start_time} ns") # calcola fine


if __name__ == "__main__":
    assigmant_execute()