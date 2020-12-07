import psycopg2
import random
import sys

TABLE_LENGTH = 100
barr_mudalator = 1
if TABLE_LENGTH / 100 > 0:
    barr_mudalator = TABLE_LENGTH / 100

with (psycopg2.connect(dbname='db_057', user='db_057', host='sci-didattica.unitn.it', password='pass_057')) as connection:
    cursor = connection.cursor()
    cursor.execute('DROP TABLE IF EXISTS "Boat";')
    connection.commit()
    cursor.execute('DROP TABLE IF EXISTS "Sailor";')
    connection.commit()


    cursor.execute('CREATE TABLE "Sailor" ("id" INT PRIMARY KEY, "name" CHAR(50) NOT NULL, "address" CHAR(50) NOT NULL, "age" INT NOT NULL, "level" FLOAT NOT NULL);')
    connection.commit()
    cursor.execute('CREATE TABLE "Boat" ("bid" CHAR(25) PRIMARY KEY, "bname" CHAR(50) NOT NULL, "size" CHAR(30) NOT NULL, "captain" INT NOT NULL REFERENCES "Sailor"("id"));')
    connection.commit()



    level = random.sample(range(186, 3000000), TABLE_LENGTH-1)
    level.append(185)
    name = ["Liam", "Olivia", "Noah", "Emma", "Oliver", "Ava", "William", "Sophia", "Elijah", "Isabella", "James", "Charlotte", "Benjamin", "Amelia", "Lucas", "Mia", "Mason", "Harper", "Ethan", "Evelyn"]
    name_list = []
    for i in range(0,TABLE_LENGTH,1):
        name_list.append(random.randint(0,19))
    

    andress = "via Giangiorgio Oliviero,15"
    age = 42

    # setup toolbar
    print('Load data in "Sailor"')
    sys.stdout.write("[%s]" % (" " * 100))
    sys.stdout.flush()
    sys.stdout.write("\b" * (100+1))

    for i in range(0,TABLE_LENGTH,1): 
        temp = f'''INSERT INTO "Sailor" ("id", "name", "address", "age", "level") VALUES ({i}, '{name[name_list[i]]}', '{andress}', {age}, {level[i]})'''
        cursor.execute(temp)
        connection.commit()
        if i % barr_mudalator == 0:
            sys.stdout.write("-")
            sys.stdout.flush()

    sys.stdout.write("]\n")



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

        # setup toolbar
    print('Load data in "Boat"')
    sys.stdout.write("[%s]" % (" " * 100))
    sys.stdout.flush()
    sys.stdout.write("\b" * (100+1))

    for i in range(0,TABLE_LENGTH,1): 
        temp = f'''INSERT INTO "Boat" ("bid", "bname", "size", "captain") VALUES ({i}, '{bname[bname_list[i]]}', '{size[size_list[i]]}', {captain_list[i]})'''
        cursor.execute(temp)
        connection.commit()
        if i % barr_mudalator == 0:
            sys.stdout.write("-")
            sys.stdout.flush()
            
    sys.stdout.write("]\n")