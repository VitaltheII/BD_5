import psycopg2
from pprint import pprint

#Функция, создающая структуру БД (таблицы)
def create_tables(connect):
        with connect.cursor() as curs:
            curs.execute("""
            DROP TABLE client_phone
            ;""")
            connect.commit()

            curs.execute("""
            DROP TABLE client_db
            ;""")
            connect.commit()

            curs.execute("""
            DROP TABLE phone_number
            ;""")
            connect.commit()


            curs.execute("""
            CREATE TABLE IF NOT EXISTS client_db (
            id SERIAL PRIMARY KEY,
            name VARCHAR(40) not null,
            surname VARCHAR(40) not null,
            email VARCHAR(40) not null UNIQUE)
            ;""")
            connect.commit()

            curs.execute("""
            CREATE TABLE IF NOT EXISTS phone_number (
            id SERIAL PRIMARY KEY,
            phone_number BIGINT UNIQUE)
            ;""")
            connect.commit()

            curs.execute("""
            CREATE TABLE IF NOT EXISTS client_phone (
            id SERIAL PRIMARY KEY,
            name_id INTEGER REFERENCES client_db (id),
            phone_id INTEGER REFERENCES phone_number (id))
            ;""")
            connect.commit()

            connect.close()


#Функция, позволяющая добавить нового клиента
def insert_new_client(connect, client_info):
        new_client_id = 0
        new_phone_id = 0
        with connect.cursor() as curs:
            curs.execute("""
            SELECT id FROM client_db
            ORDER BY id DESC
            LIMIT 1;
            """)
            if not curs.fetchone():
                new_client_id = 1
            else:
                curs.execute("""
                SELECT id FROM client_db
                ORDER BY id DESC
                LIMIT 1;
                """)
                new_client_id = curs.fetchone()[0] + 1

            curs.execute("""
            SELECT id FROM phone_number
            ORDER BY id DESC
            LIMIT 1;
            """)
            if not curs.fetchone():
                new_phone_id = 1
            else:
                curs.execute("""
                SELECT id FROM phone_number
                ORDER BY id DESC
                LIMIT 1;
                """)
                new_phone_id = curs.fetchone()[0] + 1

            curs.execute("""
            INSERT INTO client_db (name, surname, email)
            VALUES (%s, %s, %s);
            """, (client_info[0], client_info[1], client_info[2]))
            connect.commit()

            if len(client_info) == 4:
                curs.execute("""
                INSERT INTO phone_number (phone_number)
                VALUES (%s);
                """, (client_info[3],))
                connect.commit()

                curs.execute("""
                INSERT INTO client_phone (name_id, phone_id)
                VALUES (%s, %s);
                """, (new_client_id, new_phone_id))
                connect.commit()

            connect.close()

#Вспомогательные фукнции для проверки
def select_table(connect, table_name):
    with connect.cursor() as curs:
        curs.execute("""
            SELECT * FROM client_db;
            """)
        print(curs.fetchall())


def select_phone(connect, table_name):
    with connect.cursor() as curs:
        curs.execute("""
            SELECT * FROM phone_number;
            """)
        print(curs.fetchall())


def select_cp(connect, table_name):
    with connect.cursor() as curs:
        curs.execute("""
            SELECT * FROM client_phone;
            """)
        print(curs.fetchall())
    connect.close()



def find_clientid(connect, email):
    with connect.cursor() as curs:
        curs.execute("""
            SELECT id FROM client_db
            WHERE email = %s;
            """, (email,))
        return curs.fetchone()[0]


def find_phoneid(connect, phone):
    with connect.cursor() as curs:
        curs.execute("""
            SELECT id FROM phone_number
            WHERE phone_number = %s;
            """, (phone,))
        return curs.fetchone()[0]


#Функция, позволяющая добавить телефон для существующего клиента
def add_phone(connect, email, newphone):
    with connect.cursor() as curs:
        curs.execute("""
            INSERT INTO phone_number (phone_number)
            VALUES (%s);
            """, ([newphone]))
        connect.commit()

        curs.execute("""
        INSERT INTO client_phone (name_id, phone_id)
        VALUES (%s, %s);
        """, (find_clientid(connect, email), find_phoneid(connect, newphone)))
        connect.commit()



        connect.close()

#Функция, позволяющая изменить данные о клиенте
def change_name(connect, email, newname):
    with connect.cursor() as curs:
        curs.execute("""
            UPDATE client_db
            SET name = %s
            WHERE id = %s;
            """, (newname, find_clientid(connect, email)))
        connect.commit()

#Функция, позволяющая изменить данные о клиенте
def change_email(connect, email, newemail):
    with connect.cursor() as curs:
        curs.execute("""
            UPDATE client_db
            SET email = %s
            WHERE id = %s;
            """, (newemail, find_clientid(connect, email)))
        connect.commit()

#Функция, позволяющая удалить телефон для существующего клиента
def delete_phone(connect, phone):
    with connect.cursor() as curs:
        curs.execute("""
        SELECT id from client_phone
        WHERE phone_id = %s;
        """, (find_phoneid(connect, phone),))
        id = curs.fetchone()


        curs.execute("""
            DELETE from client_phone
            WHERE id = %s;
            """, (id,))
        connect.commit()

        curs.execute("""
        DELETE from phone_number
        WHERE id = %s;
        """, (find_phoneid(connect, phone),))
        connect.commit()

#Функция, позволяющая удалить существующего клиента
def delete_client(connect, email):
    with connect.cursor() as curs:
        curs.execute("""
        SELECT phone_id from client_phone
        WHERE name_id = %s;
        """, (find_clientid(connect, email),))

        for el in curs.fetchall():
            curs.execute("""
            DELETE from client_phone
            WHERE phone_id = %s;
            """, (el,))
            connect.commit()

            curs.execute("""
            DELETE from phone_number
            WHERE id = %s;
            """, (el,))
            connect.commit()

        curs.execute("""
        DELETE from client_db
        WHERE id = %s;
        """, (find_clientid(connect, email),))
        connect.commit()

#Функция, позволяющая найти клиента по его данным (имени, фамилии, email-у или телефону)
def findclient(connect, dict):
    list = []
    if 'email' in dict.keys():
        with connect.cursor() as curs:
            curs.execute("""
            SELECT name, surname, email, phone_number from client_db c
            LEFT JOIN client_phone cp ON c.id = cp.name_id
            LEFT JOIN phone_number pn ON cp.phone_id = pn.id
            WHERE email = %s;
            """, (dict['email'], ))
            output1 = curs.fetchall()
            for i in output1:
                list.append(i)

    if 'phone' in dict.keys():
        with connect.cursor() as curs:
            curs.execute("""
            SELECT name, surname, email, phone_number from client_db c
            LEFT JOIN client_phone cp ON c.id = cp.name_id
            LEFT JOIN phone_number pn ON cp.phone_id = pn.id
            WHERE phone_number = %s;
            """, (dict['phone'],))
            output2 = curs.fetchall()
            for w in output2:
                list.append(w)

    if 'name' in dict.keys():
        with connect.cursor() as curs:
            curs.execute("""
            SELECT name, surname, email, phone_number from client_db c
            LEFT JOIN client_phone cp ON c.id = cp.name_id
            LEFT JOIN phone_number pn ON cp.phone_id = pn.id
            WHERE name = %s;
            """, (dict['name'],))
            output3 = curs.fetchall()
            for g in output3:
                list.append(g)

    if 'surname' in dict.keys():
        with connect.cursor() as curs:
            curs.execute("""
            SELECT name, surname, email, phone_number from client_db c
            LEFT JOIN client_phone cp ON c.id = cp.name_id
            LEFT JOIN phone_number pn ON cp.phone_id = pn.id
            WHERE surname = %s;
            """, (dict['surname'],))
            output4 = curs.fetchall()
            for m in output4:
                list.append(m)

    print(set(list))



#Воспроизводим
conn = psycopg2.connect(database='', user='',
                        password='')

client_list = ['Cal', 'Tilky', 'work89@yandex.ru', 7654]

# create_tables(conn)
# insert_new_client(conn, client_list)
# add_phone(conn, 'work89@yandex.ru', 7654)
# delete_phone(conn, 89456454545)
# delete_client(conn, 'work8@yandex.ru')
# dict = {'name': 'Jack', 'email': 'work10@yandex.ru', 'phone': 89456}
# findclient(conn, dict)


# select_table(conn, 'client_db')
# select_phone(conn, 'client_db')
# select_cp(conn, 'client_db')