import itertools
import psycopg2
import psycopg2.errorcodes
from psycopg2 import Error
import csv


#підключення до бази
def create_connection(db_name, db_user, db_password, db_host):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
        )
        print("З'єднання з БД  успішне")
    except Error as err:
        print(f"{err}")
    return connection

connect = create_connection("postgres", "postgres", "mydb", "localhost", )


# ф-ія створення власної бази даних
def create_database(connect, query):
    connect.autocommit = True
    curs = connect.cursor()
    try:
        curs.execute(query)
        print("Успішно")
    except Error as err:
        print(f"{err} ")


# створюємо БД
create_database_query = "CREATE DATABASE DB_lab1"
create_database(connect, create_database_query)

connect = create_connection("DB_lab1", "postgres", "mydb", "localhost", )

def execute_query(connect, query):
    connect.autocommit = True
    curs = connect.cursor()
    try:
        curs.execute(query)
        print("Успішний запит")
    except Error as err:
        print(f"{err}")




""" якщо таблиця з такою назвою уже існує - розкомітьте наступні 2 рядки для її видалення"""
"""delete_ZNO_2019_2020_table = """ DROP TABLE IF EXISTS physic_2019_2020"""
execute_query(connection, delete_physic_2019_2020_table)"""

create_ZNO_2019_2020_table = """
CREATE TABLE IF NOT EXISTS physic_2019_2020 (
    YEAR                 INT NOT NULL,
    OUTID	             TEXT PRIMARY KEY,
    Birth	             INT NOT NULL,
    SEXTYPENAME	         TEXT NOT NULL,
    REGNAME	             TEXT NOT NULL,
    AREANAME             TEXT NOT NULL,
    TERNAME	             TEXT NOT NULL,
    REGTYPENAME	         TEXT NOT NULL,
    TerTypeName     	 TEXT NOT NULL,
    ClassProfileNAME     TEXT,
    ClassLangName        TEXT,
    EONAME	             TEXT,
    EOTYPENAME	         TEXT,
    EORegName	         TEXT,
    EOAreaName	         TEXT,
    EOTerName	         TEXT,
    EOParent	         TEXT,
    UkrTest	             TEXT,
    UkrTestStatus	     TEXT,
    UkrBall100	         DECIMAL(5,2),
    UkrBall12	         DECIMAL(5,2),
    UkrBall	             DECIMAL(5,2),
    UkrAdaptScale	     DECIMAL(5,2),
    UkrPTName	         TEXT ,
    UkrPTRegName         TEXT ,
    UkrPTAreaName	     TEXT,
    UkrPTTerName	     TEXT,
    histTest	         TEXT,
    HistLang	         TEXT,
    histTestStatus	     TEXT,
    histBall100	         DECIMAL(5,2),
    histBall12	         DECIMAL(5,2),
    histBall	         DECIMAL(5,2),
    histPTName	         TEXT,
    histPTRegName	     TEXT,
    histPTAreaName	     TEXT,
    histPTTerName	     TEXT,
    mathTest	         TEXT,
    mathLang	         TEXT,
    mathTestStatus	     TEXT,
    mathBall100	         DECIMAL(5,2),
    mathBall12	         DECIMAL(5,2),
    mathBall	         DECIMAL(5,2),
    mathPTName	         TEXT,
    mathPTRegName        TEXT,
    mathPTAreaName	     TEXT,
    mathPTTerName	     TEXT,
    physTest	         TEXT,
    physLang	         TEXT,
    physTestStatus	     TEXT,
    physBall100	         DECIMAL(5,2),
    physBall12	         DECIMAL(5,2),
    physBall	         DECIMAL(5,2),
    physPTName	         TEXT,
    physPTRegName	     TEXT,
    physPTAreaName	     TEXT,
    physPTTerName	     TEXT,
    chemTest	         TEXT,
    chemLang	         TEXT,
    chemTestStatus	     TEXT,
    chemBall100	         DECIMAL(5,2),
    chemBall12	         DECIMAL(5,2),
    chemBall	         DECIMAL(5,2),
    chemPTName	         TEXT,
    chemPTRegName	     TEXT,
    chemPTAreaName	     TEXT,
    chemPTTerName	     TEXT,
    bioTest	             TEXT,
    bioLang	             TEXT,
    bioTestStatus	     TEXT,
    bioBall100	         DECIMAL(5,2),
    bioBall12	         DECIMAL(5,2),
    bioBall	             DECIMAL(5,2),
    bioPTName	         TEXT,
    bioPTRegName	     TEXT,
    bioPTAreaName	     TEXT,
    bioPTTerName	     TEXT,
    geoTest	             TEXT,
    geoLang	             TEXT,
    geoTestStatus	     TEXT,
    geoBall100	         DECIMAL(5,2),
    geoBall12	         DECIMAL(5,2),
    geoBall	             DECIMAL(5,2),
    geoPTName	         TEXT,
    geoPTRegName	     TEXT,
    geoPTAreaName	     TEXT,
    geoPTTerName	     TEXT,
    engTest	             TEXT,
    engTestStatus	     TEXT,
    engBall100	         DECIMAL(5,2),
    engBall12	         DECIMAL(5,2),
    engDPALevel	         TEXT,
    engBall	             DECIMAL(5,2),
    engPTName	         TEXT,
    engPTRegName         TEXT,
    engPTAreaName	     TEXT,
    engPTTerName	     TEXT,
    fraTest	             TEXT,
    fraTestStatus	     TEXT,
    fraBall100	         DECIMAL(5,2),
    fraBall12	         DECIMAL(5,2),
    fraDPALevel	         TEXT,
    fraBall	             DECIMAL(5,2),
    fraPTName	         TEXT,
    fraPTRegName	     TEXT,
    fraPTAreaName	     TEXT,
    fraPTTerName	     TEXT,
    deuTest	             TEXT,
    deuTestStatus	     TEXT,
    deuBall100	         DECIMAL(5,2),
    deuBall12	         DECIMAL(5,2),
    deuDPALevel	         TEXT,
    deuBall	             DECIMAL(5,2),
    deuPTName	         TEXT,
    deuPTRegName	     TEXT,
    deuPTAreaName	     TEXT,
    deuPTTerName	     TEXT,
    spaTest	             TEXT,
    spaTestStatus	     TEXT,
    spaBall100	         DECIMAL(5,2),
    spaBall12	         DECIMAL(5,2),
    spaDPALevel	         TEXT,
    spaBall	             DECIMAL(5,2),
    spaPTName	         TEXT,
    spaPTRegName	     TEXT,
    spaPTAreaName	     TEXT,
    spaPTTerName	     TEXT
)
"""

execute_query(connect, create_physic_2019_2020_table)


def insert_data_from_csv_to_table(f, year, connect, cursor):


    with open(f, "r", encoding="cp1251") as csv_file:

        # cписок назв колонок(потрібен для подальшого реформування даних в потрібні формати)
        columns = ["OUTID", "Birth", "SEXTYPENAME", "REGNAME", "AREANAME", "TERNAME", "REGTYPENAME", "TerTypeName",
                   "ClassProfileNAME", "ClassLangName", "EONAME", "EOTYPENAME", "EORegName", "EOAreaName", "EOTerName",
                   "EOParent", "UkrTest", "UkrTestStatus", "UkrBall100", "UkrBall12", "UkrBall", "UkrAdaptScale",
                   "UkrPTName", "UkrPTRegName", "UkrPTAreaName", "UkrPTTerName", "histTest", "HistLang",
                   "histTestStatus", "histBall100", "histBall12", "histBall", "histPTName", "histPTRegName",
                   "histPTAreaName", "histPTTerName", "mathTest", "mathLang", "mathTestStatus", "mathBall100",
                   "mathBall12", "mathBall", "mathPTName", "mathPTRegName", "mathPTAreaName", "mathPTTerName",
                   "physTest", "physLang", "physTestStatus", "physBall100", "physBall12", "physBall",
                   "physPTName", "physPTRegName", "physPTAreaName", "physPTTerName", "chemTest", "chemLang",
                   "chemTestStatus", "chemBall100", "chemBall12", "chemBall", "chemPTName", "chemPTRegName",
                   "chemPTAreaName", "chemPTTerName", "bioTest", "bioLang", "bioTestStatus", "bioBall100", "bioBall12",
                   "bioBall", "bioPTName", "bioPTRegName", "bioPTAreaName", "bioPTTerName", "geoTest	", "geoLang",
                   "geoTestStatus", "geoBall100", "geoBall12", "geoBall", "geoPTName", "geoPTRegName", "geoPTAreaName",
                   "geoPTTerName", "engTest", "engTestStatus", "engBall100", "engBall12", "engDPALevel", "engBall",
                   "engPTName", "engPTRegName", "engPTAreaName", "engPTTerName", "fraTest", "fraTestStatus",
                   "fraBall100", "fraBall12", "fraDPALevel", "fraBall", "fraPTName", "fraPTRegName", "fraPTAreaName",
                   "fraPTTerName", "deuTest", "deuTestStatus", "deuBall100", "deuBall12", "deuDPALevel", "deuBall",
                   "deuPTName", "deuPTRegName", "deuPTAreaName", "deuPTTerName", "spaTest", "spaTestStatus",
                   "spaBall100",
                   "spaBall12", "spaDPALevel", "spaBall", "spaPTName", "spaPTRegName", "spaPTAreaName", "spaPTTerName"]

        reader = csv.DictReader(csv_file, delimiter=';')

        # к-сть закомічених партій
        quantity_parties = 0
        # розмір партії
        size_parties = 300
        inserting = False

        # поки не вставимо всі рядки
        while not inserting:
            try:
                sinsert = '''INSERT INTO physic_2019_2020 (year, ''' + ', '.join(columns) + ') VALUES '
                count = 0
                for row in reader:
                    count += 1
                    # реформуємо дані в коректні формати
                    for i in row:
                        # пропускаємо порожні комірки
                        if row[i] == 'null':
                            pass
                        # текстові значення беремо в одинарні лапки
                        elif i.lower() != 'birth' and 'ball' not in i.lower():
                            row[i] = "'" + row[i].replace("'", "''") + "'"
                        # в числових значеннях замінюємо кому на крапку
                        elif 'ball100' in i.lower():
                            row[i] = row[i].replace(',', '.')
                    sinsert += '\n\t(' + str(year) + ', ' + ','.join(row.values()) + '),'

                    # якщо додали 300 рядків -коммітимо транзакцію
                    if count == size_parties:
                        count = 0
                        sinsert = sinsert.rstrip(',') + ';'
                        curs.execute(sinsert)
                        connect.commit()

                        quantity_parties += 1
                        sinsert = '''INSERT INTO physic_2019_2020 (year, ''' + ', '.join(columns) + ') VALUES '

                # якщо рядків більше немає -- коммітимо транзакцію і закінчуємо вставку
                if count != 0:
                    sinsert = sinsert.rstrip(',') + ';'
                    curs.execute(sinsert)
                    connect.commit()
                inserting = True

            except psycopg2.OperationalError as e:
                # якщо з'єднання з базою даних втрачено
                if e.pgcode == psycopg2.errorcodes.ADMIN_SHUTDOWN:
                    print("Зникло з'єднання з базою")

                    connection_restored = False
                    while not connection_restored:
                        try:
                            # намагаємось підключитись до бази даних
                            """для підключення до Вашої БД замініть значення в 3 лапках на власний пароль"""
                            connect = create_connection("lab1", "postgres", "Maya!maks15", "localhost", )
                            cursor = connect.cursor()

                            connect_restored = True

                        except Error as err:
                            pass

                    print("З'єднання з базою відновлено")
                    csv_file.seek(0, 0)
                    reader = itertools.islice(csv.DictReader(csv_file, delimiter=';'),
                                              quantity_parties * size_parties, None)

    return connect, curs

# SQL запит ( порівняти найкращий бал з укр.мови і літератури у 2020 і 2019 році)
squery = '''
    select_query = '''
    SELECT regname, year, max(physBall100)
    FROM tbl_zno_data
    WHERE physTestStatus = 'Зараховано'
    GROUP BY regname, year;
'''

curs.execute(squery)

with open('physic_2019_2020.csv', 'w', encoding="utf-8") as result:
    save = csv.writer(result)
    save.saverow(['Область', 'Максимальний бал', Рік'])
    row=cursor.fetchone()
    while row:
        writer.writerow(row)
        row = cursor.fetchone()


curs.close()
connect.close()'''