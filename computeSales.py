# I will save[vat,name,sum]
import logging
import os
import re
import sqlite3
from time import time

logging.basicConfig()
logger = logging.getLogger("computeSales")
logger.setLevel(logging.ERROR)  # set logging.DEBUG to see the warnings and benchmark info

file_list = []

conn = sqlite3.connect('database.db')


def db_init():
    c = conn.cursor()
    # Create table
    c.execute('drop table if exists orders')
    c.execute(
        '''
            CREATE TABLE orders(
                id  INTEGER PRIMARY KEY,
                vat TEXT NOT NULL,
                name TEXT NOT NULL,
                sum REAL NOT NULL
           );
        ''')


def print_stats_by_product(name):
    name = name.upper()
    t_start = time()

    query = '''
            SELECT vat, sum(sum) as sum
                from orders
                where name = '{}'
                group by vat
                order by vat
'''.format(name)
    c = conn.cursor()
    c.execute(query)
    for row in c.fetchall():
        print(row[0], round(row[1], 2))
    logger.info("ExecTime:{}ms,  ".format(int((time() - t_start) * 1000)))


def print_stats_by_vat(vat):
    t_start = time()

    query = '''
            SELECT name, sum(sum) as sum
                from orders
                where vat = '{}'
                group by name
                order by vat
'''.format(vat)
    c = conn.cursor()
    c.execute(query)
    for row in c.fetchall():
        print(row[0], round(row[1], 2))
    logger.info("ExecTime:{}ms,  ".format(int((time() - t_start) * 1000)))


def save_item(item_str):
    try:
        if not item_str:
            raise Exception('EMPTY BILL:' + item_str)
        item_str = str(item_str.replace('\t', ' '))
        pattern = re.compile(r'\s +')
        item_str = re.sub(pattern, ' ', item_str)

        items = item_str.split("\n")
        items = filter(lambda s: s != "", items)
        items = list(map(lambda s: s.strip(), items))
        if len(items) < 3:
            raise Exception("EMPTY BILL:" + item_str)

        # vat must be between 6 and 16 characters(Malta 6, Italy 16) and it could be digits and characters
        vat = items.pop(0).split(":")[1].strip()
        if 8 < len(vat) > 16 or not vat.isalnum():
            raise Exception("VAT FORMAT ERROR:{}\non:{}".format(vat, item_str))

        def parse_item(s):
            # name: quantity price sum_item
            item = s.split(":")
            values = item[1].strip().split(" ")

            # I create a dictionary just for clarity
            item = {"name": item[0].strip().upper(), "quantity": int(values[0]), "price": float(values[1]),
                    "sum": float(values[2])}
            if round(item["quantity"] * item["price"], 2) != item["sum"]:
                raise Exception(
                    "WRONG SUM: Expected {} and found {} \non:{}".format(round(item["quantity"] * item["price"], 1),
                                                                         item["sum"], item_str))
            return vat, item['name'], item['sum']

        sum_order = items.pop(len(items) - 1).split(":")[1].strip()  # ΣΥΝΟΛΟ: ΧΧΧ
        items = list(map(parse_item, items))
        sum_pred = round(sum(row[2] for row in items), 2)
        if sum_order == sum_pred:
            raise Exception("WRONG SUM: Expected {} and found {} \non:{}".format(sum_order, sum_pred, item_str))
        c = conn.cursor()
        c.executemany('INSERT INTO orders(vat,name,sum) VALUES (?,?,?)', items)
        return True
    except Exception as e:
        logger.warning(item_str)
        logger.warning(e)


def read_file(filename):
    t_start = time()
    count = 0
    try:
        if filename in file_list:
            raise Exception("This file is already in the system: " + filename)

        file_list.append(filename)
        if not os.path.isfile(filename):
            raise Exception("File {} not found".format(filename))

        tmp_str = ""
        with open(filename, encoding="utf-8") as f:
            f.readline()
            for index, line in enumerate(f):
                if line.startswith("-"):
                    save_item(tmp_str)
                    count += 1
                    tmp_str = f.readline()
                else:
                    tmp_str += line
                if index % 20000 == 0:
                    conn.commit()
        conn.commit()
    except Exception as e:
        logger.warning(e)
    logger.info("ExecTime:{}s, Count: {}".format(round(time() - t_start, 5), count))


db_init()


def menu():
    while True:
        answer = input(
            'Give your preference:\n'
            '\t1: read new input file\n'
            '\t2: print statistics for a specific product\n'
            '\t3: print statistics for a specific AFM\n'
            '\t4: exit the program\n')
        if answer == '1':
            read_file(input("Give the input filename:\n"))
        elif answer == '2':
            print_stats_by_product(input("Give the product name:\n"))
        elif answer == '3':
            print_stats_by_vat(input("Give the AFM:\n"))
        elif answer == '4':
            break


def test_exec():
    read_file('veryEasyInput1')
    read_file('veryEasyInput2')

    print_stats_by_product('ΚΡΕΑΤΑ')
    print_stats_by_product('κρεατα')
    print_stats_by_product('ΧΩΡΙΑΤΙΚΗ')
    print_stats_by_product('ΑΠΑΚΙ')
    print_stats_by_product('ΤΖΑΤΖΙΚΙ')

    print_stats_by_vat('0123456787')
    print_stats_by_vat('0123456788')
    print_stats_by_vat('0123456789')


test_exec()
# menu()
conn.close()
os.remove("database.db")
