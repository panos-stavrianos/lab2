import random
import string

divider_str = "-----------------------------------------"
n_receipts = 100000

foods = ["Πίτα (αρτοσκεύασμα)",
         "Βασιλόπιτα",
         "Πίτα με κοτόπουλο και μανιτάρια",
         "Λουκανικόπιτα",
         "Μανιταρόπιτα",
         "Μηλόπιτα",
         "Μπουγάτσα",
         "Πατατόπιτα",
         "Σαμόσα",
         "Σιάμισιη",
         "Σοπάρνικ",
         "Σπανακόπιτα",
         "Ταπιόκα",
         "Τυρόπιτα",
         "Φανουρόπιτα"]


def random_price():
    return round(random.uniform(0.5, 10.0), 2)


def random_spaces():
    spaces = ""
    spaces += ' ' * random.randrange(1, 50)
    return spaces


def random_item():
    food = random.choice(foods)
    name = food[0]
    price = food[1]
    quantity = random.randrange(1, 3)
    sum_item = round(quantity * price, 2)
    return "{1}:{0}{2}{0}{3}{0}{4}{0}".format(random_spaces(), name, quantity, price, sum_item), sum_item


foods = list(map(lambda name: [name, random_price()], foods))

with open("data_{}.txt".format(n_receipts), "w") as f:
    for i in range(n_receipts):
        sum_order = 0
        order = ""
        n_items = random.randrange(1, 5)
        if i == 0:
            order += divider_str
        if random.randrange(100) != 1:  # errors
            order += "\nΑΦΜ:{0}{1}".format(random_spaces(), ''.join(random.choice(string.digits) for _ in range(10)))

        for j in range(n_items):
            if random.randrange(100) != 1:  # errors
                item = random_item()
                order += "\n" + item[0]
                sum_order += item[1]
        order += "\nΣΥΝΟΛΟ:{0}{1}".format(random_spaces(), str(round(sum_order, 2)))
        order += "\n" + divider_str
        f.write(order)
