import json
import random
import faker
import time
from datetime import datetime, timedelta
import sys
import os



def generate_sample_data():
    current_path=os.path.dirname(os.path.abspath(__file__))
    path_arr=current_path.split('\\')

    path_arr=path_arr[:len(path_arr)-1]

    path_arr.append("data")

    new_path="\\".join(path_arr)

    #print(new_path)


    fake = faker.Faker()

    records = []

    current_ts=datetime.now()

    date_list=[]
    #generate random dates before 100 days from current date for better analysis

    for i in range(0,100):
        date_list.append(current_ts+timedelta(days=i))


    # we are generating 100 records in a json for analysis
    for i in range(100):

        time.sleep(0.5)  #so that the timestamps are different

        record = {
            "transactionID": fake.uuid4(),
            "customer": {
                "customerID": random.randint(1,50),  #changed so that we can see dummy data in output
                "firstName": fake.first_name(),
                "lastName": fake.last_name(),
                "email": fake.email(),
                "phoneNumber": fake.phone_number(),
                "billingAddress": {
                    "billing_street": fake.street_address(),
                    "billing_city": fake.city(),
                    "billing_state": fake.state_abbr(),
                    "billing_zipcode": fake.zipcode(),
                },
                "shippingAddress": {
                    "shipping__street": fake.street_address(),
                    "shipping__city": fake.city(),
                    "shipping__state": fake.state_abbr(),
                    "shipping_zipcode": fake.zipcode(),
                },
            },
            "timestamp": random.choice(date_list).isoformat(),
            "products": [
                {
                    "productID": fake.uuid4(),
                    "productName": fake.word(),
                    "category": fake.word(),
                    "quantity": random.randint(1, 5),
                    "unitPrice": round(random.randint(10, 1000), 2),
                }
                for j in range(random.randint(1, 5))
            ],
            "payment": {
                "paymentMethod": random.choice(["Credit Card", "PayPal", "Google Pay"]),
                "cardType": fake.credit_card_provider(),
                "cardLast4Digits": fake.credit_card_number(card_type=None)[-4:],
                "paymentStatus": random.choice(["Success", "Pending", "Failed"]),
            },
            "orderStatus": random.choice(["Pending", "Shipped", "Delivered"]),
        }

        records.append(record)

    # write to a JSON file
    with open(f"{new_path}\\sample_data.json", "w") as json_file:
        json.dump(records, json_file, indent=2)

