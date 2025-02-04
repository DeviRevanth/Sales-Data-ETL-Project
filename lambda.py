from faker import Faker
import random
import json
from json import JSONDecodeError
import logging 
from datetime import datetime, timedelta
logger = logging.getLogger()
logger.setLevel(logging.INFO)
def lambda_handler(event, context):
    try:
        logging.info("Lambda Execution Started")
        num_records=10000
        data=generate_sales_data(num_records)
        logging.info(f"{num_records} records Generated")
        return {
            'statusCode': 200,
            'body': data
        }
    except Exception as e:
        logging.error(f"Failed to Generate Data with error -> {e}")
        return {
            'statusCode': 400,
            'body' : "Failure Occured"
        }

def generate_sales_data(num_records):
    try:
        try:
            with open("stores.json", 'r') as stores_data:
                stores = json.load(stores_data)
            with open("products.json", 'r') as products_data:
                products = json.load(products_data)
            fake = Faker('en_IN') # Default Fake data to Indian names
            sales_data = []
            for _ in range(num_records):
                product = random.choice(products)
                store = random.choice(stores)
                quantity = random.randint(1, 3)  # Random quantity between 1 and 3
                transaction_time = datetime.now() - timedelta(days=random.randint(0, 100))  # Last 100 days
                mrp = product.get("mrp")
                sales_data.append({
                    "transaction_id": fake.uuid4(),  # generates fake transaction id 
                    "timestamp": transaction_time.isoformat(),
                    "store_id": store.get("store_id"),
                    "store_name": store.get("store_name"),
                    "store_location": store.get("location"),
                    "product_id": product.get("product_id"),
                    "product_name": product.get("product_name"),
                    "category": product.get("category"),
                    "retail_price": product.get("retail_price"),
                    "quantity": quantity,
                    "brand": product.get("brand"),
                    "mrp": mrp,
                    "total": mrp * quantity,
                    "customer_name": fake.name(), # Generates Fake name 
                    "customer_email": fake.email(),  # Generates Fake mail ids
                })
            return sales_data
        except FileNotFoundError as e:
            logging.error(f"Error -> {e}")
            raise
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON -> {e}")
            raise
        except Exception as e:
            logging.error(f"An unexpected error occurred -> {e}")
            raise
    except Exception as e:
        logging.error(f"Failed to Execute generate_sales_data method in Lambda with error -> {e}")
        raise
