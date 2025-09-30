"""
Synthetic e-commerce data generator — populates PostgreSQL source
database with realistic fake data for pipeline testing.
"""

import random
import uuid
from datetime import datetime, timedelta

import psycopg2
from faker import Faker

from config.settings import settings

fake = Faker()

CATEGORIES = ["electronics", "clothing", "home", "sports", "books", "beauty", "toys"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "bank_transfer", "crypto"]
STATUSES = ["pending", "processing", "completed", "shipped", "refunded", "cancelled"]
SEGMENTS = ["premium", "standard", "basic", "new"]


def create_tables(conn):
    """Create source tables in PostgreSQL."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS customers (
                customer_id VARCHAR(36) PRIMARY KEY,
                email VARCHAR(255) UNIQUE,
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                phone VARCHAR(20),
                segment VARCHAR(20),
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS orders (
                order_id VARCHAR(36) PRIMARY KEY,
                customer_id VARCHAR(36) REFERENCES customers(customer_id),
                order_date DATE,
                status VARCHAR(20),
                total_amount DECIMAL(10,2),
                shipping_address TEXT,
                payment_method VARCHAR(30)
            );

            CREATE TABLE IF NOT EXISTS products (
                product_id VARCHAR(36) PRIMARY KEY,
                title VARCHAR(255),
                description TEXT,
                price DECIMAL(10,2),
                category VARCHAR(50),
                created_at TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS order_items (
                order_item_id VARCHAR(36) PRIMARY KEY,
                order_id VARCHAR(36) REFERENCES orders(order_id),
                product_id VARCHAR(36) REFERENCES products(product_id),
                quantity INT,
                unit_price DECIMAL(10,2),
                discount DECIMAL(5,4) DEFAULT 0
            );
        """)
    conn.commit()


def generate_data(conn, n_customers=500, n_products=100, n_orders=2000):
    """Generate and insert fake e-commerce data."""
    customers = []
    for _ in range(n_customers):
        customers.append(
            (
                str(uuid.uuid4()),
                fake.unique.email(),
                fake.first_name(),
                fake.last_name(),
                fake.phone_number()[:20],
                random.choice(SEGMENTS),
                fake.date_time_between(start_date="-2y", end_date="now"),
                fake.date_time_between(start_date="-6m", end_date="now"),
            )
        )

    products = []
    for _ in range(n_products):
        products.append(
            (
                str(uuid.uuid4()),
                fake.catch_phrase(),
                fake.text(max_nb_chars=200),
                round(random.uniform(5.0, 500.0), 2),
                random.choice(CATEGORIES),
            )
        )

    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO customers (customer_id, email, first_name, last_name, phone, segment, created_at, updated_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
            customers,
        )
        cur.executemany(
            "INSERT INTO products (product_id, title, description, price, category) "
            "VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
            products,
        )

        for _ in range(n_orders):
            order_id = str(uuid.uuid4())
            customer = random.choice(customers)
            n_items = random.randint(1, 5)
            order_products = random.sample(products, min(n_items, len(products)))
            total = 0

            cur.execute(
                "INSERT INTO orders (order_id, customer_id, order_date, status, total_amount, shipping_address, payment_method) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (
                    order_id,
                    customer[0],
                    fake.date_between(start_date="-1y", end_date="today"),
                    random.choice(STATUSES),
                    0,  # placeholder
                    fake.address(),
                    random.choice(PAYMENT_METHODS),
                ),
            )

            for prod in order_products:
                qty = random.randint(1, 3)
                discount = round(random.uniform(0, 0.3), 4) if random.random() > 0.7 else 0
                line_total = prod[3] * qty * (1 - discount)
                total += line_total

                cur.execute(
                    "INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, discount) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (str(uuid.uuid4()), order_id, prod[0], qty, prod[3], discount),
                )

            cur.execute(
                "UPDATE orders SET total_amount = %s WHERE order_id = %s",
                (round(total, 2), order_id),
            )

    conn.commit()
    print(f"Generated: {n_customers} customers, {n_products} products, {n_orders} orders")


if __name__ == "__main__":
    pg = settings.postgres
    conn = psycopg2.connect(
        host=pg.host, port=pg.port, dbname=pg.db, user=pg.user, password=pg.password
    )
    create_tables(conn)
    generate_data(conn)
    conn.close()
