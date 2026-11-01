CREATE TABLE customers (
    customer_id INTEGER      PRIMARY KEY,
    name        VARCHAR(50)  NOT NULL,
    city        VARCHAR(30)  NOT NULL
);

CREATE TABLE orders (
    order_id    INTEGER       PRIMARY KEY,
    customer_id INTEGER       NOT NULL REFERENCES customers(customer_id),
    amount      NUMERIC(10,2) NOT NULL,
    order_date  DATE          NOT NULL
);

INSERT INTO customers (customer_id, name, city) VALUES
    (1, 'Ali Mammadov',    'Baku'),
    (2, 'Leyla Hasanova',  'Baku'),
    (3, 'Murad Aliyev',    'Ganja'),
    (4, 'Aysel Quliyeva',  'Sumgait'),
    (5, 'Nigar Rzayeva',   'Ganja'),
    (6, 'Rauf Safarov',    'Baku');

INSERT INTO orders (order_id, customer_id, amount, order_date) VALUES
    (1,  1,  50.00, '2026-10-20'),
    (2,  2, 120.50, '2026-10-20'),
    (3,  3,  75.00, '2026-10-20'),
    (4,  1, 200.00, '2026-10-21'),
    (5,  4,  60.00, '2026-10-21'),
    (6,  5,  35.50, '2026-10-21'),
    (7,  3,  90.00, '2026-10-21'),
    (8,  6, 150.00, '2026-10-22'),
    (9,  2,  80.00, '2026-10-22'),
    (10, 6,  45.00, '2026-10-22'),
    (11, 4, 110.00, '2026-10-22'),
    (12, 5, 300.00, '2026-10-23'),
    (13, 1,  25.00, '2026-10-23'),
    (14, 3,  60.00, '2026-10-24'),
    (15, 5,  40.00, '2026-10-24'),
    (16, 2,  95.00, '2026-10-24'),
    (17, 4,  20.00, '2026-10-24'),
    (18, 6, 130.00, '2026-10-24');
