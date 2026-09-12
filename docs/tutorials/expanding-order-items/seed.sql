CREATE TABLE orders (id BIGINT PRIMARY KEY, customer VARCHAR(64), revision BIGINT);
INSERT INTO orders VALUES (1, 'ada', 0), (2, 'lin', 1);
