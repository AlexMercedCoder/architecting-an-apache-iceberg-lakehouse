-- Command: docker exec -it postgres psql -U myuser mydb
-- This command will open the postgres shell, then run the following sql

-- Main Example from Book

CREATE TABLE fashion_sales (
    id SERIAL PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(50),
    sales_amount DECIMAL(10, 2),
    sales_date DATE,
    store_location VARCHAR(100),
    customer_age_group VARCHAR(50),
    campaign_name VARCHAR(100)
);

INSERT INTO fashion_sales (product_name, category, sales_amount, sales_date, store_location, customer_age_group, campaign_name)
VALUES
    ('Slim Fit Jeans', 'Denim', 89.99, '2024-03-01', 'New York', '18-24', 'Spring Launch'),
    ('Leather Jacket', 'Outerwear', 249.99, '2024-03-01', 'Los Angeles', '25-34', 'Spring Launch'),
    ('Graphic T-Shirt', 'Tops', 39.99, '2024-03-02', 'Chicago', '18-24', 'March Madness'),
    ('Summer Dress', 'Dresses', 129.99, '2024-03-03', 'New York', '35-44', 'March Madness'),
    ('Casual Sneakers', 'Footwear', 99.99, '2024-03-03', 'Los Angeles', '25-34', 'Spring Launch');

-- Second Example for Additional Practice

CREATE TABLE sales_data (
    id SERIAL PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(50),
    sales_amount DECIMAL(10, 2),
    sales_date DATE
);

INSERT INTO sales_data (product_name, category, sales_amount, sales_date)
VALUES
    ('Product A', 'Electronics', 1000.50, '2024-03-01'),
    ('Product B', 'Clothing', 750.25, '2024-03-02'),
    ('Product C', 'Home Goods', 1200.75, '2024-03-03'),
    ('Product D', 'Electronics', 900.00, '2024-03-04'),
    ('Product E', 'Clothing', 600.50, '2024-03-05');

-- \q to quit the postgres shell
