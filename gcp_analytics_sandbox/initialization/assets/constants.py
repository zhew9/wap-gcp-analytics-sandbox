from collections import OrderedDict
import datetime
import random
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
import uuid

# initialize some consts and helper functions, like various weights, UUID namespaces, DuckDB/SQL table schemas, etc.

# seasonality weights for days of the week and ISO weeks
DAY_OF_WEEK_WEIGHTS = [
    0.75,  # Monday
    0.75,  # Tuesday
    0.85,  # Wednesday
    1.00,  # Thursday
    1.30,  # Friday
    1.65,  # Saturday
    1.40   # Sunday
]
# index 0 = ISO week 1, index 52 = ISO week 53.
ISO_WEEK_WEIGHTS = [
    # Jan
    0.80, 0.75, 0.75, 0.75, 0.85,
    # Feb (Valentine's Day ~Week 6/7)
    1.10, 0.85, 0.80,
    # Mar (Spring Break / Easter lead-up)
    0.90, 0.90, 0.95, 1.05,
    # Apr (Easter can fall here)
    1.05, 0.95, 0.95, 0.95,
    # May (Mother's Day ~Week 19, Memorial Day ~Week 22)
    1.00, 1.15, 1.00, 1.00, 1.20,
    # Jun (Father's Day ~Week 24)
    1.05, 1.10, 1.05, 1.05,
    # Jul (Independence Day (US) ~Week 27)
    1.25, 1.00, 1.00, 1.00,
    # Aug (Back to School)
    1.25, 1.30, 1.30, 1.30,
    # Sep (Labor Day ~Week 36)
    1.10, 0.90, 0.90, 0.90,
    # Oct (Halloween Lead-up)
    0.95, 1.00, 1.10, 1.10, 1.10,
    # Nov (CRITICAL CHANGES HERE)
    1.00, 1.50, # Early holiday build-up
    2.20, # Week 47: Thanksgiving & BLACK FRIDAY RUSH
    2.40, # Week 48: CYBER MONDAY & continued online sales
    # Dec
    1.90, # Week 49: Holiday shopping
    2.00, # Week 50: Holiday shopping
    2.20, # Week 51: Final holiday shopping rush before Christmas
    1.60, # Week 52: Contains Christmas Day & start of post-Christmas sales
    1.40  # Week 53: Catches the very end of the year / post-Christmas sales
]


import datetime
import random
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


def generate_timestamp_first_half(
    target_date: datetime.date, tz_name: str | None = None
) -> datetime.datetime:
    """
    Generates a random timestamp in the first half of a given day.
    
    This function creates a timestamp between 00:00:00 (inclusive) and
    12:00:00 (exclusive). It correctly handles timezone boundaries and
    Daylight Saving Time (DST) transitions.

    Args:
        target_date: A `datetime.date` object for the desired day.
        tz_name: Optional. An IANA timezone name (e.g., 'America/New_York').
                 If not provided, a naive datetime object is returned.

    Returns:
        A `datetime.datetime` object for the first half of the specified day.
        It will be timezone-aware if a `tz_name` is provided.

    Raises:
        ZoneInfoNotFoundError: If `tz_name` is not a valid IANA timezone.
    """
    if tz_name:
        # --- Timezone-Aware Path ---
        try:
            timezone = ZoneInfo(tz_name)
        except ZoneInfoNotFoundError:
            print(f"Error: Timezone '{tz_name}' not found.")
            raise

        # Define the start (midnight) and end (noon) of the interval.
        start_of_day = datetime.datetime(
            target_date.year, target_date.month, target_date.day, tzinfo=timezone
        )
        midday = datetime.datetime(
            target_date.year, target_date.month, target_date.day, 12, tzinfo=timezone
        )

        # Convert boundaries to Unix timestamps.
        start_timestamp = start_of_day.timestamp()
        end_timestamp = midday.timestamp()

        # Generate a random timestamp within the interval.
        random_ts = random.uniform(start_timestamp, end_timestamp)
        if random_ts >= end_timestamp: # Ensure exclusive upper bound
             random_ts = start_timestamp

        return datetime.datetime.fromtimestamp(random_ts, tz=timezone)

    else:
        # --- Naive Datetime Path ---
        start_of_day = datetime.datetime.combine(target_date, datetime.time.min)
        
        # First half of the day has 12 hours * 3600 seconds/hour = 43,200 seconds.
        random_seconds = random.randint(0, 43199)

        return start_of_day + datetime.timedelta(seconds=random_seconds)


def generate_timestamp_second_half(
    target_date: datetime.date, tz_name: str | None = None
) -> datetime.datetime:
    """
    Generates a random timestamp in the second half of a given day.

    This function creates a timestamp between 12:00:00 (inclusive) and
    24:00:00 (exclusive). It correctly handles timezone boundaries and
    Daylight Saving Time (DST) transitions.

    Args:
        target_date: A `datetime.date` object for the desired day.
        tz_name: Optional. An IANA timezone name (e.g., 'Europe/London').
                 If not provided, a naive datetime object is returned.

    Returns:
        A `datetime.datetime` object for the second half of the specified day.
        It will be timezone-aware if a `tz_name` is provided.

    Raises:
        ZoneInfoNotFoundError: If `tz_name` is not a valid IANA timezone.
    """
    if tz_name:
        # --- Timezone-Aware Path ---
        try:
            timezone = ZoneInfo(tz_name)
        except ZoneInfoNotFoundError:
            print(f"Error: Timezone '{tz_name}' not found.")
            raise

        # Define the start (noon) and end (midnight of the next day) of the interval.
        midday = datetime.datetime(
            target_date.year, target_date.month, target_date.day, 12, tzinfo=timezone
        )
        next_day_date = target_date + datetime.timedelta(days=1)
        start_of_next_day = datetime.datetime(
            next_day_date.year, next_day_date.month, next_day_date.day, tzinfo=timezone
        )

        # Convert boundaries to Unix timestamps.
        start_timestamp = midday.timestamp()
        end_timestamp = start_of_next_day.timestamp()

        # Generate a random timestamp within the interval.
        random_ts = random.uniform(start_timestamp, end_timestamp)
        if random_ts >= end_timestamp: # Ensure exclusive upper bound
             random_ts = start_timestamp

        return datetime.datetime.fromtimestamp(random_ts, tz=timezone)

    else:
        # --- Naive Datetime Path ---
        start_of_second_half = datetime.datetime.combine(target_date, datetime.time(12, 0))
        
        # Second half of the day has 12 hours * 3600 seconds/hour = 43,200 seconds.
        random_seconds = random.randint(0, 43199)

        return start_of_second_half + datetime.timedelta(seconds=random_seconds)


def generate_timestamp_full_day(
    target_date: datetime.date, tz_name: str | None = None
) -> datetime.datetime:
    """
    Generates a random timestamp within a full 24-hour day.

    This function creates a timestamp between 00:00:00 (inclusive) and
    the end of the day (exclusive). It correctly handles timezone boundaries and
    Daylight Saving Time (DST) transitions.

    Args:
        target_date: A `datetime.date` object for the desired day.
        tz_name: Optional. An IANA timezone name (e.g., 'America/New_York').
                 If not provided, a naive datetime object is returned.

    Returns:
        A `datetime.datetime` object for the specified day.
        It will be timezone-aware if a `tz_name` is provided.

    Raises:
        ZoneInfoNotFoundError: If `tz_name` is not a valid IANA timezone.
    """
    if tz_name:
        # --- Timezone-Aware Path ---
        try:
            timezone = ZoneInfo(tz_name)
        except ZoneInfoNotFoundError:
            print(f"Error: Timezone '{tz_name}' not found.")
            raise

        # Define the start (midnight) and end (midnight of the next day) of the interval.
        start_of_day = datetime.datetime(
            target_date.year, target_date.month, target_date.day, tzinfo=timezone
        )
        next_day_date = target_date + datetime.timedelta(days=1)
        start_of_next_day = datetime.datetime(
            next_day_date.year, next_day_date.month, next_day_date.day, tzinfo=timezone
        )

        # Convert boundaries to Unix timestamps.
        start_timestamp = start_of_day.timestamp()
        end_timestamp = start_of_next_day.timestamp()

        # Generate a random timestamp within the interval.
        random_ts = random.uniform(start_timestamp, end_timestamp)
        if random_ts >= end_timestamp: # Ensure exclusive upper bound
             random_ts = start_timestamp

        return datetime.datetime.fromtimestamp(random_ts, tz=timezone)

    else:
        # --- Naive Datetime Path ---
        start_of_day = datetime.datetime.combine(target_date, datetime.time.min)
        
        # A full day has 24 hours * 3600 seconds/hour = 86,400 seconds.
        random_seconds = random.randint(0, 86399)

        return start_of_day + datetime.timedelta(seconds=random_seconds)

# for use when generating orders/customers with some seasonality, we have a set of weights for
#  day of week and week of the year in constants.py
def get_seasonality_weights(target_date: datetime.date):
    """
    Retrieves the day-of-week and ISO week-of-year seasonality weights and
    returns their product as an estimated seasonality factor for the target date.
    """
    # Get day of the week (Monday=0, Sunday=6)
    day_of_week_index = target_date.weekday()
    day_weight = DAY_OF_WEEK_WEIGHTS[day_of_week_index]
    # Get the ISO week number (ranges from 1 to 53)
    _, iso_week, _ = target_date.isocalendar()
    # Get the corresponding week weight from the ISO-aligned list
    # (ISO Week 1 corresponds to index 0)
    week_index = iso_week - 1
    week_weight = ISO_WEEK_WEIGHTS[week_index]

    return day_weight*week_weight


# unweighted and weighted choices for random data
PLATFORMS=["Android", "IOS", "Browser"]
# Canadian PROVINCES, weighted by population
PROVINCES = OrderedDict([ 
        ("ON", 38.45), ("QC", 22.98),("BC", 13.52),("AB", 11.52),
        ("MB", 3.63),("SK", 3.06),("NS", 2.62),("NB", 2.09),
        ("NL", 1.38),("PE", 0.42),("NT", 0.11),("YT", 0.11), ("NU", 0.10)
        ])

# weighted probabilities for things like num of order lines, quantities of products purchased, etc.
NUM_LINES = list(range(1,9))
NUM_LINES_WEIGHTS = [20, 16, 12, 8, 6, 4, 2, 1]
QUANTITY = list(range(1,10))
QUANTITY_WEIGHTS = [100, 80, 60, 40, 20, 10, 5, 2, 1]
CHANNEL_WEIGHTS = [5,3,2]
CUSTOMER_TYPE_WEIGHTS = [[4,1,1,1],[1,4,1,1],[1,1,4,1],[1,1,1,4],[1,1,1,1],[0,1,1,1],[1,0,1,1],[1,1,0,1],[1,1,1,0]]
NUM_PAYMENTS= [1,2,3,4]
NUM_PAYMENTS_WEIGHTS = [125,25,5,1]

# Create a UUID object from our seed string to act as the base namespace
NAMESPACE_SEED = 'com.gcp.dagster.analytics.sandbox.generation.v1'
BASE_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_DNS, NAMESPACE_SEED)

# Now, create a specific, constant namespace for each table we're generating uuids for
NAMESPACE_PRODUCTS = uuid.uuid5(BASE_NAMESPACE, 'products')
NAMESPACE_SUPPLIERS = uuid.uuid5(BASE_NAMESPACE, 'suppliers')
NAMESPACE_CUSTOMERS = uuid.uuid5(BASE_NAMESPACE, 'customers')
NAMESPACE_PROMOTIONS = uuid.uuid5(BASE_NAMESPACE, 'promotions')
NAMESPACE_DISCOUNTS = uuid.uuid5(BASE_NAMESPACE, 'discounts')
NAMESPACE_TRANSACTIONS = uuid.uuid5(BASE_NAMESPACE, 'transactions')
NAMESPACE_TRANSACTION_LINES = uuid.uuid5(BASE_NAMESPACE, 'transaction_lines')
NAMESPACE_TRANSACTION_PAYMENTS = uuid.uuid5(BASE_NAMESPACE, 'transaction_payments')

# SQL Schemas for our source OLTP DuckDB tables
TRANSACTION_HEADERS_SCHEMA = """(
    transaction_id BIGINT PRIMARY KEY,
    transaction_uuid VARCHAR UNIQUE NOT NULL, -- For globally unique identification
    channel_id INTEGER NOT NULL REFERENCES channels(channel_id),
    employee_id BIGINT DEFAULT NULL REFERENCES employees(employee_id), -- default NULL because we're only modeling online retail atm
    customer_id BIGINT NOT NULL REFERENCES customers(customer_id),
    gross_subtotal_amount DECIMAL(10,4) NOT NULL,
    total_tax_amount DECIMAL(10,4) NOT NULL,
    total_amount DECIMAL(10,4) NOT NULL,
    transaction_type VARCHAR DEFAULT 'Purchase' CHECK (transaction_type IN ('Purchase', 'Return')),
    original_transaction_id BIGINT DEFAULT NULL REFERENCES transaction_headers(transaction_id), -- for returns, references the original purchase
    transaction_date TIMESTAMPTZ NOT NULL,
    date_modified TIMESTAMPTZ,
    CONSTRAINT chk_return CHECK (transaction_type = 'Purchase' OR (original_transaction_id IS NOT NULL AND transaction_type = 'Return'))
)"""

TRANSACTION_LINES_SCHEMA = """(
    transaction_line_id BIGINT PRIMARY KEY,
    transaction_id BIGINT NOT NULL REFERENCES transaction_headers(transaction_id),
    line_number INTEGER NOT NULL,
    product_id INTEGER NOT NULL REFERENCES products(product_id),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,4) NOT NULL,
    gross_line_total DECIMAL(10,4) NOT NULL,
    transaction_date TIMESTAMPTZ NOT NULL,
    date_modified TIMESTAMPTZ
)"""

TRANSACTION_DISCOUNTS_SCHEMA = """(
    transaction_discount_id BIGINT PRIMARY KEY,
    promotion_id INTEGER REFERENCES promotions(promotion_id) NOT NULL,
    transaction_line_id BIGINT REFERENCES transaction_lines(transaction_line_id),
    transaction_id BIGINT REFERENCES transaction_headers(transaction_id),
    discounted_units INTEGER,
    discount_amount DECIMAL(10,4) NOT NULL,
    discount_sequence INTEGER DEFAULT 1,
    date_applied TIMESTAMPTZ NOT NULL,
    date_modified TIMESTAMPTZ,
    CONSTRAINT chk_discount_target
    CHECK ((transaction_line_id IS NULL AND transaction_id IS NOT NULL) OR (transaction_line_id IS NOT NULL AND transaction_id IS NULL)),
    CONSTRAINT chk_discount_units
    CHECK ((transaction_line_id IS NULL AND discounted_units IS NULL) OR (transaction_line_id IS NOT NULL AND discounted_units IS NOT NULL))
)"""

TRANSACTION_PAYMENTS_SCHEMA = """(
    transaction_payment_id BIGINT PRIMARY KEY,
    transaction_id BIGINT NOT NULL REFERENCES transaction_headers(transaction_id),
    payment_type_id INTEGER NOT NULL REFERENCES payment_types(payment_type_id),
    amount_paid DECIMAL(10,4) NOT NULL CHECK (amount_paid >= 0),
    -- we're going to abstract away some details or PII like credit/debit card numbers, etc.
    -- we're also going to abstract away gift card details, e.g no gift card table
    payment_provider_reference VARCHAR, -- e.g., Stripe or Adyen transaction ID
    payment_date TIMESTAMPTZ NOT NULL,
    date_modified TIMESTAMPTZ
)"""

CUSTOMERS_SCHEMA = """(
    customer_id BIGINT PRIMARY KEY,
    customer_uuid VARCHAR UNIQUE NOT NULL, -- For globally unique identification
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    email VARCHAR UNIQUE NOT NULL,
    gender VARCHAR NOT NULL CHECK (gender IN ('M', 'F')),
    province VARCHAR NOT NULL,
    date_of_birth DATE,
    status VARCHAR DEFAULT 'Active' CHECK (status IN ('Active', 'Inactive', 'Banned')),
    signup_platform VARCHAR NOT NULL CHECK (signup_platform IN ('Android', 'IOS', 'Browser')),
    registration_date TIMESTAMPTZ NOT NULL,
    date_modified TIMESTAMPTZ,
    date_deleted TIMESTAMPTZ
)"""

# pre-processed/generated, but can be extended to be updated/generated dynamically 

PROMOTIONS_SCHEMA = """(
    promotion_id INTEGER PRIMARY KEY,
    promotion_uuid VARCHAR UNIQUE NOT NULL, -- For globally unique identification
    promotion_name VARCHAR NOT NULL, -- e.g. 'Fall 2024 Puzzle Games Promotion'
    promotion_code VARCHAR UNIQUE, -- e.g. 'FALLSALE'
    description TEXT,
    discount_level VARCHAR NOT NULL CHECK (discount_level IN ('LINE_ITEM', 'ORDER')),
    discount_pool VARCHAR CHECK (discount_pool IN ('PERCENT','FLAT','BOGO','FIXED')),
    stackable BOOLEAN DEFAULT FALSE,
    priority INTEGER NOT NULL DEFAULT 50,
    discount_type VARCHAR NOT NULL CHECK (discount_type IN ('PERCENT', 'FLAT', 'FIXED')),
    discount_value DECIMAL(10,4) NOT NULL,
    min_quantity INTEGER DEFAULT 1,
    min_subtotal DECIMAL(10,4) DEFAULT 0.00,
    promotion_group_id INTEGER REFERENCES promotion_groups(promotion_group_id),
    status VARCHAR DEFAULT 'Active' CHECK (status IN ('Active', 'Expired', 'Upcoming')),
    start_date TIMESTAMPTZ NOT NULL,
    end_date TIMESTAMPTZ,            -- Can be NULL for ongoing promotions
    date_added TIMESTAMPTZ NOT NULL,
    date_removed TIMESTAMPTZ,       -- mainly for promotions removed earlier
    date_modified TIMESTAMPTZ,
    date_deleted TIMESTAMPTZ,
    CONSTRAINT chk_promo_dates CHECK (end_date IS NULL OR end_date >= start_date)
)"""

PROMOTION_GROUPS_SCHEMA = """(
    promotion_group_id INTEGER PRIMARY KEY,
    group_name VARCHAR NOT NULL,          -- e.g., 'Q4 Card Game Clearance 2024', 'Summer Party Games Sale'
    group_type VARCHAR NOT NULL CHECK (group_type IN ('Custom','Category')),
    description TEXT,
    category_id INTEGER REFERENCES categories(category_id), -- Optional, if group is category-specific
    -- could have included publisher_id, author_id, etc. as keys for groupings
    date_added TIMESTAMPTZ NOT NULL, 
    date_removed TIMESTAMPTZ,
    date_modified TIMESTAMPTZ,
    date_deleted TIMESTAMPTZ,
    CONSTRAINT chk_category_group_type CHECK ((category_id IS NULL) OR (category_id IS NOT NULL AND group_type = 'Category'))
)"""

PROMOTION_GROUPS_PRODUCTS_SCHEMA = """(
    promotion_group_id INTEGER references promotion_groups(promotion_group_id),
    product_id INTEGER REFERENCES products(product_id),
    date_added TIMESTAMPTZ NOT NULL,
    date_removed TIMESTAMPTZ,
    date_modified TIMESTAMPTZ,
    date_deleted TIMESTAMPTZ,
    PRIMARY KEY (promotion_group_id, product_id)
)"""

CHANNELS_SCHEMA = """(
    channel_id INTEGER PRIMARY KEY,
    channel_uuid VARCHAR UNIQUE NOT NULL, -- For globally unique identification
    channel_name VARCHAR NOT NULL UNIQUE,
    channel_type VARCHAR NOT NULL CHECK (channel_type IN ('Online', 'In-Store', 'Mobile App', 'Third-Party','Marketplace')),
    description TEXT,
    date_added TIMESTAMPTZ NOT NULL,
    date_removed TIMESTAMPTZ, -- Optional for ongoing channels
    date_modified TIMESTAMPTZ,
    date_deleted TIMESTAMPTZ,
    CONSTRAINT chk_channel_dates CHECK (date_removed IS NULL OR date_removed >= date_added)
)"""

PAYMENT_TYPES_SCHEMA = """(
    payment_type_id INTEGER PRIMARY KEY,
    payment_type_uuid VARCHAR UNIQUE NOT NULL, -- For globally unique identification
    type_name VARCHAR NOT NULL UNIQUE,
    description TEXT,
    date_added TIMESTAMPTZ NOT NULL,
    date_removed TIMESTAMPTZ, -- Optional for ongoing payment types
    date_modified TIMESTAMPTZ,
    date_deleted TIMESTAMPTZ,
    CONSTRAINT chk_payment_type_dates CHECK (date_removed IS NULL OR date_removed >= date_added)
)"""

PRODUCTS_SCHEMA = """(  
    product_id INTEGER PRIMARY KEY,
    product_uuid VARCHAR UNIQUE NOT NULL, -- For globally unique identification
    name VARCHAR NOT NULL,
    description TEXT,
    year_published INTEGER,
    boardgamegeek_id INTEGER,
    minage INTEGER,
    minplaytime INTEGER,
    maxplaytime INTEGER,
    minplayers INTEGER,
    maxplayers INTEGER,
    rating_rank INTEGER,
    usersrated INTEGER,
    average DECIMAL(10,4),
    thumbnail VARCHAR,
    image VARCHAR,
    cost_price DECIMAL(10,4),
    msrp DECIMAL(10,4),
    unit_price DECIMAL(10,4),
    status VARCHAR DEFAULT 'Active' CHECK (status IN ('Active', 'Discontinued', 'Pre-Order', 'Out of Stock')),
    date_added TIMESTAMPTZ NOT NULL,
    date_removed TIMESTAMPTZ,
    date_modified TIMESTAMPTZ,
    date_deleted TIMESTAMPTZ,
    CONSTRAINT chk_product_dates CHECK (date_removed IS NULL OR date_removed >= date_added)
)"""

AUTHORS_SCHEMA = """(
    author_id INTEGER PRIMARY KEY,
    author_uuid VARCHAR UNIQUE NOT NULL, -- For globally unique identification
    author_name VARCHAR NOT NULL, -- e.g., 'Reiner Knizia', 'Klaus Teuber'
    date_added TIMESTAMPTZ NOT NULL,
    date_removed TIMESTAMPTZ,
    date_modified TIMESTAMPTZ,
    date_deleted TIMESTAMPTZ
)"""

AUTHORS_PRODUCTS_SCHEMA = """(
    author_id INTEGER REFERENCES authors(author_id),
    product_id INTEGER REFERENCES products(product_id),
    date_added TIMESTAMPTZ NOT NULL,
    date_removed TIMESTAMPTZ,
    date_modified TIMESTAMPTZ,
    date_deleted TIMESTAMPTZ,
    PRIMARY KEY (author_id, product_id)
)"""

CATEGORIES_SCHEMA = """(
    category_id INTEGER PRIMARY KEY,
    category_name VARCHAR NOT NULL,
    date_added TIMESTAMPTZ NOT NULL,
    date_removed TIMESTAMPTZ,
    date_modified TIMESTAMPTZ,
    date_deleted TIMESTAMPTZ
)"""

CATEGORIES_PRODUCTS_SCHEMA = """(
    category_id INTEGER REFERENCES categories(category_id),
    product_id INTEGER REFERENCES products(product_id),
    date_added TIMESTAMPTZ NOT NULL,
    date_removed TIMESTAMPTZ,
    date_modified TIMESTAMPTZ,
    date_deleted TIMESTAMPTZ,
    PRIMARY KEY (category_id, product_id)
)"""

PUBLISHERS_SCHEMA = """(
    publisher_id INTEGER PRIMARY KEY,
    publisher_uuid VARCHAR UNIQUE NOT NULL, -- For globally unique identification
    publisher_name VARCHAR NOT NULL,
    date_added TIMESTAMPTZ NOT NULL,
    date_removed TIMESTAMPTZ,
    date_modified TIMESTAMPTZ,
    date_deleted TIMESTAMPTZ
)"""

PUBLISHERS_PRODUCTS_SCHEMA = """(
    product_id INTEGER REFERENCES products(product_id),
    publisher_id INTEGER REFERENCES publishers(publisher_id),
    date_added TIMESTAMPTZ NOT NULL,
    date_removed TIMESTAMPTZ,
    date_modified TIMESTAMPTZ,
    date_deleted TIMESTAMPTZ,
    CONSTRAINT chk_publisher_dates CHECK (date_removed IS NULL OR date_removed >= date_added),
    PRIMARY KEY (product_id, publisher_id)
)"""

SUPPLIERS_SCHEMA = """(
    supplier_id INTEGER PRIMARY KEY,
    supplier_uuid VARCHAR UNIQUE NOT NULL, -- For globally unique identification
    supplier_name VARCHAR NOT NULL,
    description TEXT,
    country VARCHAR,
    city VARCHAR,
    province VARCHAR,
    address VARCHAR,
    postal_code VARCHAR,
    contact_name VARCHAR,
    contact_number VARCHAR,
    contact_email VARCHAR,
    date_added TIMESTAMPTZ NOT NULL,
    date_removed TIMESTAMPTZ,
    date_modified TIMESTAMPTZ,
    date_deleted TIMESTAMPTZ,
    CONSTRAINT chk_supplier_dates CHECK (date_removed IS NULL OR date_removed >= date_added)
)"""

SUPPLIERS_PRODUCTS_SCHEMA = """(
    supplier_id INTEGER REFERENCES suppliers(supplier_id),
    product_id INTEGER REFERENCES products(product_id),
    date_added TIMESTAMPTZ NOT NULL,
    date_removed TIMESTAMPTZ,
    date_modified TIMESTAMPTZ,
    date_deleted TIMESTAMPTZ,
    PRIMARY KEY (supplier_id, product_id)
)"""

# we're going to abstract away employee roles and permissions for now, so this is just a basic employee table
# in fact we're likely going to model all transactions as digital, so the employee_id will be NULL for most? transactions
EMPLOYEES_SCHEMA = """(
    employee_id BIGINT PRIMARY KEY,
    employee_uuid VARCHAR UNIQUE NOT NULL, -- For globally unique identification
    employee_number VARCHAR UNIQUE NOT NULL, -- e.g., 'EMP001', 'EMP002'
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    email VARCHAR UNIQUE NOT NULL, -- NOT NULL for potential system access
    phone_number VARCHAR,
    date_of_birth DATE,
    status VARCHAR DEFAULT 'Active' CHECK (status IN ('Active', 'Inactive', 'On Leave')),
    date_hired TIMESTAMPTZ NOT NULL,
    date_terminated TIMESTAMPTZ, -- Optional for ongoing employees
    date_modified TIMESTAMPTZ,
    date_deleted TIMESTAMPTZ,
    CONSTRAINT chk_employee_dates CHECK (date_terminated IS NULL OR date_terminated >= date_hired)
)"""