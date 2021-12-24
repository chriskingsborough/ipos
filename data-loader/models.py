from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, Column
from sqlalchemy import (
    Integer,
    BIGINT,
    String,
    Text,
    Date,
    DateTime,
    Float,
    Numeric
)

Base = declarative_base()

class IPOs(Base):
    """Initial Publie Offerings that have occurred
    
    Data source: https://www.iposcoop.com/last-100-ipos/
    """

    __tablename__ = 'ipos'

    id = Column("id", BIGINT, primary_key=True)
    company = Column("company", String(400))
    symbol = Column("symbol", String(10))
    industry = Column("industry", String(100))
    offer_date = Column("offer_date", Date)
    shares_million = Column("shares_millions", Float)
    offer_price = Column("offer_price", Float)
    first_day_close = Column("first_day_close", Float)
    fingerprint = Column("fingerprint", String(100))

class UpcomingIPOs(Base):
    """Upcoming Initial Publie Offerings that not have occurred
    
    Data source: https://www.iposcoop.com/ipo-calendar/
    """

    __tablename__ = 'upcoming_ipos'

    id = Column("id", BIGINT, primary_key=True)
    company = Column("company", String(400))
    symbol = Column("symbol", String(10))
    lead_managers = Column("lead_managers", String(1000))
    shares_million = Column("shares_millions", Float)
    price_low = Column("price_low", Float)
    price_high = Column("price_high", Float)
    est_volume = Column("est_volume", String(50))
    expected_to_trade = Column("expected_to_trade", Date)
    fingerprint = Column("fingerprint", String(100))


# class DateDim(Table):
#     """Date Dimension"""

#     __name__ = 'date_dim'

#     id = Column("id", BIGINT, primary_key=True)
#     date = Column("date", Date)
#     day_of_month = Column("day_of_month", Integer)
#     day_of_week = Column("day_of_week", Integer)
#     month = Column("month", Integer)
#     quarter = Column("quarter", Integer)
#     year = Column("year", Integer)

# class LeadManagersDim(Table):
#     """Lead Manager"""

#     __name__ = 'lead_managers_dim'

#     id = Column("id", BIGINT, primary_key=True)
#     lead_manager = Column("lead_manager", String(400))

# class LeadManagersBridge(Table):
#     """Bridge table between lead managers and stocks"""

#     __name__ = 'lead_managers_bridge'

#     id = Column("id", BIGINT, primary_key=True)
#     lead_manager_id = Column("lead_manager_id", BIGINT)
#     symbol = Column("symbol", String(10))

