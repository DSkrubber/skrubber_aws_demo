from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import INTEGER, VARCHAR

from app.database import Base


class User(Base):
    __tablename__ = "users"

    user_id = Column(INTEGER, primary_key=True)
    user_role = Column(VARCHAR, default="employee")
