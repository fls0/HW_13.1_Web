from datetime import date, datetime

from pydantic import BaseModel, EmailStr, Field


class ContactSchema(BaseModel):
    first_name: str = Field(min_length=3, max_length=150)
    last_name: str = Field(min_length=3, max_length=150)
    email: EmailStr = Field(pattern=r"^[a-zA-Z0-9.+_-]+@[a-zA-Z0-9.-]+.[a-z]+$")
    phone_number: str = Field(pattern=r"^(?:\+38|38|8|)?[0-9]{7,11}$")
    birthday: date = Field(default=None)


class ContactCreateSchema(ContactSchema):
    pass


class ContactUpdateSchema(ContactSchema):
    pass


class ContactResponseSchema(BaseModel):
    id: int = 1
    first_name: str
    last_name: str
    email: EmailStr
    phone_number: str
    birthday: date

    class Config:
        from_attributes = True