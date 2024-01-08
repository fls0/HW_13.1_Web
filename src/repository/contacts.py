from datetime import date, timedelta

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.operators import or_

from src.database.models import Contact
from src.schemas.contacts import ContactCreateSchema, ContactUpdateSchema


async def get_contacts(limit: int, offset: int, db: AsyncSession):
    stmt = select(Contact).offset(offset).limit(limit)
    contacts = await db.execute(stmt)
    return contacts.scalars().all()


async def get_contact(contact_id: int, db: AsyncSession):
    stmt = select(Contact).filter_by(id=contact_id)
    contact = await db.execute(stmt)
    return contact.scalar_one_or_none()


async def create_contact(body: ContactCreateSchema, db: AsyncSession):
    contact = Contact(**body.model_dump(exclude_unset=True))
    db.add(contact)
    await db.commit()
    await db.refresh(contact)
    return contact


async def update_contact(contact_id: int, body: ContactUpdateSchema, db: AsyncSession):
    stmt = select(Contact).filter_by(id=contact_id)
    contact = await db.execute(stmt)
    contact = contact.scalar_one_or_none()

    if contact is not None:
        for field, value in body.model_dump(exclude_unset=True).items():
            setattr(contact, field, value)

        await db.merge(contact)
        await db.commit()
        await db.refresh(contact)

    return contact


async def delete_contact(contact_id: int, db: AsyncSession):
    stmt = select(Contact).filter_by(id=contact_id)
    contact = await db.execute(stmt)
    contact = contact.scalar_one_or_none()

    if contact is not None:
        await db.delete(contact)
        await db.commit()

    return contact


async def search_contacts(search_text: str,
                          limit: int,
                          offset: int,
                          db: AsyncSession):
    stmt = select(Contact).filter(
        or_(
            func.lower(Contact.email).ilike(f"%{search_text.lower()}%"),
            or_(
                func.lower(Contact.first_name).ilike(f"%{search_text.lower()}%"),
                func.lower(Contact.last_name).ilike(f"%{search_text.lower()}%")
            )
        )
    )
    stmt = stmt.limit(limit).offset(offset)
    contacts = await db.execute(stmt)
    return contacts.scalars().all()


async def get_birthdays(days: int, limit: int, offset: int, db: AsyncSession):

    today = date.today()
    end_date = today + timedelta(days=days)

    stmt = select(Contact).filter(
        func.date_part('month', Contact.birthday) >= today.month,
        func.date_part('month', Contact.birthday) <= end_date.month,
        func.date_part('day', Contact.birthday) >= today.day,
        func.date_part('day', Contact.birthday) <= end_date.day
    ).order_by(Contact.birthday)

    stmt = stmt.limit(limit).offset(offset)
    contacts = await db.execute(stmt)
    return contacts.scalars().all()