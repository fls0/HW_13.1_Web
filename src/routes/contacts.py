from fastapi import APIRouter, HTTPException, Depends, status, Path, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.db import get_db
from src.repository import contacts as repositories_contacts
from src.schemas.contacts import ContactCreateSchema, ContactUpdateSchema, ContactResponseSchema

router = APIRouter(prefix='/contacts', tags=['contacts'])


@router.get("/", response_model=list[ContactResponseSchema])
async def get_contacts(limit: int = Query(10, ge=10, le=500),
                       offset: int = Query(0, ge=0),
                       db: AsyncSession = Depends(get_db)):
    contacts = await repositories_contacts.get_contacts(limit, offset, db)
    return contacts


@router.get("/{contact_id}", response_model=ContactResponseSchema)
async def get_contact(contact_id: int, db: AsyncSession = Depends(get_db)):
    contact = await repositories_contacts.get_contact(contact_id, db)
    if contact is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="NOT FOUND")
    return contact


@router.post("/", response_model=ContactResponseSchema, status_code=status.HTTP_201_CREATED)
async def create_contact(body: ContactCreateSchema, db: AsyncSession = Depends(get_db)):
    contact = await repositories_contacts.create_contact(body, db)
    return contact


@router.put("/{contact_id}", response_model=ContactResponseSchema, status_code=status.HTTP_200_OK)
async def update_contact(body: ContactUpdateSchema, contact_id: int, db: AsyncSession = Depends(get_db)):
    contact = await repositories_contacts.update_contact(contact_id, body, db)
    if contact is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="NOT FOUND")
    return contact


@router.delete("/{contact_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_contact(contact_id: int, db: AsyncSession = Depends(get_db)):
    contact = await repositories_contacts.delete_contact(contact_id, db)
    return contact


@router.get("/search/{search_text}", response_model=list[ContactResponseSchema])
async def search_contacts(search_text: str,
                          limit: int = Query(10, ge=10, le=500),
                          offset: int = Query(0, ge=0),
                          db: AsyncSession = Depends(get_db)):
    if len(search_text) < 3:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Search text must be at least 3 characters long")
    contacts = await repositories_contacts.search_contacts(search_text, limit, offset, db)
    if len(contacts) == 0:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="NOT FOUND")
    return contacts


@router.get("/birthdays/{days}", response_model=list[ContactResponseSchema])
async def get_birthdays(days: int,
                        limit: int = Query(10, ge=10, le=500),
                        offset: int = Query(0, ge=0),
                        db: AsyncSession = Depends(get_db)):
    if days <= 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Days must be positive")
    contacts = await repositories_contacts.get_birthdays(days, limit, offset, db)
    if len(contacts) == 0:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="NOT FOUND")
    return contacts