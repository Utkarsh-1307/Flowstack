from pydantic import BaseModel, EmailStr, Field
from uuid import UUID
from datetime import datetime


class UserCreateSchema(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=8)


class UserResponseSchema(BaseModel):
    id: UUID
    email: str
    created_at: datetime

    model_config = {"from_attributes": True}
