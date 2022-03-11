from pydantic import BaseModel, Field


class UserSchema(BaseModel):
    user_id: int = Field(..., example=1)
    user_role: str = Field(..., example="employee")

    class Config:
        orm_mode = True


class ConnectionErrorSchema(BaseModel):
    detail: str

    class Config:
        schema_extra = {
            "example": {"detail": "Error: Connection error."},
        }
