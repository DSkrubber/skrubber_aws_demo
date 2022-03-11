from typing import Dict

from botocore.exceptions import BotoCoreError
from fastapi import Depends, FastAPI, Path, Request, status
from fastapi.responses import JSONResponse
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.database import Base, engine, get_db
from app.dynamodb_queues import save_dynamo_queue
from app.lambda_app import lambda_trigger
from app.models import User
from app.s3_connect import BUCKET, connect_s3, get_bucket_object, put_in_bucket
from app.schemas import ConnectionErrorSchema, UserSchema

Base.metadata.drop_all(bind=engine)
Base.metadata.create_all(bind=engine)


app = FastAPI(
    title="Skrubber App",
    version="0.0.1",
)


@app.exception_handler(BotoCoreError)
def boto3_error(request: Request, exc: BotoCoreError):
    return JSONResponse(
        status_code=404,
        content={"detail": f"AWS error: {exc}"},
    )


@app.exception_handler(SQLAlchemyError)
def database_error(request: Request, exc: SQLAlchemyError):
    return JSONResponse(
        status_code=404,
        content={"detail": f"Database error: {exc}"},
    )


@app.post(
    "/users/{user_id}",
    status_code=status.HTTP_201_CREATED,
    response_model=Dict[str, UserSchema],
    responses={
        500: {"model": ConnectionErrorSchema},
    },
    summary="Create users data in different aws resources.",
)
def add_user(
    user_id: int = Path(..., example=1),
    db: Session = Depends(get_db),
):
    s3 = connect_s3()
    put_in_bucket(s3, BUCKET, user_id)
    user_s3 = get_bucket_object(s3, BUCKET, user_id)
    user_s3_id, user_s3_role = user_s3.get("user_id"), user_s3.get("user_role")
    user = User(user_id=user_s3_id, user_role=user_s3_role)
    save_dynamo_queue(user_id)
    new_res = lambda_trigger(user_id)
    user_lambda = User(user_id=new_res, user_role="lambda_role")
    db.add_all((user, user_lambda))
    db.commit()
    result = {
        "s3_user": UserSchema.from_orm(user),
        "lambda_user": UserSchema.from_orm(user_lambda),
    }
    return result
