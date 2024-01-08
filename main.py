import redis.asyncio as redis
import typing
import re

from fastapi import FastAPI, Depends, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi_limiter import FastAPILimiter
from fastapi.responses import JSONResponse
from ipaddress import ip_address

from src.conf.config import config
from src.database.db import get_db
from src.routes import contacts, auth, users

app = FastAPI()


banned_ips = [
    ip_address("192.168.1.1"),
    ip_address("192.168.1.2"),
    # ip_address("127.0.0.1"),
]
origins = [ 
    "http://localhost:3000"
    ]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

user_agent_ban_list = [r"Googlebot", r"Python-urllib"]

@app.middleware("http")
async def user_agent_ban_middleware(request: Request, call_next: typing.Callable):
    print(request.headers.get("Authorization"))
    user_agent = request.headers.get("user-agent")
    print(user_agent)
    for ban_pattern in user_agent_ban_list:
        if re.search(ban_pattern, user_agent):
            return JSONResponse(
                status_code=status.HTTP_403_FORBIDDEN,
                content={"detail": "You are banned"},
            )
    response = await call_next(request)
    return response


app.include_router(auth.router, prefix="/api")
app.include_router(contacts.router, prefix="/api")
app.include_router(users.router, prefix="/api")


def register_startup_event_limiter(app: FastAPI):
    @app.on_event("startup")
    async def startup():
        r = await redis.Redis(
            host=config.REDIS_DOMAIN,
            port=config.REDIS_PORT,
            db=0
        )
        await FastAPILimiter.init(r)


@app.get("/")
def index():
    return {"message": "Address Book Application"}


@app.get("/api/healthchecker")
async def healthchecker(db: AsyncSession = Depends(get_db)):
    try:
        # Make request
        result = await db.execute(text("SELECT 1"))
        result = result.fetchone()
        if result is None:
            raise HTTPException(
                status_code=500, detail="Database is not configured correctly")
        return {"message": "Welcome to FastAPI!"}
    except Exception as e:
        print(e)
        raise HTTPException(
            status_code=500, detail="Error connecting to the database")
