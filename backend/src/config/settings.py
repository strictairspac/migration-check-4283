import os
DATABASE_URL=os.getenv('DATABASE_URL','postgresql+psycopg://migrate:migratepass@db:5432/migrationcheck')
REDIS_URL=os.getenv('REDIS_URL','redis://redis:6379/0')
JWT_SECRET=os.environ['JWT_SECRET']
TARGET_ROW_LIMIT=int(os.getenv('TARGET_ROW_LIMIT','1000'))
EXECUTOR_STALE_SECONDS=int(os.getenv('EXECUTOR_STALE_SECONDS','120'))
