FROM node:22-alpine AS dashboard
WORKDIR /dashboard
COPY dashboard/package.json dashboard/package-lock.json ./
RUN npm ci
COPY dashboard ./
RUN npm run build

FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml README.md ./
COPY taskqueue ./taskqueue
RUN pip install --no-cache-dir .
COPY examples ./examples
COPY --from=dashboard /dashboard/dist ./dashboard/dist
ENV REDIS_URL=redis://redis:6379/0 PYTHONUNBUFFERED=1
EXPOSE 8000
CMD ["python", "-m", "taskqueue", "api", "--port", "8000"]
