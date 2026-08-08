FROM python:3.12-slim
WORKDIR /app
COPY . .
RUN pip install -e ".[dev]"
EXPOSE 8000
CMD ["lineage-vault", "serve"]
