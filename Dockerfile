FROM python:3.12-slim

WORKDIR /app

RUN pip install uv

COPY pyproject.toml .
COPY src/ src/

RUN uv pip install --system -e .

EXPOSE 8079

CMD ["uvicorn", "deliverator.app:app", "--host", "0.0.0.0", "--port", "8079"]
