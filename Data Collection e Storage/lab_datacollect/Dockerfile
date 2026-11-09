# Ambiente Python do laboratório (scripts e Jupyter).
# Roda em Linux para que pandas, pyarrow, pyiceberg e confluent-kafka funcionem
# igual em qualquer máquina, sem depender do Python instalado no Windows/Mac.
FROM python:3.12.15-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUTF8=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Só as dependências entram na imagem; o código é montado em /app pelo compose,
# então editar um arquivo no seu editor tem efeito imediato no container.
COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt && rm /tmp/requirements.txt

# Usuário sem privilégio de root
RUN useradd --create-home --uid 1000 lab
USER lab
WORKDIR /app

EXPOSE 8888
