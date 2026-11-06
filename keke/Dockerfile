FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    TZ=Asia/Shanghai

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY keke.py .
COPY config.example.yaml ./config.yaml

# 记忆文件写在这里，记得挂卷持久化
RUN mkdir -p /app/memory

CMD ["python", "-u", "keke.py"]
