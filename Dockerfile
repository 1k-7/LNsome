# Base image
FROM python:3.10-slim

# 1. Install system build dependencies AND jemalloc for memory optimization
RUN apt-get update && apt-get install -y \
gcc \
g++ \
git \
libxml2-dev \
libxslt-dev \
zlib1g-dev \
libjpeg-dev \
libffi-dev \
libssl-dev \
libjemalloc2 \
&& rm -rf /var/lib/apt/lists/*

# Set work directory
WORKDIR /app

# 2. Upgrade pip
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# 3. Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copy project files
COPY . .

# 5. IMPORTANT: Optimization Flags
# Switch to production to reduce logging/caching overhead
ENV LNCRAWL_MODE="production"
ENV PYTHONUNBUFFERED=1

# FORCE Python to use jemalloc to prevent memory fragmentation/leaks
ENV LD_PRELOAD="/usr/lib/x86_64-linux-gnu/libjemalloc.so.2"

# Create downloads directory
RUN mkdir -p downloads

# Command to run the bot
CMD ["python", "bot.py"]