# ==============================================================================
# Dockerfile: 24/7 Self-Healing Flight Engine Node on Hugging Face Spaces
# Base Image: python:3.10-slim
# Target Port: 7860 (Hugging Face Spaces strict requirement)
# ==============================================================================

FROM python:3.10-slim

# Prevent Python from writing .pyc files and enable unbuffered output
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=7860

WORKDIR /app

# Install essential Linux build tools needed for database drivers (PostgreSQL / C-extensions)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy and install dependencies cleanly
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy the entire project codebase into the container workspace
COPY . .

# Configure non-root user (UID 1000) for Hugging Face Spaces compatibility
RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app

USER appuser

EXPOSE 7860

# Launch Streamlit dashboard on port 7860
CMD ["streamlit", "run", "src/dashboard/app.py", "--server.port", "7860", "--server.address", "0.0.0.0"]
