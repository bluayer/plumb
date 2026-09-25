FROM python:3.12-slim
WORKDIR /app
# CPU-only torch keeps the image small; swap the index for a CUDA build if the sidecar gets a GPU.
RUN pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu
COPY pyproject.toml ./
COPY plumb_decision/ plumb_decision/
COPY questions/ questions/
RUN pip install --no-cache-dir ".[laya]"
USER 65532:65532
ENV HF_HOME=/tmp/hf
EXPOSE 50051
ENTRYPOINT ["python", "-m", "plumb_decision", "--address", "127.0.0.1:50051"]
