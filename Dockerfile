FROM jupyter/base-notebook:latest

USER root

# 시스템 패키지 설치
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    python3-dev \
    cython3 \
    pkg-config \
    libcairo2-dev \
    libjpeg-dev \
    libgif-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER jovyan

# 환경 변수 명시적으로 고정
ENV PATH=/opt/conda/bin:$PATH
ENV CONDA_PREFIX=/opt/conda

# conda 업데이트 및 mamba 설치
RUN /opt/conda/bin/conda update -n base -c defaults conda -y \
    && /opt/conda/bin/conda install -n base -c conda-forge mamba -y

# mamba로 패키지 설치 (경로 명시)
RUN /opt/conda/bin/mamba install -y -c conda-forge \
    pandas=2.1.4 \
    numpy=1.26.2 \
    networkx \
    matplotlib=3.8.2 \
    seaborn=0.13.0 \
    scipy=1.11.3 \
    scikit-learn=1.3.2 \
    tqdm \
    jupyter=1.0.0 \
    networkit \
    python-igraph \
    plotly=5.18.0 \
    openpyxl=3.1.2 \
    pyarrow=20.0.0 \
    python-louvain \
    umap-learn

# pip로 추가 설치 (경로 명시)
RUN /opt/conda/bin/pip install --upgrade pip setuptools wheel && \
    /opt/conda/bin/pip install --no-build-isolation --no-cache-dir \
    koreanize_matplotlib \
    leidenalg \
    google-cloud-storage==3.1.0 \
    gcsfs==2025.5.1 \
    aiohttp==3.12.2 \
    requests==2.31.0 \
    python-dotenv==1.0.0 \
    jupyterlab \
    ipywidgets==8.1.7

# 작업 디렉토리
WORKDIR /home/jovyan