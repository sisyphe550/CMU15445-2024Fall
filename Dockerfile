FROM ccr.ccs.tencentyun.com/library/ubuntu:22.04
# 设置时区避免交互式询问
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Asia/Shanghai

# 使用阿里云镜像源加速包下载
RUN sed -i 's/archive.ubuntu.com/mirrors.aliyun.com/g' /etc/apt/sources.list

# 更新系统并安装所有开发依赖
RUN apt-get update && apt-get install -y \
    build-essential \
    clang-12 \
    clang-format-12 \
    clang-tidy-12 \
    clangd-12 \
    cmake \
    doxygen \
    git \
    zip \
    pkg-config \
    zlib1g-dev \
    libelf-dev \
    libdwarf-dev \
    gdb \
    valgrind \
    vim \
    curl \
    wget \
    unzip \
    sudo \
    libc++-dev \
    libc++abi-dev \
    g++-12 \
    && rm -rf /var/lib/apt/lists/*

# 创建一个非root用户
RUN useradd -m -s /bin/bash -G sudo bustub && \
    echo "bustub:bustub" | chpasswd && \
    echo "bustub ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# 设置工作目录
WORKDIR /workspace

# 切换到bustub用户
USER bustub

# 设置环境变量（使用官方指定的clang-12和g++-12版本）
ENV CC=clang-12
ENV CXX=clang++-12

# 暴露调试端口（可选）
EXPOSE 7777

# 默认启动bash
CMD ["/bin/bash"] 