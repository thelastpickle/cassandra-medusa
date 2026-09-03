FROM cassandra:4.1

# Install sshd, Python, pip, and supervisor
RUN apt-get update && apt-get install -y --no-install-recommends \
        openssh-server \
        python3 \
        python3-pip \
        python3-dev \
        gcc \
        sudo \
        supervisor \
    && rm -rf /var/lib/apt/lists/*

# Install medusa from source (the repo root is the build context).
# Only copy the files needed for the package install — avoids leaking
# unrelated repo content (credentials, test configs, etc.) into the image.
COPY medusa/ /build/medusa/
COPY pyproject.toml poetry.lock README.md /build/
RUN pip install --break-system-packages /build # nosonar — installs medusa from source intentionally

# Ensure medusa-wrapper is on PATH (the install creates it; symlink if absent)
RUN which medusa-wrapper || (ln -s "$(which medusa)" /usr/local/bin/medusa-wrapper && echo "medusa-wrapper symlinked")

# Create medusa user.  authorized_keys is injected at test startup via docker cp
# so the SSH pubkey never touches the image layer cache — each CI run gets a fresh key.
RUN useradd -m -s /bin/bash medusa \
    && mkdir -p /home/medusa/.ssh \
    && chmod 700 /home/medusa/.ssh \
    && chown -R medusa:medusa /home/medusa/.ssh

# Passwordless sudo for medusa user.
RUN mkdir -p /etc/sudoers.d \
    && printf 'medusa ALL=(ALL) NOPASSWD: ALL\n' \
       > /etc/sudoers.d/medusa \
    && chmod 440 /etc/sudoers.d/medusa

# Expose JAVA_HOME to all sessions (including non-login SSH) via /etc/environment
# so that nodetool can find java even under sudo's stripped environment.
# Docker's ENV directive is only visible to processes launched by the container
# runtime (supervisord, docker exec), not to SSH-spawned shells.
RUN echo "JAVA_HOME=/opt/java/openjdk" >> /etc/environment \
    && ln -sf /opt/java/openjdk/bin/java /usr/local/bin/java

# Required by sshd at runtime
RUN mkdir -p /run/sshd

# Copy supervisor config and cassandra startup script
COPY tests/e2e/supervisord.conf /etc/supervisor/conf.d/supervisord.conf
COPY tests/e2e/cassandra-start.sh /usr/local/bin/cassandra-start.sh
RUN chmod +x /usr/local/bin/cassandra-start.sh

# nosonar — port 22 is required: the ssh-e2e CI job connects over SSH to test medusa SSH backup mode
EXPOSE 22 9042
ENTRYPOINT ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]

