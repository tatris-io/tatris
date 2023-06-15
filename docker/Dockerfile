FROM --platform=$TARGETPLATFORM golang:1.19 AS build

ARG TARGETPLATFORM
ARG GOLANG_LDFLAGS

ARG GOPROXY=direct
# You can use GOPROXY to speed up building.
# ARG GOPROXY=https://...,direct

# use docker cache to speed up building
RUN --mount=target=/go/src,type=bind \
  --mount=type=cache,target=/root/.cache/go-build \
  --mount=type=cache,target=/root/go/pkg \
  echo "building for $TARGETPLATFORM" && \
  cd /go/src && \
  go build -ldflags="$GOLANG_LDFLAGS" -o /go/bin/tatris-server ./cmd/server/...

FROM centos:centos7

RUN yum -y install epel-release && \
  yum install -y sudo which supervisor less jq lsof net-tools dstat stress wget telnet && \
  yum -y clean all && \
  rm -rf /var/cache/yum

# global initialization
RUN echo 'PS1="\n\e[1;37m[\e[m\e[1;32m\u\e[m\e[1;33m@\e[m\e[1;35m\h\e[m \e[1;35m`hostname`\e[m \e[4m\`pwd\`\e[m\e[1;37m]\e[m\e[1;36m\e[m\n\\$ "' >> /etc/bashrc && \
  echo 'alias vim="vi"' >> /etc/bashrc && \
  echo 'alias ll="ls -laF"' >> /etc/bashrc && \
  echo 'shell /bin/bash' >> /root/.screenrc && \
  # user tatris initialization
  groupadd tatris && \
  useradd tatris -s /bin/bash -g tatris -G root && \
  echo 'shell /bin/bash' >> /home/tatris/.screenrc && \
  echo 'tatris ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers.d/tatris && \
  mkdir -p /home/tatris/bin && \
  mkdir -p /home/tatris/logs && \
  echo '[ -e "/home/tatris/bin/init_bashrc.sh" ] && source /home/tatris/bin/init_bashrc.sh' >> /home/tatris/.bashrc

COPY docker/scripts /home/tatris/scripts
COPY supervisor/* /tmp/

RUN mv /tmp/start_supervisorctl.sh /usr/local/bin/start_supervisorctl.sh && \
    mv /tmp/start_supervisord.sh /usr/local/bin/start_supervisord.sh && \
    mv /tmp/entrypoint.sh /usr/local/bin/entrypoint.sh && \
    mv /tmp/supervisord.conf /etc/supervisord.conf && \
    mv /tmp/tatris.ini /etc/supervisord.d/tatris.ini

VOLUME /home/tatris/logs

# fix auth
RUN chown -R tatris:tatris /home/tatris

COPY --from=build /go/bin /home/tatris/bin
COPY bin/start-server.sh /home/tatris/bin
COPY conf/*.yml /home/tatris/conf/

USER tatris

WORKDIR /home/tatris

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
