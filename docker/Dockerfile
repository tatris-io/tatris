FROM golang:1.19 AS build

ENV GOPROXY https://goproxy.cn,direct
ENV GO111MODULE on

WORKDIR /go/cache
ADD go.mod .
ADD go.sum .
RUN go mod download

COPY . /go/src/
WORKDIR /go/src/
RUN make build



FROM centos:centos7

RUN ln -snf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && echo "Asia/Shanghai" >> /etc/timezone && \
  yum -y install epel-release && yum install -y sudo net-tools iproute dstat which supervisor stress unzip jq screen nginx wget telnet cronolog less && \
  yum -y clean all && \
  rm -rf /var/cache/yum

COPY supervisor/start_supervisorctl.sh /usr/local/bin/start_supervisorctl.sh
COPY supervisor/start_supervisord.sh /usr/local/bin/start_supervisord.sh
COPY supervisor/supervisord.conf /etc/supervisord.conf
COPY supervisor/tatris.ini /etc/supervisord.d/tatris.ini
COPY supervisor/start_tatris.sh /home/admin/bin/start_tatris.sh

COPY --chown=admin:admin scripts/*.sh /home/admin/bin/
COPY docker/entrypoint.sh /entrypoint.sh

# global initialization
RUN echo 'PS1="\n\e[1;37m[\e[m\e[1;32m\u\e[m\e[1;33m@\e[m\e[1;35m\h\e[m \e[1;35m`hostname`\e[m \e[4m\`pwd\`\e[m\e[1;37m]\e[m\e[1;36m\e[m\n\\$ "' >> /etc/bashrc && \
  echo 'alias vim="vi"' >> /etc/bashrc && \
  echo 'alias ll="ls -laF"' >> /etc/bashrc && \
  echo 'shell /bin/bash' >> /root/.screenrc && \
# admin initialization
  groupadd --gid 500 admin && \
  useradd admin -s /bin/bash --uid 500 --gid 500 -G root && \
  echo 'shell /bin/bash' >> /home/admin/.screenrc && \
  echo 'admin ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers.d/admin && \
  mkdir -p /home/admin/logs && \
  echo '[ -e "/home/admin/bin/init_bashrc.sh" ] && source /home/admin/bin/init_bashrc.sh' >> /home/admin/.bashrc

VOLUME /home/admin/logs

# fix auth
RUN chown -R admin:admin /home/admin

COPY --from=build /go/src/bin /home/admin/bin

USER admin

WORKDIR /home/admin

ENTRYPOINT ["/entrypoint.sh"]