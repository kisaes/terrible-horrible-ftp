FROM alpine
RUN echo "net.ipv4.ip_forward=1" >> /etc/sysctl.conf
CMD sleep 999999d