# iron/monger
FROM iron/busybox

ADD monger /root/monger

ENTRYPOINT ["/root/monger"]
