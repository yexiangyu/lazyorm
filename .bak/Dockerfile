FROM alpine
RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/g' /etc/apk/repositories
RUN apk update && apk upgrade
RUN apk add python3 py3-pip gcc python3-dev musl-dev git 
RUN pip install git+https://github.com/yexiangyu/lazyorm@dev -i https://mirrors.aliyun.com/pypi/simple/
RUN pip install pytest
CMD /bin/sh
