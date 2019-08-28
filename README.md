# HbaseHDFSDemo

使用docker 安装并运行hbase
```
docker pull harisekhon/hbase:1.3

docker run -d -h hbase -p 2181:2181 -p 8080:8080 -p 8085:8085 -p 9090:9090 -p 9095:9095 \
-p 16000:16000 -p 16010:16010 -p 16201:16201 -p 16301:16301 --name hbase1.3 harisekhon/hbase:1.3

```
启动后续需修改本机/etc/hosts文件
```
sudo vim /etc/hosts
##指定docker启动hbase时的host为127.0.0.1
127.0.0.1    hbase

```