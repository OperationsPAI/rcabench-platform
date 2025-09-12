# Infrastructure Setup

## Distributed File System

| Service       | URL                              | User       | Password   |
| :------------ | :------------------------------- | :--------- | :--------- |
| redis         | <http://redis.example.org:6379>    |            |            |
| minio         | <http://minio.example.org:9000>    | minioadmin | minioadmin |
| minio console | <http://minio.example.org:9001>    | minioadmin | minioadmin |
| prometheus    | <http://prometheus.example.org:9090> |            |            |
| grafana       | <http://grafana.example.org:3000>   | minioadmin | minioadmin |

Please carefully operate the distributed file system services. Losing data will cause many problems. If you are not sure, please ask the administrator for help.

Install JuiceFS client: <https://juicefs.com/docs/zh/community/getting-started/installation>

Mount JuiceFS to your machine:

```bash
sudo juicefs mount redis://redis.example.org:6379/1 /mnt/jfs -d --cache-size=1024
```

Note that the cache size is set to 1024MiB instead of the default 100GiB. If your machine has enough disk space, you can set it to `--cache-size=102400` or other values.

Check if the mount was successful:

```bash
df -h
ls /mnt/jfs
```

Umount JuiceFS:

```bash
sudo juicefs umount /mnt/jfs
```
