docker run -v $PWD/src:/mnt/locust -v $PWD/logs:/mnt/locust_log yz2297/locust_openwhisk \
	-f /mnt/locust/general_locust_file.py \
	--csv=/mnt/locust_log/matmult --headless -t $1 \
	--host https://172.17.0.1 --users $2 \
	--tags matmult --logfile /mnt/locust_log/locust_openwhisk_log.txt