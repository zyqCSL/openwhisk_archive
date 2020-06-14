docker run -v $PWD/src:/mnt/locust -v $PWD/logs:/mnt/locust_log yz2297/locust_openwhisk \
	-f /mnt/locust/general_locust_file.py \
	--csv=/mnt/locust_log/image_process --headless -t $1 \
	--host https://172.17.0.1 --users $2 \
	--tags image_process --logfile /mnt/locust_log/locust_openwhisk_log.txt