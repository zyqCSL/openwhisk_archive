docker run -v $PWD/src:/mnt/locust -v $PWD/faas_data:/mnt/faas_data -v $PWD/logs:/mnt/locust_log yz2297/locust_openwhisk \
	-f /mnt/locust/general_locust_file.py \
	--csv=/mnt/locust_log/$3 --headless -t $1 \
	--host https://172.17.0.1 --users $2 \
	--tags $3 --logfile /mnt/locust_log/locust_openwhisk_log.txt