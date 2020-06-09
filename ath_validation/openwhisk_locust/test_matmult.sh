docker run -v $PWD:/mnt/locust -v $HOME/openwhisk_locust_log:/mnt/locust_log yz2297/locust_openwhisk \
	-f /mnt/locust/general_locust_file.py \
	--csv=/mnt/locust_log/matmult --headless -t 3m \
	--host https://172.17.0.1 --users 10 \
	--tags matmult