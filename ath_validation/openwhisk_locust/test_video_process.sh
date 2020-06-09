docker run -v $PWD:/mnt/locust -v $HOME/openwhisk_locust_log:/mnt/locust_log yz2297/locust_openwhisk \
	-f /mnt/locust/general_locust_file.py \
	--csv=/mnt/locust/video_process --headless -t $1 \
	--host https://172.17.0.1 --users $2 \
	--tags video_process