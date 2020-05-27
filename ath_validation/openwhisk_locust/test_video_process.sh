docker run -v $PWD:/mnt/locust locustio/locust -v $HOME/openwhisk_locust_log:/mnt/locust_log \
	-f /mnt/locust/general_locust_file.py \
	--csv=/mnt/locust/video_process --headless -t 3m \
	--host https://172.17.0.1 --users 10 \
	--tags video_process