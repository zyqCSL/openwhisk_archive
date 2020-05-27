docker run -v $PWD:/mnt/locust -f /mnt/locust/general_locust_file.py \
	--csv=/mnt/locust/image_process --headless -t 3m \
	--host https://172.17.0.1 --users 1 \
	--tags image_process locustio/locust