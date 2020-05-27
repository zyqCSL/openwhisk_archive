docker run -v $PWD:/mnt/locust locustio/locust -f /mnt/locust/locustfile.py \
	--csv=image_process --headless -t 3m \
	--host https://172.17.0.1 --users 1 \
	--tags image_process