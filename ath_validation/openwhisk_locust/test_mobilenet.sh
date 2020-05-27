docker run -v $PWD:/mnt/locust locustio/locust -f /mnt/locust/general_locust_file.py \
	--csv=./mobilenet --headless -t 3m \
	--host https://172.17.0.1 --users 1 \
	--tags mobilenet