import random
from locust import HttpUser, task, tag, between
import base64
import os
from pathlib import Path
import logging

data_dir  = Path('/mnt/locust/faas_data')    # for docker usage
image_dir = data_dir / 'image_process_base64'
video_dir = data_dir / 'video_process_base64'

image_data = {}
image_names = []
mobilenet_names = []

for img in os.listdir(str(image_dir)):
    full_path = image_dir / img
    image_names.append(img)
    with open(str(full_path), 'r') as f:
        image_data[img] = f.read()

video_data = {}
video_names = []

for video in os.listdir(str(video_dir)):
    full_path = video_dir / video
    video_names.append(video)
    with open(str(full_path), 'r') as f:
        video_data[video] = f.read()

# get through wsk -i  property get --auth
auth_str = '23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP'
pwd_1, pwd_2 = auth_str.strip().split(':')
auth = (pwd_1, pwd_2)

class OpenWhiskUser(HttpUser):
    wait_time = between(5, 9)

    # # return wait time in second
    # def wait_time(self):
    #     self.last_wait_time += 1
    #     return self.last_wait_time

    @task
    @tag('image_process')
    def image_process(self):
        params = {}
        params['blocking'] = 'true'
        params['result'] = 'true'

        url = '/api/v1/namespaces/_/actions/image_process'

        img = random.choice(image_names)
        body = {}
        body['image'] = image_data[img]

        r = self.client.post(url, params=params,
            json=body, auth=auth, verify=False,
            name="/image_process")

        logging.info('image_proess resp.status = %d, text=%s' %(r.status_code,
            r.text))

    @task
    @tag('mobilenet')
    def mobilenet(self):
        params = {}
        params['blocking'] = 'true'
        params['result'] = 'true'

        url = '/api/v1/namespaces/_/actions/mobilenet'

        img = random.choice(image_names)
        body = {}
        body['image'] = image_data[img]
        body['format'] = img.split('.')[-1]

        r = self.client.post(url, params=params,
            json=body, auth=auth, verify=False,
            name='/mobilenet')

        logging.info('mobilenet resp.status = %d, text=%s' %(r.status_code,
            r.text))
    @task
    @tag('video_process')
    def video_process(self):
        params = {}
        params['blocking'] = 'true'
        params['result'] = 'true'

        url = '/api/v1/namespaces/_/actions/video_process'

        video = random.choice(video_names)
        body = {}
        body['video'] = video_data[video]
        body['video_name'] = video

        r = self.client.post(url, params=params,
            json=body, auth=auth, verify=False,
            name='/video_process')

        logging.info('video_process resp.status = %d, text=%s' %(r.status_code,
            r.text))