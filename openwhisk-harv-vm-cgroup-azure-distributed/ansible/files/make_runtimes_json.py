import json
# from pathlib import Path

with open('./runtimes_original.json', 'r') as f:
    runtimes = json.load(f)


prefix = 'sailresearch'
tag = 'latest'
images = ['python3_openwhisk']
copies = 50
copy_images = ['chameleon_openwhisk',
               'image_processing_openwhisk',
               'matmult_openwhisk',
               'linpack_openwhisk',
               'pyaes_openwhisk',
               'video_process_openwhisk',
               'lr_review_openwhisk',
               'mobilenet_openwhisk']

for img in images:
    img_config = {}
    img_config['prefix'] = prefix
    img_config['name'] = img
    img_config['tag'] = tag
    runtimes['blackboxes'].append(img_config)

for img in copy_images:
    for i in range(0, copies):
        img_config = {}
        img_config['prefix'] = prefix
        img_config['name'] = img + '-' + str(i)
        img_config['tag'] = tag
        runtimes['blackboxes'].append(img_config)

with open('./runtimes_exp.json', 'w+') as f:
    json.dump(runtimes, f, indent=4)


