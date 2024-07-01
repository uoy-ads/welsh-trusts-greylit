# Welsh Archaeological Trusts Grey Literature Ingestion @ ADS


## Description
This script will ingest Welsh Archaerological Truts grey literature data from the archwilio.org.uk database


## Installation

1. Clone the repository


```
git clone https://github.com/uoy-ads/welsh-trust-greylit.git
cd welsh-trust-greylit

```

2. Copy  `config_template.ini` to `config.ini` and add the requires credentials


3. Build the Docker image
`docker build -t welsh-trust-greylit .`


4. Run the container (the app will automatically start)

`docker run -it --rm welsh-trust-greylit`


Part of the Dockerfile was adapted from https://github.com/uoy-ads/ads-ingest/blob/main/server/Dockerfile by @adsjim

This app is released under CC0 license (see `CC0_LICENSE.txt`) but to avoid plagiarism, please cite if reusing in a scholarly or scientific context.
