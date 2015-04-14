# Tagcloud-Backend
Backend for a simple tagcloud service

<h3>Requirements</h3>

Install<a href = "http://docs.docker.com/installation/"> Docker </a> and <a href = "https://docs.docker.com/compose/install/#install-compose"> Docker Compose </a>

<h3>Usage</h3>
From terminal, cd into "Tagcloud-Backend" folder

You should edit your OAuth credentials in "OAuth.json"

Edit Dockerfile to set streaming duration and number of words to be returned

e.g. "CMD python main.py 3 5" will fetch data for 3 seconds and display top 5 results
  
    $ docker-compose up

You will find the output in "data.json"
