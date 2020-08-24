# simple-load-balancer

## Features

- Uses round robin algorithm to balance load among nodes
- Passive Health Check every 2 minutes
- Actively marking dead nodes

## Use

`docker-compose up`

Open `http://localhost:3030` on the browser and it will be serviced by different servers
