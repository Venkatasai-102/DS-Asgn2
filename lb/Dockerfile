FROM ubuntu:20.04

# Set the working directory to /app
WORKDIR /app

COPY . /app/

RUN apt-get -y update
RUN apt-get  install -y python3
RUN apt-get  install -y python3-pip

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt


RUN apt-get -y update



# Add Docker's official GPG key:
RUN apt-get -y update
RUN apt-get -y  install ca-certificates curl gnupg
RUN install -m 0755 -d /etc/apt/keyrings
RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
RUN chmod a+r /etc/apt/keyrings/docker.gpg

# Add the repository to Apt sources:
RUN echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
   tee /etc/apt/sources.list.d/docker.list > /dev/null
RUN apt-get -y update


RUN apt-get -y  install docker-ce-cli 

# Run app.py when the container launches
CMD ["uvicorn", "lb:app", "--host", "0.0.0.0", "--port", "8000","--reload"]