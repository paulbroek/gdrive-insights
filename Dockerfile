FROM python:3.9

RUN python -m pip install --upgrade pip

RUN apt-get update   		            && \
	apt-get install git -y				&& \
	apt-get install openssh-client

COPY requirements.txt /tmp

RUN /usr/local/bin/python -m pip install --upgrade pip
RUN pip install -r /tmp/requirements.txt

COPY . /tmp

RUN pip install -U git+https://git@github.com/paulbroek/rarc-utils.git 

# install package
RUN pip install /tmp/

# copy config files
# COPY /tmp/gdrive_insights/config/postgres.cfg /usr/local/lib/python3.9/site-packages/gdrive_insights/config

# adds deps here that can later me moved to requirements.txt
# RUN pip install pandas  

WORKDIR /src
