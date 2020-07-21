# couch_hbaf
 Repository for HBAF for project Couch

How To Build a HBAF Container Using Python
===============================================
June, 2020

Build Configuration/Management
------------------------------

We'll create a file called "Dockerfile" that contains the steps and information needed to build an image. The build process is done in layers, with the starting point typically being an operating system or, more likely, an OS and framework combination. For example, you need Python 3 installed. So it's pretty common to start from that point. You can -- and there might be good reasons for this -- start with the operating system and then install the framework all inside your image as you build it. You can also do just that -- start with an OS and add a framework -- and save that image and use it as your base for other images. Yes, like any IT technology, you can make this as simple or as complicated as you like. We'll start with an OS+Framework combination to keep things simple. We'll then copy our code in the image, then make sure the dependencies are installed. Then, we'll give the image a command to be executed when someone runs the image in a container. The following file, "Dockerfile" does those things:

```
FROM python:latest
COPY . /python_app
WORKDIR /python_app 
RUN pip3 install -r requirements.txt
CMD python ./short_term_main.py 
```


A line-by-line explanation is later in this article, but let's just build thing and run it; we come back to the details later.

Let's Get Some Code
-------------------
create directory for example src/python/hello-world.  
Fork or clone the github repository at https://github.com/weusthofm/couch_hbaf.  
Move into the directory src/python/hello-world.  

Let's Build And Run
-------------------
To build the image, run  
```
docker build --tag python-app .
```
This will “tag” the image my-python-app and build it. After it is built, you can run the image as a container.  
 
To run the image (again, we'll explain this later), run    

```
docker run  python-app
```
This starts the application as a container.   

To edit the container

```
 docker run -it python-app /bin/bash
 ```
 
This will “tag” the image python-app and build it. After it is built, you can run the image as a container.  

Here are the steps with the logic behind the python scripts:
A. The first step is to define the server connection details, for both reading data (server_read) and uploading data (server_upload). If this is None (It's for security reasons), the system asks the user’s input for the server connection attributes
B. Based on the server_read, it calculates all the existing device ids that correspond to a certain user with a certain device
C. For each unique device id, it reads all the available raw smartphone data that exist on the server_read
D. Based on the raw smartphone data, it calculates four dataframes; Physical_Activity, Social_Activity, Emotional_Activity and Cognitive_Activity
E. Based on the extracted dataframes, it uploads the processed data on the server_store.


Every time that you run the code, the ‘server_read' and ‘server_store' values will be None and thus, the system will ask for your input to gain access on the server.
Please insert the following for the server_read:
Please enter the host: host.com 
Please enter the user: user 
Please enter the password: password 
Please enter the name of the database: databasename

Please insert the following for the server_store:
Please enter the host: host.com
Please enter the user: user
Please enter the password: password 
Please enter the name of the database: databasename

These should be written as follows:
```
server_read = {'host': 'host.com', 'user': 'user', 'passwd': 'password', 'db': 'databasename' }
server_store = {'host': 'host.com', 'user': 'Short_Behaviour', 'passwd': 'password', 'db': 'databasename'}

```

---------------------------

*Last updated: July 21, 2020*
