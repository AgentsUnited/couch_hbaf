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



```
The Cycle
---------

So that's the basic cycle:

1.  Create the source code
2.  Create a Dockerfile file
3.  Build the image
4.  Run the image in a container

About that Dockerfile
---------------------

The file “Dockerfile” is used to guide the construction of your image. Here’s a short, step-by-step breakdown: `FROM python:latest` This is your base image, the starting point. In this case, it’s the official image from the Python Software Foundation and has Python:3 installed. That means we don’t have to install any framework; it’s already included with this base image.  ![](./Build%20Your%20_Hello%20World_%20Container%20Using%20Python%20_%20Red%20Hat%20Developer_files/Screen-Shot-2019-03-05-at-11.44.31-AM.png)

```
FROM python:latest
COPY . /python_app
WORKDIR /python_app
```

Copies the program into the image.

```
RUN pip install -r requirements.txt
```

Install the packages necessary for our code into the image via the file requirements.txt.


CMD [ "python", "./index.py"]
```

This is what runs when the image is started (i.e. `docker run`).

---------------------------

*Last updated: June 21, 2020*
