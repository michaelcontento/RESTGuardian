from setuptools import setup, find_packages

setup(
    name = "RESTGuardian",
    version="0.0.1",
    packages=find_packages(),
    scripts=["bin/restguardian"],
    install_requires=["docutils>=0.3", "tornado>=1.1", "mysql-python>=1.2.3"],

    author="Michael Contento",
    author_email="michaelcontento@gmail.com",
    description="RESTful MySQL",
    license="Apache",
    url="https://github.com/michaelcontento/RESTGuardian",
    download_url="https://github.com/michaelcontento/RESTGuardian/tarball/master",
    classifiers=[
        "Development Status :: 3 - Alpha",
	"Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: Apache Software License",
	"Operating System :: OS Independent",
	"Programming Language :: Python",
	"Topic :: Database :: Front-Ends",
	"Topic :: Internet :: WWW/HTTP :: HTTP Servers",
	"Topic :: Software Development"	
    ],
)
